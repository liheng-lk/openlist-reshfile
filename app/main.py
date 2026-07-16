import os
import httpx
import asyncio
import logging
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import List, Optional, Dict
from contextlib import asynccontextmanager
from .manager import manager
from .scheduler import scheduler, reload_jobs, run_index_update
from .crawler import recursive_scan_and_refresh
from .incremental import remote_incremental_scan_and_refresh
from .watcher import IncrementalSyncWatcher

task_status: Dict[str, dict] = {}
incremental_watcher = IncrementalSyncWatcher(manager)

@asynccontextmanager
async def lifespan(app: FastAPI):
    reload_jobs()
    scheduler.start()
    incremental_watcher.start(task_status)
    yield
    await incremental_watcher.stop()
    scheduler.shutdown()

class AccessLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return not (msg.find("GET /api/task-status") != -1 or msg.find("GET /api/logs") != -1)

logging.getLogger("uvicorn.access").addFilter(AccessLogFilter())

app = FastAPI(title="OpenList 自动化索引助手", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

class TaskSchema(BaseModel):
    name: str
    mount_path: str
    cron: str
    enabled: bool = True
    incremental_enabled: bool = True
    watch_enabled: bool = False
    local_watch_path: Optional[str] = ""
    watch_mode: str = "polling"
    debounce_seconds: int = 5
    deep_check_limit: int = 200

class ConfigSchema(BaseModel):
    server_url: str
    username: str
    password: str
    tasks: List[TaskSchema]
    tool_password: Optional[str] = None

def verify_token(request: Request):
    config = manager.get_config()
    current_pass = config.get("tool_password", os.getenv("TOOL_PASSWORD", "admin888"))
    token = request.cookies.get("tool_auth")
    if token != current_pass:
        raise HTTPException(status_code=401, detail="未登录")
    return True

@app.get("/", response_class=HTMLResponse)
@app.head("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/api/login")
async def login(data: dict):
    config = manager.get_config()
    current_pass = config.get("tool_password", os.getenv("TOOL_PASSWORD", "admin888"))
    if data.get("password") == current_pass:
        response = JSONResponse({"status": "ok"})
        response.set_cookie(key="tool_auth", value=current_pass, httponly=True)
        return response
    raise HTTPException(status_code=401, detail="密码错误")

@app.post("/api/change-password")
async def change_password(data: dict, auth=Depends(verify_token)):
    new_pass = data.get("new_password")
    if not new_pass or len(new_pass) < 6: raise HTTPException(status_code=400)
    config = manager.get_config()
    config["tool_password"] = new_pass
    manager.save_config(config)
    response = JSONResponse({"status": "ok"})
    response.set_cookie(key="tool_auth", value=new_pass, httponly=True)
    return response

@app.get("/api/config")
async def get_config(auth=Depends(verify_token)): return manager.get_config()

@app.post("/api/config")
async def save_config(config: ConfigSchema, auth=Depends(verify_token)):
    old_config = manager.get_config()
    config_dict = config.model_dump()
    config_dict["tool_password"] = old_config.get("tool_password")
    manager.save_config(config_dict)
    reload_jobs()
    incremental_watcher.reload_config()
    return {"status": "ok"}

@app.get("/api/openlist/browse")
async def browse_openlist(path: str = "/", auth=Depends(verify_token)):
    config = manager.get_config()
    server_url = config.get("server_url", "").rstrip('/')
    username, password = config.get("username"), config.get("password")
    if not username or not password:
        return {"items": [], "error": "请先配置 OpenList 凭据并保存"}

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            # 1. 登录验证
            l_res = await client.post(f"{server_url}/api/auth/login", json={"username": username, "password": password})
            l_data = l_res.json()
            if l_res.status_code != 200 or not l_data.get("data"):
                return {"items": [], "error": f"登录 OpenList 失败: {l_data.get('message', '未知错误')}"}
            
            token = l_data.get("data", {}).get("token")
            
            # 2. 获取目录列表
            ls_res = await client.post(
                f"{server_url}/api/fs/list", 
                json={"path": path, "page": 1, "per_page": 200}, 
                headers={"Authorization": token}
            )
            ls_data = ls_res.json()
            if ls_res.status_code != 200 or not ls_data.get("data"):
                 return {"items": [], "error": f"获取目录失败: {ls_data.get('message', '权限不足或路径不存在')}"}
            
            content = ls_data.get("data", {}).get("content") or []
            items = [{"name": f["name"], "path": os.path.join(path, f["name"]).replace('\\', '/'), "type": "directory"} for f in content if f.get("is_dir")]
            return {"current_path": path, "items": items}
    except Exception as e:
        return {"items": [], "error": f"连接服务器出错: {type(e).__name__} {str(e)}"}

@app.get("/api/logs")
async def get_logs(auth=Depends(verify_token)): return {"logs": manager.get_logs()}

@app.get("/api/task-status")
async def get_task_status(auth=Depends(verify_token)): return task_status

@app.post("/api/run-now")
async def run_now(task_name: str, force_full: bool = False, auth=Depends(verify_token)):
    config = manager.get_config()
    task = next((t for t in config.get("tasks", []) if t['name'] == task_name), None)
    if not task: raise HTTPException(status_code=404)
    
    async def execute_task():
        task_status[task_name] = {"state": "scanning", "message": "正在获取文件统计..."}
        server_url = config.get("server_url", "").rstrip('/')
        username, password = config.get("username"), config.get("password")
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                l_res = await client.post(f"{server_url}/api/auth/login", json={"username": username, "password": password})
                token = l_res.json().get("data", {}).get("token")
                if not token:
                    task_status[task_name] = {"state": "error", "message": "OpenList 登录失效"}
                    manager.add_log(task_name, "登录 OpenList 失败，请检查配置账号密码")
                    return
                if task.get("incremental_enabled", True):
                    manager.add_log(task_name, "登录成功，启动远端快照增量引擎！")
                else:
                    manager.add_log(task_name, "登录成功，已切回「文件列表可见」模式，启动深度递归爬取引擎！")

                if task.get("incremental_enabled", True):
                    total_files, total_dirs = await remote_incremental_scan_and_refresh(
                        client,
                        server_url,
                        token,
                        task,
                        manager,
                        task_status,
                        force_full=force_full,
                    )
                else:
                    # 启动递归扫描爬虫
                    total_files, total_dirs = await recursive_scan_and_refresh(
                        client, server_url, token, task['mount_path'], manager, task_name, task_status
                    )

                    # 追加执行原生数据库增量构建（确保 Bleve 引擎更新）
                    await client.post(f"{server_url}/api/admin/index/update", params={"path": task['mount_path']}, headers={"Authorization": token})
                
                if task.get("incremental_enabled", True):
                    task_status[task_name] = {"state": "done", "message": f"增量同步完毕，扫描文件 {total_files} 个"}
                    manager.add_log(task_name, f"✅ 增量同步完毕，OpenList 索引已对齐！本轮扫描文件: {total_files}")
                else:
                    task_status[task_name] = {"state": "done", "message": f"递归完毕，共刷新记录 {total_files} 个文件"}
                    manager.add_log(task_name, f"✅ 目录强制更新完毕，且已触发原生引擎缓存对齐！总遍历文件: {total_files}")
                
        except Exception as e:
            task_status[task_name] = {"state": "error", "message": f"异常: {str(e)}"}
            manager.add_log(task_name, f"执行器内部异常: {str(e)}")
    
    asyncio.create_task(execute_task())
    return {"status": "started"}
