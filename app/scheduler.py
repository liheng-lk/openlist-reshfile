import httpx
import logging
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from .manager import manager
from .crawler import recursive_scan_and_refresh
from .incremental import remote_incremental_scan_and_refresh

scheduler = AsyncIOScheduler()

async def run_index_update(task):
    config = manager.get_config()
    server_url = config.get("server_url").rstrip('/')
    username = config.get("username")
    password = config.get("password")
    
    if not username or not password:
        manager.add_log(task['name'], "Error: Credentials incomplete.")
        return

    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            # 1. Login to get token
            login_res = await client.post(f"{server_url}/api/auth/login", json={
                "username": username,
                "password": password
            })
            if login_res.status_code != 200:
                manager.add_log(task['name'], f"登录失败: {login_res.text}")
                return
            
            token = login_res.json().get("data", {}).get("token")
            if not token:
                manager.add_log(task['name'], "登录失败: 未获取到 token")
                return
            
            if task.get("incremental_enabled", True):
                manager.add_log(task['name'], "自动执行 - 启动远端快照增量巡检！")
            else:
                manager.add_log(task['name'], "自动执行 - 已切回可视列表模式，启动深度递归！")
            headers = {"Authorization": token}

            if task.get("incremental_enabled", True):
                total_files, total_dirs = await remote_incremental_scan_and_refresh(
                    client, server_url, token, task, manager
                )
            else:
                total_files, total_dirs = await recursive_scan_and_refresh(
                    client, server_url, token, task['mount_path'], manager, task['name']
                )

            # 下发增量更新指令给Bleve
            if not task.get("incremental_enabled", True):
                await client.post(
                    f"{server_url}/api/admin/index/update",
                    params={"path": task['mount_path']},
                    headers=headers
                )
            
            manager.add_log(task['name'], f"自动执行 - 同步结束！本轮扫描记录 {total_files} 个文件。")

    except Exception as e:
        manager.add_log(task['name'], f"异常: {str(e)}")

def reload_jobs():
    for job in scheduler.get_jobs():
        job.remove()
    config = manager.get_config()
    for task in config.get("tasks", []):
        if not task.get("enabled", True):
            continue
        
        try:
            scheduler.add_job(
                run_index_update,
                CronTrigger.from_crontab(task['cron']),
                args=[task],
                id=task['name'],
                replace_existing=True
            )
            manager.add_log("系统", f"已排期任务 '{task['name']}'，Cron 表达式: '{task['cron']}'")
        except Exception as e:
            manager.add_log("系统", f"排期失败 '{task['name']}': {str(e)}")

# Initial load
reload_jobs()
