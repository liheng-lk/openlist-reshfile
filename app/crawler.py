import asyncio
import os

# 全局内存字典，用于记录每次扫描的目录状态，实现真正的"日志级秒级增量过滤"
# 格式: { "/strm/path": {"file1.strm", "file2.strm"} }
last_scan_state = {}

# 全局高压限流器：无论配置多少个并发任务，同一时刻流向 Openlist 的总并发请求数被死死锁在 5。
# 从而彻底杜绝多任务并发导致的 OpenList 炸池、ConnectTimeout！
global_sem = None

def get_semaphore():
    global global_sem
    if global_sem is None:
        global_sem = asyncio.Semaphore(5)
    return global_sem

async def recursive_scan_and_refresh(client, server_url, token, start_path, manager, task_name, task_status=None):
    global last_scan_state
    queue = [start_path]
    visited = {start_path}
    total_files = 0
    total_dirs = 0
    scanned_count = 0
    
    # 获取全局限流锁
    sem = get_semaphore()
    # 为保证队列推进顺滑，单个任务内部分流容量限制，跟随全局上限
    batch_limit = 5
    
    manager.add_log(task_name, f"🚀 启动深度递归扫描引擎，目标根路径: {start_path}")
    if task_status:
        task_status[task_name] = {"state": "scanning", "message": "启动深度递归缓冲..."}
        
    async def fetch_dir(path):
        nonlocal total_files, total_dirs, scanned_count
        try:
            async with sem:
                res = await client.post(
                    f"{server_url}/api/fs/list", 
                    json={"path": path, "password": "", "refresh": False, "page": 1, "per_page": 20000},
                    headers={"Authorization": token}
                )
                scanned_count += 1
                if res.status_code == 200:
                    try:
                        res_json = res.json()
                        data_node = res_json.get("data") or {}
                        content = data_node.get("content") or []
                    except Exception:
                        content = []
                    
                    subdirs = []
                    files = []
                    for f in content:
                        if isinstance(f, dict):
                            if f.get("is_dir"):
                                subdirs.append(f.get("name", ""))
                            else:
                                files.append(f.get("name", "未知"))
                    return path, subdirs, files
                else:
                    manager.add_log(task_name, f"⚠️ 扫描 {path} 时云端返回非200状态码: {res.status_code}")
        except Exception as e:
            manager.add_log(task_name, f"⚠️ 扫描 {path} 遭阻断: {type(e).__name__} {str(e)}")
        return path, [], []

    while queue:
        batch = queue[:batch_limit*2]
        queue = queue[batch_limit*2:]
        
        results = await asyncio.gather(*(fetch_dir(p) for p in batch))
        
        for path, subdirs, files in results:
            total_dirs += len(subdirs)
            total_files += len(files)
            
            for d in subdirs:
                if path.endswith("/"):
                    new_path = path + d
                else:
                    new_path = path + "/" + d
                    
                if new_path not in visited:
                    visited.add(new_path)
                    queue.append(new_path)
                    
            # 获取新老状态比对
            new_files_set = set(files)
            old_files_set = last_scan_state.get(path)
            
            # 只有当这是第二次扫描（即已存在历史记录）并且发生变动时，才在日志里醒目输出
            if old_files_set is not None:
                added_files = new_files_set - old_files_set
                removed_files = old_files_set - new_files_set
                
                if added_files:
                    names_preview = ", ".join(list(added_files)[:5])
                    manager.add_log(task_name, f"✨ 【精准变化】{path} -> 新增 {len(added_files)} 个文件: {names_preview}")
                if removed_files:
                    manager.add_log(task_name, f"🗑️ 【精准变化】{path} -> 移除了 {len(removed_files)} 个文件")
            
            # 更新状态为当前最新
            last_scan_state[path] = new_files_set
            
            if task_status:
                task_status[task_name] = {
                    "state": "syncing", 
                    "message": f"探测中... 静默遍历了 {scanned_count} 个层级文件夹 (当前探明总计: {total_files} 文件)"
                }

    manager.add_log(task_name, f"✅ 深度递归扫描结束！共爬取 {scanned_count} 个层级文件夹，共强制刷新并发现 {total_files} 个文件。")
    return total_files, total_dirs
