import asyncio
import hashlib
import json
import os
import posixpath
from collections import deque
from typing import Dict, Iterable, List, Set, Tuple

import httpx

from .crawler import get_semaphore
from .manager import base_dir


STATE_DIR = os.path.join(base_dir, "state")
STATE_PATH = os.path.join(STATE_DIR, "remote_snapshots.json")
MAX_INDEX_PATHS = 80
REMOTE_RETRIES = 3
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
_state_lock = asyncio.Lock()
_task_locks: Dict[str, asyncio.Lock] = {}


def _ensure_state_dir():
    os.makedirs(STATE_DIR, exist_ok=True)


def _load_state() -> dict:
    _ensure_state_dir()
    if not os.path.exists(STATE_PATH):
        return {"version": 1, "tasks": {}}
    try:
        with open(STATE_PATH, "r") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {"version": 1, "tasks": {}}


def _save_state(state: dict):
    _ensure_state_dir()
    tmp_path = f"{STATE_PATH}.tmp"
    with open(tmp_path, "w") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    os.replace(tmp_path, STATE_PATH)


async def remote_incremental_scan_and_refresh(
    client,
    server_url: str,
    token: str,
    task: dict,
    manager,
    task_status=None,
    force_full: bool = False,
):
    task_name = task["name"]
    root_path = _normalize_remote_path(task["mount_path"])
    state_key = _task_state_key(task_name, root_path)
    task_lock = _get_task_lock(state_key)
    async with task_lock:
        return await _remote_incremental_scan_and_refresh_locked(
            client,
            server_url,
            token,
            task,
            manager,
            task_status,
            force_full,
            task_name,
            root_path,
            state_key,
        )


async def _remote_incremental_scan_and_refresh_locked(
    client,
    server_url: str,
    token: str,
    task: dict,
    manager,
    task_status,
    force_full: bool,
    task_name: str,
    root_path: str,
    state_key: str,
):
    async with _state_lock:
        state = _load_state()
        task_state = state.setdefault("tasks", {}).get(state_key, {})
    old_dirs = task_state.get("directories") or {}
    has_baseline = bool(old_dirs)
    full_scan = force_full or not has_baseline
    deep_check_limit = _bounded_int(task.get("deep_check_limit", 200), 0, 5000)
    deep_cursor = int(task_state.get("deep_cursor", 0) or 0)

    queue = deque([root_path])
    visited = {root_path}
    scanned_dirs = 0
    skipped_dirs = 0
    scanned_files = 0
    changed_paths: Set[str] = set()
    scanned_nodes: Dict[str, dict] = {}
    removed_subtrees: Set[str] = set()

    if full_scan:
        manager.add_log(task_name, f"启动远端基线扫描：{root_path}")
    else:
        deep_check_paths, deep_cursor = _next_deep_check_paths(
            old_dirs,
            root_path,
            deep_cursor,
            deep_check_limit,
        )
        for check_path in deep_check_paths:
            if check_path not in visited:
                visited.add(check_path)
                queue.append(check_path)
        manager.add_log(
            task_name,
            f"启动远端快照增量巡检：{root_path}，本轮深巡 {len(deep_check_paths)} 个目录",
        )

    if task_status is not None:
        task_status[task_name] = {
            "state": "scanning",
            "message": "正在进行远端增量巡检...",
        }

    while queue:
        path = queue.popleft()
        node = await _fetch_dir_node(client, server_url, token, path, manager, task_name)
        scanned_dirs += 1
        scanned_files += node["file_count"]
        scanned_nodes[path] = node

        old_node = old_dirs.get(path)
        node_changed = old_node is None or node["signature"] != old_node.get("signature")
        if node_changed:
            changed_paths.add(path)
            for removed_path in _removed_child_paths(old_node, node):
                removed_subtrees.add(removed_path)

        if full_scan or node_changed:
            child_paths = node["child_dirs"]
        else:
            child_paths = _changed_child_paths(old_node, node)
            skipped_dirs += max(len(node["child_dirs"]) - len(child_paths), 0)

        for child_path in child_paths:
            if child_path not in visited:
                visited.add(child_path)
                queue.append(child_path)

        if task_status is not None:
            task_status[task_name] = {
                "state": "syncing",
                "message": f"已扫描 {scanned_dirs} 个目录，文件 {scanned_files} 个，跳过 {skipped_dirs} 个未变子树",
                "scanned_dirs": scanned_dirs,
                "scanned_files": scanned_files,
                "skipped_dirs": skipped_dirs,
                "changed_dirs": len(changed_paths),
            }

    merged_dirs = _merge_directory_state(old_dirs, scanned_nodes, removed_subtrees)
    known_files = _count_known_files(merged_dirs)
    known_dirs = len(merged_dirs)
    async with _state_lock:
        latest_state = _load_state()
        latest_state.setdefault("tasks", {})[state_key] = {
            "task_name": task_name,
            "root_path": root_path,
            "deep_cursor": 0 if full_scan else deep_cursor,
            "directories": merged_dirs,
        }
        _save_state(latest_state)

    index_paths = [root_path] if full_scan else _collapse_index_paths(root_path, changed_paths)
    refreshed = await _refresh_indexes(client, server_url, token, index_paths, manager, task_name)

    if task_status is not None:
        task_status[task_name] = {
            "state": "done",
            "message": f"增量完成，文件 {known_files} 个，扫描 {scanned_dirs} 个目录，刷新 {refreshed} 个索引",
            "files": known_files,
            "known_dirs": known_dirs,
            "scanned_dirs": scanned_dirs,
            "scanned_files": scanned_files,
            "skipped_dirs": skipped_dirs,
            "changed_dirs": len(changed_paths),
            "refreshed": refreshed,
        }

    if full_scan:
        manager.add_log(
            task_name,
            f"远端基线扫描完成：扫描 {scanned_dirs} 个目录，发现 {known_files} 个文件，已刷新根索引",
        )
    else:
        manager.add_log(
            task_name,
            f"远端增量巡检完成：已知 {known_files} 个文件，扫描 {scanned_dirs} 个目录，变化 {len(changed_paths)} 个目录，跳过 {skipped_dirs} 个未变子树，刷新 {refreshed} 个目录",
        )

    return known_files, scanned_dirs


async def _fetch_dir_node(client, server_url: str, token: str, path: str, manager, task_name: str) -> dict:
    res = await _post_openlist(
        client,
        f"{server_url}/api/fs/list",
        manager,
        task_name,
        f"扫描 {path}",
        json={
            "path": path,
            "password": "",
            "refresh": True,
            "page": 1,
            "per_page": 20000,
        },
        headers={"Authorization": token},
    )
    if res.status_code != 200:
        raise RuntimeError(f"扫描 {path} 失败: HTTP {res.status_code}")

    data_node = (res.json().get("data") or {})
    content = data_node.get("content") or []
    entries = [_entry_snapshot(item) for item in content if isinstance(item, dict)]
    entries.sort(key=lambda item: (not item["is_dir"], item["name"]))

    child_dirs = []
    child_signatures = {}
    for entry in entries:
        if not entry["is_dir"]:
            continue
        child_path = _join_remote(path, entry["name"])
        child_dirs.append(child_path)
        child_signatures[child_path] = entry["signature"]

    signature = _hash_json(entries)
    return {
        "signature": signature,
        "child_dirs": child_dirs,
        "child_signatures": child_signatures,
        "file_count": sum(1 for entry in entries if not entry["is_dir"]),
    }


async def _refresh_indexes(
    client,
    server_url: str,
    token: str,
    paths: Iterable[str],
    manager,
    task_name: str,
) -> int:
    headers = {"Authorization": token}
    success = 0
    for path in paths:
        res = await _post_openlist(
            client,
            f"{server_url}/api/admin/index/update",
            manager,
            task_name,
            f"刷新索引 {path}",
            params={"path": path},
            headers=headers,
        )
        if res.status_code == 200:
            success += 1
        else:
            manager.add_log(task_name, f"远端索引刷新失败: {path} -> HTTP {res.status_code}")
    return success


async def _post_openlist(client, url: str, manager, task_name: str, action: str, **kwargs):
    last_exc = None
    for attempt in range(1, REMOTE_RETRIES + 1):
        try:
            async with get_semaphore():
                res = await client.post(url, **kwargs)
            if res.status_code not in RETRYABLE_STATUS_CODES or attempt == REMOTE_RETRIES:
                return res
            manager.add_log(
                task_name,
                f"{action} 返回 HTTP {res.status_code}，准备第 {attempt + 1}/{REMOTE_RETRIES} 次重试",
            )
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            last_exc = exc
            if attempt == REMOTE_RETRIES:
                raise
            manager.add_log(
                task_name,
                f"{action} 网络超时/中断: {type(exc).__name__}，准备第 {attempt + 1}/{REMOTE_RETRIES} 次重试",
            )

        await asyncio.sleep(2 * attempt)

    if last_exc:
        raise last_exc
    raise RuntimeError(f"{action} 失败")


def _entry_snapshot(item: dict) -> dict:
    snapshot = {
        "name": item.get("name", ""),
        "is_dir": bool(item.get("is_dir")),
        "size": item.get("size", 0),
        "modified": item.get("modified") or item.get("updated_at") or "",
        "sign": item.get("sign") or "",
        "hashinfo": item.get("hash_info") or item.get("hashinfo") or "",
    }
    snapshot["signature"] = _hash_json(snapshot)
    return snapshot


def _changed_child_paths(old_node: dict, new_node: dict) -> List[str]:
    old_child_signatures = old_node.get("child_signatures") or {}
    return [
        child_path
        for child_path in new_node["child_dirs"]
        if old_child_signatures.get(child_path) != new_node["child_signatures"].get(child_path)
    ]


def _removed_child_paths(old_node: dict, new_node: dict) -> Set[str]:
    if not old_node:
        return set()
    old_children = set(old_node.get("child_dirs") or [])
    new_children = set(new_node.get("child_dirs") or [])
    return old_children - new_children


def _merge_directory_state(
    old_dirs: Dict[str, dict],
    scanned_nodes: Dict[str, dict],
    removed_subtrees: Set[str],
) -> Dict[str, dict]:
    merged = dict(old_dirs)
    for removed_path in removed_subtrees:
        for known_path in list(merged.keys()):
            if known_path == removed_path or known_path.startswith(f"{removed_path}/"):
                merged.pop(known_path, None)
    merged.update(scanned_nodes)
    return merged


def _collapse_index_paths(root_path: str, changed_paths: Set[str]) -> List[str]:
    if not changed_paths:
        return []
    if len(changed_paths) > MAX_INDEX_PATHS:
        return [root_path]

    collapsed: List[str] = []
    for path in sorted(changed_paths, key=lambda item: (item.count("/"), item)):
        if any(path == parent or path.startswith(f"{parent}/") for parent in collapsed):
            continue
        collapsed.append(path)
    return collapsed


def _get_task_lock(state_key: str) -> asyncio.Lock:
    lock = _task_locks.get(state_key)
    if lock is None:
        lock = asyncio.Lock()
        _task_locks[state_key] = lock
    return lock


def _count_known_files(directories: Dict[str, dict]) -> int:
    return sum(int(node.get("file_count") or 0) for node in directories.values())


def _next_deep_check_paths(
    old_dirs: Dict[str, dict],
    root_path: str,
    cursor: int,
    limit: int,
) -> Tuple[List[str], int]:
    if limit <= 0:
        return [], cursor

    known_paths = sorted(path for path in old_dirs.keys() if path != root_path)
    if not known_paths:
        return [], 0

    cursor = cursor % len(known_paths)
    take = min(limit, len(known_paths))
    paths = [known_paths[(cursor + offset) % len(known_paths)] for offset in range(take)]
    return paths, (cursor + take) % len(known_paths)


def _bounded_int(value, minimum: int, maximum: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        number = minimum
    return max(minimum, min(number, maximum))


def _task_state_key(task_name: str, root_path: str) -> str:
    return hashlib.sha256(f"{task_name}:{root_path}".encode()).hexdigest()


def _hash_json(value) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode()).hexdigest()


def _normalize_remote_path(path: str) -> str:
    normalized = (path or "/").replace("\\", "/")
    if not normalized.startswith("/"):
        normalized = "/" + normalized
    normalized = posixpath.normpath(normalized)
    return "/" if normalized == "." else normalized


def _join_remote(parent: str, child: str) -> str:
    return _normalize_remote_path(posixpath.join(parent, child))
