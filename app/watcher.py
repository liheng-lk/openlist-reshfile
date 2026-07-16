import asyncio
import contextlib
import os
import posixpath
import time
from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Set

import httpx
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver


WATCH_EVENT_TYPES = {"created", "deleted", "modified", "moved"}
MAX_PATHS_PER_BATCH = 80


@dataclass(frozen=True)
class WatchTask:
    name: str
    remote_root: str
    local_root: str
    mode: str
    debounce_seconds: int


class _LocalChangeHandler(FileSystemEventHandler):
    def __init__(self, service: "IncrementalSyncWatcher", task_name: str):
        self.service = service
        self.task_name = task_name

    def on_any_event(self, event: FileSystemEvent):
        if event.event_type not in WATCH_EVENT_TYPES:
            return

        self.service.enqueue_local_event(
            self.task_name,
            event.src_path,
            event.is_directory,
            event.event_type,
        )

        dest_path = getattr(event, "dest_path", None)
        if dest_path:
            self.service.enqueue_local_event(
                self.task_name,
                dest_path,
                event.is_directory,
                event.event_type,
            )


class IncrementalSyncWatcher:
    def __init__(self, manager):
        self.manager = manager
        self.task_status: Optional[Dict[str, dict]] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.queue: Optional[asyncio.Queue] = None
        self.worker_task: Optional[asyncio.Task] = None
        self.tasks: Dict[str, WatchTask] = {}
        self.observers: Dict[str, object] = {}

    def start(self, task_status: Dict[str, dict]):
        self.task_status = task_status
        self.loop = asyncio.get_running_loop()
        self.queue = asyncio.Queue()
        self.worker_task = asyncio.create_task(self._worker())
        self.reload_config()

    async def stop(self):
        self._stop_observers()
        if self.worker_task:
            self.worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.worker_task
        self.worker_task = None
        self.queue = None
        self.loop = None

    def reload_config(self):
        self._stop_observers()
        self.tasks = {}

        config = self.manager.get_config()
        for raw_task in config.get("tasks", []):
            watch_task = self._build_watch_task(raw_task)
            if not watch_task:
                continue
            self._start_observer(watch_task)

    def enqueue_local_event(self, task_name: str, path: str, is_directory: bool, event_type: str):
        if not self.loop or not self.queue:
            return
        self.loop.call_soon_threadsafe(
            self.queue.put_nowait,
            (task_name, path, is_directory, event_type, time.monotonic()),
        )

    def _build_watch_task(self, raw_task: dict) -> Optional[WatchTask]:
        if not raw_task.get("enabled", True) or not raw_task.get("watch_enabled", False):
            return None

        local_root = (raw_task.get("local_watch_path") or "").strip()
        remote_root = (raw_task.get("mount_path") or "").strip()
        name = (raw_task.get("name") or "").strip()
        if not name or not local_root or not remote_root:
            return None

        local_root = os.path.abspath(os.path.expanduser(local_root))
        if not os.path.isdir(local_root):
            self.manager.add_log(name, f"增量监听未启动：容器内路径不存在或不可访问: {local_root}")
            if self.task_status is not None:
                self.task_status[name] = {
                    "state": "error",
                    "message": "监听目录不可访问",
                }
            return None

        mode = (raw_task.get("watch_mode") or "polling").lower()
        if mode not in {"native", "polling"}:
            mode = "polling"

        try:
            debounce_seconds = int(raw_task.get("debounce_seconds", 5))
        except (TypeError, ValueError):
            debounce_seconds = 5
        debounce_seconds = max(1, min(debounce_seconds, 300))

        return WatchTask(
            name=name,
            remote_root=_normalize_remote_path(remote_root),
            local_root=local_root,
            mode=mode,
            debounce_seconds=debounce_seconds,
        )

    def _start_observer(self, watch_task: WatchTask):
        observer = PollingObserver(timeout=5) if watch_task.mode == "polling" else Observer()
        try:
            observer.schedule(
                _LocalChangeHandler(self, watch_task.name),
                watch_task.local_root,
                recursive=True,
            )
            observer.start()
        except Exception as exc:
            self.manager.add_log(
                watch_task.name,
                f"增量监听启动失败: {type(exc).__name__} {exc}",
            )
            if self.task_status is not None:
                self.task_status[watch_task.name] = {
                    "state": "error",
                    "message": "增量监听启动失败",
                }
            return
        self.tasks[watch_task.name] = watch_task
        self.observers[watch_task.name] = observer
        self.manager.add_log(
            watch_task.name,
            f"增量监听已启动：{watch_task.local_root} -> {watch_task.remote_root} ({watch_task.mode})",
        )
        if self.task_status is not None:
            self.task_status[watch_task.name] = {
                "state": "watching",
                "message": "增量监听中",
            }

    def _stop_observers(self):
        for observer in self.observers.values():
            observer.stop()
        for observer in self.observers.values():
            observer.join(timeout=5)
        self.observers = {}

    async def _worker(self):
        pending: Dict[str, dict] = {}
        while True:
            timeout = self._next_wait_seconds(pending)
            try:
                item = await asyncio.wait_for(self.queue.get(), timeout=timeout)
            except asyncio.TimeoutError:
                item = None

            if item:
                task_name, local_path, is_directory, event_type, event_time = item
                watch_task = self.tasks.get(task_name)
                if watch_task:
                    remote_paths = self._map_event_to_remote_paths(
                        watch_task,
                        local_path,
                        is_directory,
                        event_type,
                    )
                    if remote_paths:
                        bucket = pending.setdefault(
                            task_name,
                            {
                                "paths": set(),
                                "last_event": event_time,
                                "debounce": watch_task.debounce_seconds,
                            },
                        )
                        bucket["paths"].update(remote_paths)
                        bucket["last_event"] = event_time
                        bucket["debounce"] = watch_task.debounce_seconds
                        if self.task_status is not None:
                            self.task_status[task_name] = {
                                "state": "syncing",
                                "message": f"已捕获 {len(bucket['paths'])} 个待同步目录",
                            }

            await self._flush_ready(pending)

    def _next_wait_seconds(self, pending: Dict[str, dict]) -> float:
        if not pending:
            return 1.0
        now = time.monotonic()
        next_due = min(
            bucket["last_event"] + bucket["debounce"]
            for bucket in pending.values()
        )
        return max(0.2, min(1.0, next_due - now))

    async def _flush_ready(self, pending: Dict[str, dict]):
        now = time.monotonic()
        ready_names = [
            task_name
            for task_name, bucket in pending.items()
            if now - bucket["last_event"] >= bucket["debounce"]
        ]
        for task_name in ready_names:
            bucket = pending.pop(task_name)
            paths = sorted(bucket["paths"])
            watch_task = self.tasks.get(task_name)
            if not watch_task:
                continue
            await self._sync_remote_paths(watch_task, paths)

    def _map_event_to_remote_paths(
        self,
        watch_task: WatchTask,
        local_path: str,
        is_directory: bool,
        event_type: str,
    ) -> Set[str]:
        local_path = os.path.abspath(local_path)
        if not _is_relative_to(local_path, watch_task.local_root):
            return set()

        if is_directory and event_type == "modified":
            index_local_path = local_path
        else:
            index_local_path = os.path.dirname(local_path)

        if not _is_relative_to(index_local_path, watch_task.local_root):
            index_local_path = watch_task.local_root

        rel_path = os.path.relpath(index_local_path, watch_task.local_root)
        if rel_path == ".":
            return {watch_task.remote_root}

        remote_path = posixpath.join(
            watch_task.remote_root,
            rel_path.replace(os.sep, "/"),
        )
        return {_normalize_remote_path(remote_path)}

    async def _sync_remote_paths(self, watch_task: WatchTask, paths: Iterable[str]):
        paths = list(dict.fromkeys(paths))
        if len(paths) > MAX_PATHS_PER_BATCH:
            self.manager.add_log(
                watch_task.name,
                f"增量变更目录过多({len(paths)})，自动合并为根目录局部刷新",
            )
            paths = [watch_task.remote_root]

        config = self.manager.get_config()
        server_url = (config.get("server_url") or "").rstrip("/")
        username = config.get("username")
        password = config.get("password")
        if not server_url or not username or not password:
            self.manager.add_log(watch_task.name, "增量同步失败：OpenList 凭据不完整")
            if self.task_status is not None:
                self.task_status[watch_task.name] = {
                    "state": "error",
                    "message": "OpenList 凭据不完整",
                }
            return

        try:
            async with httpx.AsyncClient(timeout=90.0) as client:
                login_res = await client.post(
                    f"{server_url}/api/auth/login",
                    json={"username": username, "password": password},
                )
                token = login_res.json().get("data", {}).get("token")
                if not token:
                    raise RuntimeError("登录失败，未获取到 token")

                headers = {"Authorization": token}
                success = 0
                for remote_path in paths:
                    await client.post(
                        f"{server_url}/api/fs/list",
                        json={
                            "path": remote_path,
                            "password": "",
                            "refresh": True,
                            "page": 1,
                            "per_page": 1,
                        },
                        headers=headers,
                    )
                    index_res = await client.post(
                        f"{server_url}/api/admin/index/update",
                        params={"path": remote_path},
                        headers=headers,
                    )
                    if index_res.status_code == 200:
                        success += 1
                    else:
                        self.manager.add_log(
                            watch_task.name,
                            f"增量索引失败: {remote_path} -> HTTP {index_res.status_code}",
                        )

                self.manager.add_log(
                    watch_task.name,
                    f"增量同步完成：已刷新 {success}/{len(paths)} 个目录",
                )
                if self.task_status is not None:
                    self.task_status[watch_task.name] = {
                        "state": "watching",
                        "message": f"增量监听中，刚刷新 {success} 个目录",
                    }
        except Exception as exc:
            self.manager.add_log(watch_task.name, f"增量同步异常: {type(exc).__name__} {exc}")
            if self.task_status is not None:
                self.task_status[watch_task.name] = {
                    "state": "error",
                    "message": f"增量同步异常: {type(exc).__name__}",
                }


def _normalize_remote_path(path: str) -> str:
    normalized = (path or "/").replace("\\", "/")
    if not normalized.startswith("/"):
        normalized = "/" + normalized
    normalized = posixpath.normpath(normalized)
    return "/" if normalized == "." else normalized


def _is_relative_to(path: str, root: str) -> bool:
    try:
        return os.path.commonpath([path, root]) == root
    except ValueError:
        return False
