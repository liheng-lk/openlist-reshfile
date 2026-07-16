# OpenList AutoFile (OpenList 增量同步守护核心)

OpenList AutoFile 是一款基于 Python 的高性能、超极速本地目录状态观测探针，通过异步引擎深度贴合 OpenList (AList) 的 `strm` 等高级缓存映射驱动机制，实现零网络卡顿的原生“质询同步”。

## 特性 (Features)
- 🚀 **极速无感探测**：完美规避 HTTP 高并发 Timeout 锁死，限制本地 `fs/list` 并发。
- 👁️ **远端快照增量 (Remote Snapshot Diff)**：持久化目录快照到 `/config/state`，下一轮只深入疑似变化的子树。
- ⚡ **局部索引刷新**：检测到变化后只对变化目录触发 OpenList `admin/index/update`，变化过多时自动合并为根目录刷新。
- 📡 **容器可见路径/WebDAV 可选监听**：如果把同一个 WebDAV、FUSE 或宿主机目录挂进本容器，可用轮询事件快速触发局部同步。
- 🛡️ **全局异步阻塞锁 (Global Semaphore)**：不管你在系统中拉起多少个多并发重叠任务，核心底层统一管理并自动削峰排队。
- ☁️ **官方跨维更新**：探测并强制激活 OpenList 本地的 "懒加载" 生成，再下探调用 `admin/index/update` 将 Bleve/DB 索引极速锁死更新。
- 🌍 **零配置跨端网页**：前端自带完善的交互式 UI 面板，支持查看全局事件流和控制各频道并发计划。

## 增量同步模式

### 远端快照增量（推荐）
适合 OpenList 的真实目录只存在于 OpenList 容器内，或者本地目录必须等 OpenList 扫描后才会出现新增文件的场景。

第一次运行会完整扫描目标路径并建立基线；之后每轮会先比较目录快照，跳过未变化的子树，只刷新发生变化的目录。需要彻底重扫时，在任务列表点击“重建基线”。

### 容器可见路径/WebDAV 挂载监听（可选）
只有当挂载路径对 `openlist-autofile` 容器可见时才有效。路径不要求是宿主机真实磁盘，WebDAV、FUSE、rclone 等网络挂载也可以；这类目录建议使用默认的 `polling` 模式。

如果目录只在 OpenList 容器内部，本工具无法直接收到文件事件；这种情况使用远端快照增量即可。任务里的“同步路径”填写 OpenList 路径，例如 `/strm/PT/国产剧`；“容器内可见路径”填写本工具容器里能读到的路径，例如 `/watch/strm/PT/国产剧`。

## 部署运行
1. 通过 `docker-compose.yml` 安装：
```yaml
version: '3.8'
services:
  openlist-autofile:
    image: liheng6668/openlist-autofile:latest
    container_name: openlist-autofile
    restart: unless-stopped
    ports:
      - "8081:8000"
    volumes:
      - ./config:/config
      # 可选：启用容器可见路径/WebDAV 监听时挂载
      # - /your/webdav/strm:/watch/strm:ro
    environment:
      - TZ=Asia/Shanghai
      - TOOL_PASSWORD=admin888
```
2. 启动后，在浏览器访问 `http://ip:8081` 即可进入管理页面。
