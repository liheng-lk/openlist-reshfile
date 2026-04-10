# OpenList AutoFile (OpenList 增量同步守护核心)

OpenList AutoFile 是一款基于 Python 的高性能、超极速本地目录状态观测探针，通过异步引擎深度贴合 OpenList (AList) 的 `strm` 等高级缓存映射驱动机制，实现零网络卡顿的原生“质询同步”。

## 特性 (Features)
- 🚀 **极速无感探测**：完美规避 HTTP 高并发 Timeout 锁死，限制本地 `fs/list` 并发。
- 👁️ **状态记忆过滤器 (State Diff Tracker)**：自动比较上一秒与当前秒的系统状态，绝不刷屏式倾倒日志。
- 🛡️ **全局异步阻塞锁 (Global Semaphore)**：不管你在系统中拉起多少个多并发重叠任务，核心底层统一管理并自动削峰排队。
- ☁️ **官方跨维更新**：探测并强制激活 OpenList 本地的 "懒加载" 生成，再下探调用 `admin/index/update` 将 Bleve/DB 索引极速锁死更新。
- 🌍 **零配置跨端网页**：前端自带完善的交互式 UI 面板，支持查看全局事件流和控制各频道并发计划。

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
```
2. 启动后，在浏览器访问 `http://ip:8081` 即可进入管理页面。
