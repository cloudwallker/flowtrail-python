# FlowTrail Python

[English](README.md) | [简体中文](README_ZH.md)

**基于 Python、FastAPI 和 React 的单机 AI 任务平台，支持租户鉴权、DAG 编排、检查点恢复、事件回放与对象配额管理。** 可以提交文本处理、只读 HTTP 和推理任务，在控制台观察运行过程，并按租户管理小型文件对象。

**English:** A Python AI task platform with tenant-scoped access, recoverable DAG execution, and a React operations console.

![FlowTrail 实际控制台中的 mock 推理工作流](docs/images/console.jpg)

*实际控制台截图。演示使用明确标记的 mock 推理；任务执行、持久化和事件回放来自实际应用运行时。*

## 项目能力

| 能力 | 当前实现 |
| --- | --- |
| 租户鉴权 | 服务端随机生成 Bearer token，绑定租户和角色；数据库只保存 SHA-256 摘要 |
| 工作流执行 | 校验后的 DAG，支持 text、http、inference 节点、有界并发和节点截止时间 |
| 中断恢复 | 成功节点检查点跨重启保留；未完成节点以新的 attempt 再次执行 |
| 外部系统接入 | HTTP 节点选择服务端配置的精确 GET 地址；推理节点使用固定的兼容模型配置 |
| 事件回放 | SQLite 持久化 SSE 事件，支持运行内序号和 Last-Event-ID |
| 对象管理 | 按租户隔离 SQLite BLOB，对象写入与字节配额检查使用同一事务 |
| React 控制台 | 工作流模板、运行详情、事件历史、对象上传下载及配额展示 |

本项目延续 [FlowTrail Server](https://github.com/cloudwallker/flowtrail-server) 的工作流设计，用 Python 实现适合本地复现的平台接口、自动化、推理接入和权限管理闭环。

## 快速开始

需要 **Python 3.11+**、**Node.js 22.12+**。在本仓库根目录执行命令，建议先创建并激活虚拟环境。

```sh
python -m venv .venv
# Linux/macOS：source .venv/bin/activate
# PowerShell：.\.venv\Scripts\Activate.ps1
python -m pip install -e '.[test]'

cd frontend
npm ci
npm run build
cd ..

python -m flowtrail bootstrap --tenant demo --role admin
python -m flowtrail serve --host 127.0.0.1 --port 8000
```

`bootstrap` 会输出一个新生成的 token，请在本地保存，然后打开 **http://127.0.0.1:8000** 并粘贴到控制台。项目没有内置账号或默认 token。浏览器仅在内存中保存 token，刷新页面后需要重新输入。

后端启动时若存在 `frontend/dist`，就会提供控制台首页。因此应先构建前端再启动后端；后构建时需要重启后端。默认数据库是当前工作目录下的 `data/flowtrail.sqlite3`。

开发前端时，保持后端运行在 8000 端口，在另一个终端执行：

```sh
cd frontend
npm ci
npm run dev
```

打开 **http://127.0.0.1:5173**。Vite 将 `/api` 和 `/healthz` 代理到后端，实际鉴权始终由后端完成。接口文档位于 **http://127.0.0.1:8000/docs**。

## 提交与查看工作流

先将本地 `bootstrap` 输出的 token 设置到当前终端的 `FLOWTRAIL_API_TOKEN` 环境变量。下面是 POSIX shell 的 curl 示例，不包含真实凭据。

```sh
curl --fail-with-body http://127.0.0.1:8000/api/me \
  -H "Authorization: Bearer $FLOWTRAIL_API_TOKEN"

curl --fail-with-body -i http://127.0.0.1:8000/api/runs \
  -H "Authorization: Bearer $FLOWTRAIL_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: report-demo-1' \
  --data-binary @examples/report-workflow.json
```

提交返回 **202 Accepted**、`Location` 响应头以及包含 `id` 的运行对象。同一租户重复提交相同幂等键与相同请求会返回原运行；幂等键相同但请求改变时返回 409。

```sh
RUN_ID=replace-with-the-returned-run-id

curl --fail-with-body "http://127.0.0.1:8000/api/runs/$RUN_ID" \
  -H "Authorization: Bearer $FLOWTRAIL_API_TOKEN"

curl --fail-with-body "http://127.0.0.1:8000/api/runs/$RUN_ID/events?follow=false" \
  -H "Authorization: Bearer $FLOWTRAIL_API_TOKEN"

curl --fail-with-body "http://127.0.0.1:8000/api/runs/$RUN_ID/events?follow=false" \
  -H "Authorization: Bearer $FLOWTRAIL_API_TOKEN" \
  -H 'Last-Event-ID: 3'
```

`follow=false` 返回现有事件后关闭连接。默认跟随模式等待新事件，在运行结束或连接约一分钟后关闭；后续可携带最后序号重新连接，查看继续执行产生的事件。

[报告示例](examples/report-workflow.json) 包含准备节点、两个独立的 mock 推理分支和一个报告汇合节点。模板支持 `${inputs.topic}` 和祖先节点的 `${nodeId.output}`。替换只执行一遍，并逐段检查 UTF-8 字节预算，不执行表达式。

## API 与角色权限

业务接口统一使用 `Authorization: Bearer …`。租户由 token 决定，客户端不能通过请求字段或查询参数指定其他租户。访问其他租户的运行、事件或对象会返回 404。

| API | 作用 | 可用角色 |
| --- | --- | --- |
| `GET /api/me` | 当前租户及角色 | 全部 |
| `GET /api/runs` | 当前租户最近 100 个运行 | 全部 |
| `POST /api/runs` | 提交工作流与输入 | admin、operator |
| `GET /api/runs/{id}` | 定义、节点尝试、输出和状态 | 全部 |
| `POST /api/runs/{id}/resume` | 恢复 failed/interrupted 运行 | admin、operator |
| `GET /api/runs/{id}/events` | SSE 历史回放与跟随 | 全部 |
| `GET /api/objects` | 列出租户对象 | 全部 |
| `PUT /api/objects/{name}` | 使用原始字节新建或替换对象 | admin、operator |
| `GET /api/objects/{name}` | 下载对象正文 | 全部 |
| `DELETE /api/objects/{name}` | 删除对象 | admin、operator |
| `GET /api/quota` | 已用、上限和剩余对象字节数 | 全部 |
| `GET /healthz` | 公开健康检查 | 无需 token |

当前 `admin` 与 `operator` 的 API 写权限相同，`viewer` 只读。token 由本地管理 CLI 签发；例如 `python -m flowtrail bootstrap --tenant demo --role viewer` 可创建同租户的只读 token。

对象名称使用 ASCII 字母、数字、点、连字符或下划线，并以字母或数字开头。默认单对象上限 **5 MiB**，租户对象配额 **10 MiB**。替换对象时，在同一 SQLite 事务内按新大小核算。配额只统计对象正文，不代表数据库文件、索引、工作流历史或文件系统的磁盘占用。

## 服务端配置

配置从进程环境变量读取，不自动加载 `.env` 文件。

| 环境变量 | 默认值或用途 |
| --- | --- |
| `FLOWTRAIL_DB` | `data/flowtrail.sqlite3` |
| `FLOWTRAIL_QUOTA_BYTES` | `10485760`；新建租户的初始配额 |
| `FLOWTRAIL_MAX_OBJECT_BYTES` | `5242880` |
| `FLOWTRAIL_MAX_PENDING_RUNS` | `100`；全局 queued 与 running 总量上限 |
| `FLOWTRAIL_MAX_PARALLEL_RUNS` | `4` |
| `FLOWTRAIL_MAX_PARALLEL_NODES` | `8`；全局节点并发 |
| `FLOWTRAIL_PER_RUN_PARALLELISM` | `4` |
| `FLOWTRAIL_CONNECTORS_JSON` | `{}`；服务端控制的精确 GET 地址及可选请求头 |
| `FLOWTRAIL_MODEL_BASE_URL` | OpenAI 兼容基础地址，通常以 `/v1` 结尾 |
| `FLOWTRAIL_MODEL_NAME` | 固定的服务端模型名称 |
| `FLOWTRAIL_MODEL_API_KEY` | 通过本地环境变量提供的服务端密钥 |
| `FLOWTRAIL_FRONTEND_DIR` | `frontend/dist` |

运行 [HTTP 示例](examples/http-workflow.json) 前，可配置一个只读健康检查连接器，再启动服务：

```sh
export FLOWTRAIL_CONNECTORS_JSON='{"demo-health":{"url":"http://127.0.0.1:8000/healthz"}}'
python -m flowtrail serve
```

HTTP 节点仅选择 `{"connector":"demo-health"}`，不能提交 URL、方法、请求头或路径。实际请求使用 GET，禁用重定向和环境代理，并限制响应大小与执行时间。配置连接器时，应选择可安全重复请求的只读 GET 接口。

推理节点默认使用 `"mode":"mock"`，返回明确带有 mock 标记的确定性结果。使用 `"mode":"live"` 时，必须设置三个模型环境变量；适配器向 `BASE_URL/chat/completions` 发送固定模型的请求，收集完整响应后返回。配置缺失或上游错误会明确失败。当前验证采用本地协议模拟服务，没有调用真实公网模型服务；SSE 提供运行和节点事件，不提供逐 token 模型流。

## 恢复机制与资源边界

- **一个服务进程独占一个本地 SQLite 数据库。** OS 排他锁会拒绝第二个服务进程使用相同数据库；使用本地磁盘和单个 Uvicorn worker。
- 节点结果和事件在同一事务提交。启动时恢复排队及中断任务，保留成功检查点。failed 运行需要显式 resume；恢复时保留成功节点，重置未完成节点。
- 失败节点不会自动重试。每节点累计最多执行 5 次，包括中断恢复与显式 resume。每轮执行的运行截止时间为 300 秒，节点默认 10 秒，可设为 0.05–120 秒。
- 节点失败后不再启动依赖它的后续节点；已投递的独立任务收敛后结束运行。正常关闭服务会取消进行中的工作并记录中断。
- DAG 最多包含 32 个节点。请求体、模板展开、上游响应和节点输出均有大小限制；默认非对象请求及输出上限为 256 KiB。
- 中断本地推理请求不保证远端停止。恢复后再次调用可能重复计费，平台无法撤销已经发送的远端请求。

对象服务使用 SQLite BLOB 保存小文件，未实现分布式对象存储、S3、MySQL 恢复、多实例高可用或外部 HTTP 写核查。原 Java 项目的性能数据和测试数量不适用于本 Python 版本。

## 验证

```sh
python -m unittest discover -s tests -v
python scripts/smoke.py

cd frontend
npm test
npm run build
```

2026-10-22 的本地验证通过 **29 项后端测试**及 **7 项前端测试**。后端覆盖运行/事件/对象的租户隔离、角色权限、并发配额写入、非法 DAG、幂等冲突、截止时间、显式恢复、进程排他锁和模板展开预算。

冒烟脚本启动真实本地进程，使用临时凭据与数据库，在首个成功检查点后强制终止服务，再重新启动。验证已完成节点只调用一次、未完成节点再次尝试、对象数据持久化、事件回放和请求幂等；输出结果摘要，不输出访问 token。

仓库提供多阶段 [Dockerfile](Dockerfile)，用于构建 React 控制台并以普通用户运行 Python 服务。本次验证**未构建或运行 Docker 镜像**。使用该部署配置时，应持久化 `/app/data`。

## 源码入口

| 入口 | 职责 |
| --- | --- |
| `src/flowtrail/app.py` | HTTP 接口、鉴权依赖、请求体限制、SSE 与静态控制台 |
| `src/flowtrail/models.py` | DAG 校验及有字节预算的单次模板替换 |
| `src/flowtrail/runtime.py` | 调度、并发、截止时间与 HTTP/模型适配器 |
| `src/flowtrail/store.py` | 事务、检查点、事件、幂等、对象和配额 |
| `src/flowtrail/locking.py` | 服务进程对数据库的排他占用 |
| `src/flowtrail/__main__.py` | token 初始化与服务启动 CLI |
| `frontend/src/` | React 运维控制台和 API 客户端 |

## 来源与许可

本项目延续本人 [Java FlowTrail Server](https://github.com/cloudwallker/flowtrail-server) 的工作流思路，Python 源码在 AI 编程助手协助下编写。项目采用聚焦实现，当前能力以源码和上述验证为准。参考版本与来源说明见 [NOTICE](NOTICE)。

[LICENSE](LICENSE) 保留原贡献者署名，项目代码采用 MIT 许可。第三方依赖分别安装并保留各自许可证。
