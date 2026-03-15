# LangGraph SQL Agent

一个面向真实数据库的 Text-to-SQL 原型项目。系统接收自然语言问题，结合数据库 schema introspection、metadata 检索、轻量 query planning 和 LLM SQL 生成/修复，输出可执行 SQL 或直接返回查询结果。

## 当前能力

- 支持从真实数据库连接中自动抽取 schema metadata
- 支持轻量查询规划：`lookup / aggregation / ranking / subquery`
- 支持 metadata 混合检索：词法、规则、可选向量后端
- 支持基于 LLM 的 SQL 生成与错误修复
- 支持输出 `retrieval_trace`，便于调试和评测

## 项目结构

```text
.
├─ agent/                  # 规划器、SQL 代理、总协调器
├─ core/                   # 运行时、图构建、metadata、SQL、检索核心逻辑
│  └─ retrieval/           # 检索子模块
├─ metadata/               # 静态 metadata 示例
├─ scripts/                # 本地调试脚本
├─ main.py                 # 最小可运行入口
├─ requirements.txt        # 依赖清单
└─ .env.example            # 环境变量模板
```

## 环境准备

建议使用 Python 3.10 及以上版本。

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -U pip
python -m pip install -r requirements.txt
```

## 配置

复制模板并填写运行参数：

```powershell
Copy-Item .env.example .env
```

最关键的环境变量如下：

```env
DB_URL=
OPENAI_API_KEY=
OPENAI_BASE_URL=https://api.deepseek.com
OPENAI_MODEL=deepseek-chat

QDRANT_URL=
QDRANT_COLLECTION_NAME=metadata_vectors

QWEN_EMBEDDING_API_KEY=
DASHSCOPE_API_KEY=
QWEN_EMBEDDING_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1
QWEN_EMBEDDING_MODEL=text-embedding-v4
QWEN_EMBEDDING_DIMENSIONS=1024
```

说明：

- `DB_URL` 为当前运行主链路必需项
- 未配置 `QDRANT_URL` 时，会自动回退到非向量检索
- 未配置真实 embedding API 时，会回退到本地 hash embedding 占位实现

## 运行

### 1. 使用兼容入口运行

```powershell
python main.py
```

### 2. 使用自定义问题运行

```powershell
$env:QUESTION="统计非封禁用户按天的取消率，并按日期升序返回。"
python main.py
```

### 3. 在 Python 代码里调用 API

```python
from main import run_question

result = run_question(
    question="show all users",
    db_url="mysql+pymysql://user:pass@host:3306/dbname",
    execute=True,
)

print(result["sql_query"])
```

### 4. 只做规划与检索，不执行 SQL

```python
from main import run_question

result = run_question(
    question="show all users",
    metadata_path=r".\metadata\demo_metadata.json",
    execute=False,
    use_llm_planner=False,
)
```

`run_question()` 当前支持的主要参数：

- `question`：自然语言问题
- `db_url`：数据库连接串
- `metadata_path`：静态 metadata 文件路径
- `execute`：是否执行 SQL
- `use_llm_planner`：是否启用 LLM 规划器

程序会输出：

- 查询计划 `query_plan`
- 召回到的表 `retrieved_tables`
- 可选的检索后端状态 `retrieval_backend`
- SQL 执行结果或错误信息

## 调试脚本

- `scripts/inspect_subscription_columns.py`

该脚本仅用于本地快速检查数据库里 `subscription_events` 表的列注释，不属于主运行链路。

## 当前限制

- 入口仍然偏 demo，缺少独立的应用服务层
- 向量检索初始化失败时当前仍会静默降级，需要后续补日志或错误透出
- 仓库尚未附带最终许可证文件，公开发布前建议补齐
