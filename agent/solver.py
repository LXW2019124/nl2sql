"""求解器主编排模块。

`SQLProblemSolver` 是当前项目最核心的协调者，负责把一个自然语言问题依次送入：
1. 意图解析
2. 查询规划
3. metadata 检索
4. SQL 生成
5. SQL 执行与修复

它通过 LangGraph 将以上阶段串成一个可观察、可扩展的状态流。
"""

import os
import re
from pathlib import Path

from agent.planner_agent import PlannerAgent
from agent.sql_agent import SQLAgent
from core.db_runtime import DatabaseConfig, DatabaseConnector, SchemaIntrospector, infer_dialect
from core.env import load_env_file
from core.graph_builder import build_sql_agent_graph
from core.metadata_cache import MetadataCache
from core.metadata_store import MetadataStore
from core.sql_executor import execute_sql


class SQLProblemSolver:
    """把自然语言问题转换为 SQL 并可选择直接执行的主协调器。"""

    def __init__(
        self,
        metadata_path=None,
        db_url=None,
        sql_generator=None,
        planner_agent=None,
        sql_agent=None,
        max_retries=1,
        cache_dir=".cache/metadata",
    ):
        # 初始化时优先从环境变量读取数据库地址，并推断数据库方言。
        load_env_file()
        self.db_url = db_url or os.getenv("DB_URL")
        self.dialect = infer_dialect(self.db_url) if self.db_url else "mysql"
        self.max_retries = max_retries
        self.metadata_cache = MetadataCache(Path(cache_dir))
        self.metadata_store = self._build_metadata_store(metadata_path, self.db_url)
        self.planner_agent = planner_agent or PlannerAgent()
        self.sql_agent = sql_agent or SQLAgent(generator=sql_generator)
        self.sql_generator = self.sql_agent
        self.graph = build_sql_agent_graph(
            self._parse_intent_node,
            self._plan_query_node,
            self._retrieve_context_node,
            self._generate_sql_node,
            self._verify_sql_node,
        )

    def _build_metadata_store(self, metadata_path, db_url):
        """按“静态 metadata 文件优先，其次数据库直连”的策略构造 metadata store。"""
        if metadata_path:
            return MetadataStore.from_file(metadata_path)
        if db_url:
            # 只在显式开启缓存时才尝试复用缓存，默认走数据库实时探查。
            cached_payload = self.metadata_cache.load(db_url) if self._should_use_metadata_cache() else None
            if cached_payload:
                return MetadataStore.from_payload(cached_payload)
            connector = DatabaseConnector(DatabaseConfig(db_url=db_url))
            introspector = SchemaIntrospector(connector)
            payload = introspector.build_metadata()
            connector.close()
            self.metadata_cache.save(db_url, payload)
            return MetadataStore.from_payload(payload)
        raise ValueError("metadata_path or db_url is required")

    def _should_use_metadata_cache(self):
        """根据环境变量判断是否启用 metadata 缓存。"""
        return (os.getenv("METADATA_CACHE_ENABLED") or "").lower() in {"1", "true", "yes", "on"}

    def _parse_intent_node(self, state):
        """从问题中抽取最粗粒度的指标、维度和显式过滤词。"""
        question = state["question"]
        lowered = question.lower()
        metrics = []
        if "rate" in lowered:
            metrics.append("rate")
        if "cancellation" in lowered:
            metrics.append("cancellation rate")
        if any(keyword in lowered for keyword in ["count", "number of", "total"]):
            metrics.append("count")
        dimensions = []
        if "day" in lowered or "date" in lowered:
            dimensions.append("day")
        filters = re.findall(r'"([^"]+)"', question)
        print("Extracting intent...")
        return {
            "intent": {
                "metrics": metrics,
                "dimensions": dimensions,
                "filters": filters,
            }
        }

    def _plan_query_node(self, state):
        """生成供后续检索和 SQL 生成使用的查询计划。"""
        print("Planning query...")
        return {
            "query_plan": self.planner_agent.plan(
                state["question"],
                dialect=state.get("dialect", self.dialect),
            )
        }

    def _retrieve_context_node(self, state):
        """根据 query plan 检索最相关的 metadata 上下文。"""
        print("Retrieving metadata...")
        query_plan = state.get("query_plan", {})
        # 聚合/分组类问题通常需要更丰富的上下文，因此适当扩大 top_k。
        top_k = 4 if query_plan.get("needs_group_by") else 2
        retrieved_context, retrieval_trace = self.metadata_store.retrieve_with_trace(
            state["question"],
            top_k=top_k,
            query_plan=query_plan,
        )
        return {
            "retrieved_context": retrieved_context,
            "retrieved_tables": [item["table"] for item in retrieved_context],
            "retrieval_trace": retrieval_trace,
            "retrieval_backend": self.metadata_store.get_backend_status(),
        }

    def _generate_sql_node(self, state):
        """调用 SQL 代理生成 SQL。"""
        print("Generating SQL...")
        try:
            sql_query = self.sql_agent.generate(
                state["question"],
                state["retrieved_context"],
                state["intent"],
                dialect=state.get("dialect", self.dialect),
                query_plan=state.get("query_plan"),
            )
        except TypeError:
            try:
                sql_query = self.sql_agent.generate(
                    state["question"],
                    state["retrieved_context"],
                    state["intent"],
                    dialect=state.get("dialect", self.dialect),
                    query_plan=state.get("query_plan"),
                )
            except TypeError:
                try:
                    sql_query = self.sql_agent.generate(
                        state["question"],
                        state["retrieved_context"],
                        state["intent"],
                        dialect=state.get("dialect", self.dialect),
                    )
                except TypeError:
                    sql_query = self.sql_agent.generate(
                        state["question"],
                        state["retrieved_context"],
                        state["intent"],
                    )
        print("\nGenerated SQL:")
        print(sql_query)
        return {"sql_query": sql_query}

    def _verify_sql_node(self, state):
        """执行 SQL，并在失败时尝试调用修复链路。"""
        if not state.get("execute", True):
            return {"status": "planned"}

        print("Executing SQL...")
        sql_query = state["sql_query"]
        for attempt in range(self.max_retries + 1):
            result = execute_sql(
                sql_query,
                db_url=state.get("db_url", self.db_url),
            )
            if result["success"]:
                print("\nCorrect Answer!")
                print(result["results"])
                return {
                    "execution_result": result["results"],
                    "status": "executed",
                    "sql_query": sql_query,
                }

            print("\nSQL Error:", result["error"])
            if attempt >= self.max_retries or not hasattr(self.sql_agent, "fix"):
                return {"error": result["error"], "status": "error", "sql_query": sql_query}

            print("Trying to fix SQL...")
            try:
                sql_query = self.sql_agent.fix(
                    state["question"],
                    state["retrieved_context"],
                    sql_query,
                    result["error"],
                    dialect=state.get("dialect", self.dialect),
                )
            except TypeError:
                try:
                    sql_query = self.sql_agent.fix(
                        state["question"],
                        state["retrieved_context"],
                        sql_query,
                        result["error"],
                        dialect=state.get("dialect", self.dialect),
                    )
                except TypeError:
                    sql_query = self.sql_agent.fix(
                        state["question"],
                        state["retrieved_context"],
                        sql_query,
                        result["error"],
                    )
            print("\nFixed SQL:")
            print(sql_query)

    def solve(self, question, execute=True):
        """对外暴露的统一求解入口。"""
        state = self.graph.invoke(
            {
                "question": question,
                "execute": execute,
                "db_url": self.db_url,
                "dialect": self.dialect,
            }
        )

        if state.get("query_plan"):
            print("\nQuery plan:")
            print(state["query_plan"])

        if state.get("retrieved_tables"):
            print("\nRetrieved tables:")
            print(state["retrieved_tables"])

        phase_outputs = {
            # 这里保留中间产物，是为了便于调试、测试和后续可视化。
            "query_plan": state.get("query_plan"),
            "retrieved_context": state.get("retrieved_context"),
            "retrieval_trace": state.get("retrieval_trace"),
            "sql_query": state.get("sql_query"),
        }

        return {
            "status": state.get("status", "unknown"),
            "query_plan": state.get("query_plan"),
            "retrieved_context": state.get("retrieved_context", []),
            "retrieved_tables": state.get("retrieved_tables", []),
            "retrieval_trace": state.get("retrieval_trace"),
            "retrieval_backend": state.get("retrieval_backend"),
            "sql_query": state.get("sql_query"),
            "phase_outputs": phase_outputs,
            "results": state.get("execution_result"),
            "error": state.get("error"),
        }
