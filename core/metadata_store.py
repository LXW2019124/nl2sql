"""metadata 检索入口。

该模块把 query rewrite、混合召回、rerank、候选筛选以及调试 trace 统一封装起来，
是“数据库 schema metadata -> SQL 生成上下文”之间的桥梁。
"""

import json
import os

from core.retrieval.hybrid_retriever import HybridRetriever
from core.retrieval.qdrant_vector_backend import QdrantVectorBackend
from core.retrieval.query_rewriter import QueryRewriter
from core.retrieval.reranker import Reranker


class MetadataStore:
    """负责管理 metadata 载荷及其检索链路。"""

    def __init__(self, payload):
        self.payload = payload
        self.tables = payload.get("tables", [])
        self.vector_backend = self._build_vector_backend(payload)
        self.hybrid_retriever = HybridRetriever(payload, vector_backend=self.vector_backend)
        self.query_rewriter = QueryRewriter()
        self.reranker = Reranker()

    def _build_vector_backend(self, payload):
        """按环境变量决定是否启用 Qdrant 向量后端。"""
        if not os.getenv("QDRANT_URL"):
            return None
        try:
            backend = QdrantVectorBackend()
            backend.index_tables(payload.get("tables", []))
            return backend
        except Exception:
            # 向量后端初始化失败时退回纯词法/规则检索，保证主链路可用。
            return None

    def get_backend_status(self):
        """返回当前检索后端状态，便于调试和日志展示。"""
        backend_name = "disabled"
        embedder_name = "none"
        vector_enabled = self.vector_backend is not None

        if self.vector_backend is not None:
            backend_name = type(self.vector_backend).__name__
            embedder_name = type(self.vector_backend.embedder).__name__

        return {
            "vector_enabled": vector_enabled,
            "backend_name": backend_name,
            "embedder_name": embedder_name,
            "qdrant_url_configured": bool(os.getenv("QDRANT_URL")),
        }

    @classmethod
    def from_file(cls, path):
        """从 JSON 文件加载 metadata。"""
        with open(path, "r", encoding="utf-8") as handle:
            return cls(json.load(handle))

    @classmethod
    def from_payload(cls, payload):
        """直接从内存对象构建 metadata store。"""
        return cls(payload)

    def retrieve(self, question, top_k=3, query_plan=None):
        """仅返回最终选中的候选表。"""
        _, selected_candidates = self._run_retrieval_pipeline(
            question,
            top_k=top_k,
            query_plan=query_plan,
        )
        return selected_candidates

    def retrieve_with_trace(self, question, top_k=3, query_plan=None):
        """返回最终候选表以及完整的检索 trace。"""
        trace, selected_candidates = self._run_retrieval_pipeline(
            question,
            top_k=top_k,
            query_plan=query_plan,
        )
        return selected_candidates, trace

    def _run_retrieval_pipeline(self, question, top_k=3, query_plan=None):
        """执行完整检索流水线。"""
        query_plan = query_plan or {}
        rewrite_result = self.query_rewriter.rewrite(question)
        retrieval_query = rewrite_result.get("retrieval_query") or question
        scored_candidates = self.hybrid_retriever.retrieve(
            retrieval_query,
            top_k=max(top_k, len(self.tables)),
            query_plan=query_plan,
        )
        ranked_candidates = self.reranker.rank(scored_candidates)
        selected_candidates = self._select_top_k_candidates(ranked_candidates, top_k)
        trace = {
            "input": {
                "question": question,
                "normalized_question": rewrite_result.get("normalized_question"),
                "translated_question": rewrite_result.get("translated_question"),
                "retrieval_query": retrieval_query,
                "top_k": top_k,
                "query_plan": query_plan,
                "backend_status": self.get_backend_status(),
            },
            "operations": [
                {
                    "step": "query_rewrite",
                    "input": question,
                    "output": rewrite_result,
                },
                {
                    "step": "hybrid_retrieve",
                    "output": [self._serialize_candidate(candidate) for candidate in scored_candidates],
                },
                {
                    "step": "rerank",
                    "output": [self._serialize_candidate(candidate) for candidate in ranked_candidates],
                },
                {
                    "step": "select_top_k",
                    "selection_strategy": "prefer lexical matches, then fill remaining slots by rerank order",
                    "output": [self._serialize_candidate(candidate) for candidate in selected_candidates],
                },
            ],
            "output": {
                "selected_tables": [candidate.get("table") for candidate in selected_candidates],
            },
        }
        return trace, selected_candidates

    def _select_top_k_candidates(self, ranked_candidates, top_k):
        """优先保留有词法命中的候选，不足时再用 rerank 结果补齐。"""
        ranked = [table for table in ranked_candidates if table["lexical_score"] > 0]
        if not ranked:
            ranked = list(ranked_candidates)
        elif len(ranked) < top_k:
            ranked_names = {table["table"] for table in ranked}
            ranked.extend(
                table for table in ranked_candidates if table["table"] not in ranked_names
            )
        return ranked[:top_k]

    def _serialize_candidate(self, candidate):
        """裁剪调试输出，只保留关键打分字段。"""
        fields = [
            "table",
            "lexical_score",
            "vector_score",
            "graph_score",
            "planner_score",
            "final_score",
            "matched_chunk_type",
            "matched_chunk_id",
            "matched_chunk_columns",
        ]
        return {
            field: candidate.get(field)
            for field in fields
            if field in candidate and candidate.get(field) not in (None, [])
        }
