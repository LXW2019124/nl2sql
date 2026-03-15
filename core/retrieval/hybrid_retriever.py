"""混合检索器。

当前综合了三类信号：
1. 词法匹配
2. 向量召回或伪向量分数
3. 基于 query plan 的轻量规则加权
"""

from collections import Counter

from core.retrieval.text_utils import tokenize


class HybridRetriever:
    """针对表级 metadata 的混合召回器。"""

    def __init__(self, payload, vector_backend=None):
        self.payload = payload
        self.tables = payload.get("tables", [])
        self.vector_backend = vector_backend

    def retrieve(self, question, top_k=3, query_plan=None):
        """召回并返回得分最高的候选表。"""
        question_tokens = Counter(tokenize(question))
        vector_hits = self._vector_scores(question, top_k)
        scored_candidates = []

        for table in self.tables:
            vector_hit = vector_hits.get(table.get("table"), {})
            lexical_score = self._lexical_score(question_tokens, table)
            vector_score = vector_hit.get(
                "vector_score",
                self._pseudo_vector_score(question_tokens, table),
            )
            planner_score = self._planner_score(question_tokens, table, query_plan or {})
            scored_candidates.append(
                {
                    **table,
                    "lexical_score": float(lexical_score),
                    "vector_score": float(vector_score),
                    "graph_score": 1.0 if table.get("relationships") else 0.0,
                    "planner_score": float(planner_score),
                    **{key: value for key, value in vector_hit.items() if key not in {"table", "vector_score"}},
                }
            )

        scored_candidates.sort(
            key=lambda item: (
                -item["lexical_score"],
                -item["vector_score"],
                item.get("table", ""),
            )
        )
        ranked = [table for table in scored_candidates if table["lexical_score"] > 0 or table["vector_score"] > 0]
        if not ranked:
            ranked = scored_candidates
        return ranked[:top_k]

    def _vector_scores(self, question, top_k):
        """读取向量后端结果；若未启用则返回空字典。"""
        if not self.vector_backend:
            return {}
        results = self.vector_backend.search(question, top_k=top_k)
        return {item["table"]: item for item in results if item.get("table")}

    def _lexical_score(self, question_tokens, table):
        """根据表名、列名、描述、指标和关系做词法打分。"""
        score = 0

        for token in tokenize(table.get("table", "")):
            score += question_tokens[token] * 5

        for token in tokenize(table.get("description", "")):
            score += question_tokens[token] * 1

        for column in table.get("columns", []):
            for token in tokenize(column.get("name", "")):
                score += question_tokens[token] * 4
            for token in tokenize(column.get("description", "")):
                score += question_tokens[token] * 1

        for metric in table.get("metrics", []):
            for token in tokenize(metric.get("name", "")):
                score += question_tokens[token] * 4
            for token in tokenize(metric.get("description", "")):
                score += question_tokens[token] * 2

        for relation in table.get("relationships", []):
            for token in tokenize(relation.get("to_table", "")):
                score += question_tokens[token] * 3
            for token in tokenize(relation.get("description", "")):
                score += question_tokens[token] * 1

        return score

    def _pseudo_vector_score(self, question_tokens, table):
        """在没有真实向量后端时，用词项重叠模拟一个语义分数。"""
        table_text = " ".join(
            [
                table.get("table", ""),
                table.get("description", ""),
                " ".join(column.get("description", "") for column in table.get("columns", [])),
                " ".join(metric.get("description", "") for metric in table.get("metrics", [])),
            ]
        )
        table_tokens = tokenize(table_text)
        if not table_tokens:
            return 0.0

        overlap = sum(question_tokens[token] for token in table_tokens if token in question_tokens)
        return overlap / max(len(set(table_tokens)), 1)

    def _planner_score(self, question_tokens, table, query_plan):
        """根据 query plan 给更适合聚合/排序的表额外加分。"""
        score = 0.0
        if query_plan.get("needs_group_by"):
            for column in table.get("columns", []):
                column_name = (column.get("name") or "").lower()
                if column_name.endswith("_at") or "date" in column_name or "time" in column_name:
                    score += 1.0
                    break

        if query_plan.get("needs_group_by") or query_plan.get("query_type") in {"aggregation", "ranking"}:
            if table.get("metrics"):
                score += 1.0

        if "rate" in question_tokens and table.get("metrics"):
            score += 1.0

        return score
