"""结构化上下文组装器。

该模块把检索出的候选表进一步裁剪成更适合喂给 SQL 生成器的结构化上下文。
"""

from core.retrieval.text_utils import tokenize


class ContextAssembler:
    """从候选表中提炼主表、辅助表、相关字段和 join 路径。"""

    def assemble(self, question, candidates, query_plan=None, rewritten_question=None):
        """组装结构化上下文。"""
        query_plan = query_plan or {}
        filtered_candidates = self._filter_candidates(candidates, query_plan)
        primary_tables = [filtered_candidates[0]["table"]] if filtered_candidates else []
        supporting_tables = [candidate["table"] for candidate in filtered_candidates[1:]]

        relevant_columns = {}
        metrics = []
        join_paths = []
        notes = []

        for index, candidate in enumerate(filtered_candidates):
            table_name = candidate.get("table")
            selected_columns = self._select_relevant_columns(
                candidate,
                rewritten_question or question,
                query_plan,
                is_primary=(index == 0),
            )
            relevant_columns[table_name] = selected_columns

            for metric in candidate.get("metrics", []):
                metrics.append(
                    {
                        "table": table_name,
                        "name": metric.get("name"),
                        "definition": metric.get("description", ""),
                    }
                )

            for relation in candidate.get("relationships", []):
                if relation.get("to_table"):
                    join_paths.append(
                        {
                            "from": table_name,
                            "to": relation.get("to_table"),
                            "condition": relation.get("description", ""),
                        }
                    )

            matched_chunk_type = candidate.get("matched_chunk_type")
            matched_chunk_id = candidate.get("matched_chunk_id")
            if matched_chunk_type:
                note = f"{table_name} matched {matched_chunk_type} chunk"
                if matched_chunk_id:
                    note += f" ({matched_chunk_id})"
                notes.append(note)

            if query_plan.get("needs_group_by") and "request_at" in relevant_columns[table_name]:
                notes.append(f"{table_name}.request_at can be used as a time dimension")

        return {
            "question": question,
            "query_plan": query_plan,
            "primary_tables": primary_tables,
            "supporting_tables": supporting_tables,
            "relevant_columns": relevant_columns,
            "metrics": metrics,
            "join_paths": join_paths,
            "notes": notes,
        }

    def _filter_candidates(self, candidates, query_plan):
        """对 lookup 场景做一次保守收缩，避免无关表进入上下文。"""
        if len(candidates) <= 1:
            return candidates

        if query_plan.get("query_type") != "lookup":
            return candidates

        primary = candidates[0]
        filtered = [primary]
        primary_table = primary.get("table")
        primary_neighbors = {
            relation.get("to_table") for relation in primary.get("relationships", []) if relation.get("to_table")
        }

        for candidate in candidates[1:]:
            candidate_table = candidate.get("table")
            candidate_neighbors = {
                relation.get("to_table")
                for relation in candidate.get("relationships", [])
                if relation.get("to_table")
            }
            if candidate_table in primary_neighbors or primary_table in candidate_neighbors:
                filtered.append(candidate)

        return filtered

    def _select_relevant_columns(self, candidate, question, query_plan, is_primary=False):
        """从候选表中挑出更可能对 SQL 生成有帮助的列。"""
        question_lower = (question or "").lower()
        question_tokens = set(tokenize(question))
        selected = []
        seen = set()
        columns = candidate.get("columns", [])
        matched_chunk_columns = candidate.get("matched_chunk_columns", [])

        def add_column(name):
            if name and name not in seen:
                selected.append(name)
                seen.add(name)

        for column_name in matched_chunk_columns:
            add_column(column_name)

        for column in columns:
            name = column.get("name")
            normalized = (name or "").lower()
            if "status" in normalized:
                add_column(name)

        if is_primary:
            # 主表优先保留时间字段，方便生成分组、排序和时间过滤。
            for column in columns:
                name = column.get("name")
                normalized = (name or "").lower()
                if normalized.endswith("_at") or "date" in normalized or "time" in normalized:
                    add_column(name)

        for column in columns:
            name = column.get("name")
            normalized = (name or "").lower()
            if any(token in question_lower for token in ["banned", "role"]) and (
                "banned" in normalized or "role" in normalized
            ):
                add_column(name)

        scored_columns = []
        for column in columns:
            name = column.get("name")
            column_tokens = set(tokenize(name))
            column_tokens.update(tokenize(column.get("description", "")))
            overlap = len(question_tokens.intersection(column_tokens))
            if overlap > 0:
                scored_columns.append((overlap, name))

        scored_columns.sort(key=lambda item: (-item[0], item[1] or ""))
        for _, column_name in scored_columns:
            add_column(column_name)

        for column in columns:
            name = column.get("name")
            normalized = (name or "").lower()
            if normalized.endswith("_id") and normalized != "id":
                add_column(name)

        if not selected:
            for column in columns[:4]:
                add_column(column.get("name"))

        if query_plan.get("needs_group_by"):
            return selected[:4]
        return selected[:3]
