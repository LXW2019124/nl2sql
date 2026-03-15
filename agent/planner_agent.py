"""查询规划器。

该模块负责把自然语言问题先粗分为 `lookup / aggregation / ranking / subquery`
等查询模式，并产出一份轻量的 `query_plan`。后续检索层和 SQL 生成层都会依赖
这份计划来调整召回范围和 SQL 结构。
"""

import json
import os
import re

from openai import OpenAI

from core.env import load_env_file


def _clean_json(content):
    """移除模型可能包裹的 Markdown 代码块，便于后续解析 JSON。"""
    cleaned = re.sub(r"```json", "", content or "", flags=re.IGNORECASE)
    cleaned = re.sub(r"```", "", cleaned)
    return cleaned.strip()


class PlannerAgent:
    """生成查询计划的轻量代理。

    当 `use_llm=False` 时，规划完全依赖规则；当启用 LLM 时，会优先走模型，
    并在失败时自动回退到启发式规则，保证主链路稳定。
    """

    def __init__(self, model=None, api_key=None, base_url=None, client=None, use_llm=False):
        load_env_file()
        self.model = model or os.getenv("PLANNER_MODEL") or os.getenv("OPENAI_MODEL") or "deepseek-chat"
        self.client = client or self._build_client(api_key=api_key, base_url=base_url, use_llm=use_llm)

    def _build_client(self, api_key=None, base_url=None, use_llm=False):
        """按需创建 OpenAI 兼容客户端。"""
        if not use_llm:
            return None

        resolved_api_key = api_key or os.getenv("PLANNER_API_KEY") or os.getenv("OPENAI_API_KEY")
        resolved_base_url = base_url or os.getenv("PLANNER_BASE_URL") or os.getenv("OPENAI_BASE_URL")
        return OpenAI(api_key=resolved_api_key, base_url=resolved_base_url)

    def build_plan_prompt(self, question, dialect="mysql"):
        """构造给 LLM 的规划提示词。"""
        return f"""
You are a query planner for a text-to-SQL system.

Return STRICT JSON only with this schema:
{{
  "query_type": "lookup|aggregation|ranking|subquery",
  "needs_group_by": true,
  "needs_order_by": false,
  "needs_subquery": false,
  "limit": 10
}}

Planning rules:
1. Use "ranking" when the user asks for top/bottom/highest/lowest/ranked results.
2. Use "aggregation" when the user asks for totals, counts, averages, sums, or rates.
3. Use "subquery" when the user compares against averages or historical extrema.
4. Use "lookup" for simple filtering/listing.
5. Set "limit" to null when no explicit top-N style limit is requested.
6. Use only syntax and planning assumptions compatible with {dialect}.

Question:
{question}
"""

    def _heuristic_plan(self, question):
        """在没有 LLM 或 LLM 失败时，使用规则生成兜底计划。"""
        lowered = question.lower()
        limit_match = re.search(r"\btop\s+(\d+)\b", lowered)
        has_aggregation = any(
            keyword in lowered for keyword in ["count", "sum", "avg", "average", "total", "number of", "rate"]
        )
        has_ranking = any(keyword in lowered for keyword in ["top", "highest", "lowest", "rank"])
        needs_subquery = bool(
            re.search(
                r"(higher|lower|more|less)\s+than\s+(the\s+)?average|"
                r"(above|below)\s+(the\s+)?average|"
                r"historical\s+(maximum|minimum)|history\s+maximum|history\s+minimum",
                lowered,
            )
        )
        needs_group_by = has_aggregation or has_ranking

        if needs_subquery:
            query_type = "subquery"
        elif has_ranking:
            query_type = "ranking"
        elif has_aggregation:
            query_type = "aggregation"
        else:
            query_type = "lookup"

        return {
            "query_type": query_type,
            "needs_group_by": needs_group_by,
            "needs_order_by": has_ranking,
            "needs_subquery": needs_subquery,
            "limit": int(limit_match.group(1)) if limit_match else None,
        }

    def _normalize_plan(self, plan, fallback):
        """把模型输出归一化为稳定的 plan 结构。"""
        normalized = dict(fallback)
        normalized.update(plan or {})
        normalized["query_type"] = normalized.get("query_type") or fallback["query_type"]
        normalized["needs_group_by"] = bool(normalized.get("needs_group_by"))
        normalized["needs_order_by"] = bool(normalized.get("needs_order_by"))
        normalized["needs_subquery"] = bool(normalized.get("needs_subquery"))

        limit = normalized.get("limit")
        if limit in ("", "null", "None"):
            normalized["limit"] = None
        elif limit is not None:
            normalized["limit"] = int(limit)

        return normalized

    def plan(self, question, dialect=None):
        """对外暴露的规划入口。"""
        fallback_plan = self._heuristic_plan(question)
        if self.client is None:
            return fallback_plan

        prompt = self.build_plan_prompt(question, dialect=dialect or "mysql")
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You output only JSON."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0,
            )
            content = response.choices[0].message.content
            parsed = json.loads(_clean_json(content))
            return self._normalize_plan(parsed, fallback_plan)
        except Exception:
            # 规划失败时直接回退，避免阻断后续 SQL 求解主流程。
            return fallback_plan
