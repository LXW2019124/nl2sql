"""候选结果重排器。"""


class Reranker:
    """把多个分数按权重汇总成最终排序结果。"""

    def __init__(self, weights=None):
        self.weights = weights or {
            "lexical_score": 0.35,
            "vector_score": 0.35,
            "graph_score": 0.20,
            "planner_score": 0.10,
            "history_score": 0.0,
        }

    def rank(self, candidates):
        """对候选表做线性加权排序。"""
        ranked = []

        for candidate in candidates:
            score_breakdown = {}
            final_score = 0.0

            for score_name, weight in self.weights.items():
                component = float(candidate.get(score_name, 0.0))
                score_breakdown[score_name] = component
                final_score += weight * component

            enriched_candidate = dict(candidate)
            enriched_candidate["final_score"] = final_score
            enriched_candidate["score_breakdown"] = score_breakdown
            ranked.append(enriched_candidate)

        ranked.sort(key=lambda item: (-item["final_score"], item.get("table", "")))
        return ranked
