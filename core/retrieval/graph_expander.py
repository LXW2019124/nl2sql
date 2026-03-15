"""基于 schema 关系图的候选扩展器。"""


class GraphExpander:
    """沿表关系做一跳扩展。"""

    def expand(self, seed_candidates, all_tables, query_plan=None):
        """从种子候选出发补充直接相邻的表。"""
        query_plan = query_plan or {}
        max_results = 4 if query_plan.get("needs_group_by") else 2
        table_lookup = {table.get("table"): table for table in all_tables}
        expanded = []
        seen = set()

        for candidate in seed_candidates:
            table_name = candidate.get("table")
            if table_name and table_name not in seen:
                expanded.append(candidate)
                seen.add(table_name)

        for candidate in list(expanded):
            if len(expanded) >= max_results:
                break
            for relationship in candidate.get("relationships", []):
                neighbor_name = relationship.get("to_table")
                if not neighbor_name or neighbor_name in seen:
                    continue
                neighbor = table_lookup.get(neighbor_name)
                if neighbor is None:
                    continue
                expanded.append(neighbor)
                seen.add(neighbor_name)
                if len(expanded) >= max_results:
                    break

        return expanded
