"""metadata chunk 构建器。

该模块把表级 metadata 拆成更细粒度的检索单元，便于向量索引或混合检索更准确地命中
表描述、指标定义和关系路径。
"""


def _table_columns(table):
    """提取表的列名列表。"""
    return [column.get("name") for column in table.get("columns", []) if column.get("name")]


def _table_neighbors(table):
    """提取表的邻接表名称列表。"""
    return [relation.get("to_table") for relation in table.get("relationships", []) if relation.get("to_table")]


def build_chunks(payload):
    """把表级 metadata 拆成可检索 chunk。"""
    chunks = []

    for table in payload.get("tables", []):
        columns = _table_columns(table)
        neighbors = _table_neighbors(table)
        table_name = table.get("table", "")

        chunks.append(
            {
                "chunk_id": f"{table_name}.summary",
                "chunk_type": "table_summary",
                "table": table_name,
                "text": table.get("description", ""),
                "columns": columns,
                "keywords": [table_name.lower(), *[column.lower() for column in columns]],
                "neighbors": neighbors,
            }
        )

        for metric in table.get("metrics", []):
            metric_name = metric.get("name", "")
            chunks.append(
                {
                    "chunk_id": f"{table_name}.metric.{metric_name.replace(' ', '_')}",
                    "chunk_type": "metric_definition",
                    "table": table_name,
                    "text": metric.get("description", ""),
                    "columns": columns,
                    "keywords": [metric_name.lower(), table_name.lower()],
                    "neighbors": neighbors,
                }
            )

        if neighbors:
            # 关系 chunk 让检索层更容易感知“这张表可与谁 join”。
            chunks.append(
                {
                    "chunk_id": f"{table_name}.relationships",
                    "chunk_type": "relationship_path",
                    "table": table_name,
                    "text": " ".join(
                        relation.get("description", "") for relation in table.get("relationships", [])
                    ).strip(),
                    "columns": columns,
                    "keywords": [table_name.lower(), *[neighbor.lower() for neighbor in neighbors]],
                    "neighbors": neighbors,
                }
            )

    return chunks
