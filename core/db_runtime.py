"""数据库运行时与 schema 探查能力。

该模块负责：
1. 创建数据库连接。
2. 推断数据库方言。
3. 从真实数据库中抽取表、列、主键、外键、样例行、行数等 metadata。

当前实现主要面向 MySQL，但接口层尽量保持对 SQLAlchemy 兼容方言通用。
"""

from dataclasses import dataclass

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url


@dataclass
class DatabaseConfig:
    """数据库连接配置。"""

    db_url: str
    sample_limit: int = 3


class DatabaseConnector:
    """对 SQLAlchemy engine 的轻量包装。"""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.engine = create_engine(config.db_url)

    def execute(self, sql: str):
        """执行 SQL 并返回全部结果。"""
        with self.engine.connect() as connection:
            result = connection.execute(text(sql))
            return result.fetchall()

    def close(self):
        """释放 engine 资源。"""
        self.engine.dispose()


def infer_dialect(db_url: str):
    """根据连接串推断数据库方言。"""
    return make_url(db_url).get_backend_name()


class SchemaIntrospector:
    """从数据库中构建统一的 metadata 载荷。"""

    def __init__(self, connector: DatabaseConnector):
        self.connector = connector
        self.inspector = inspect(connector.engine)

    def build_metadata(self):
        """扫描所有表并组装成统一的 metadata 结构。"""
        tables = []
        table_columns = {}
        table_primary_keys = {}

        # 先做一次全表扫描，把列和主键信息缓存下来，避免重复访问 inspector。
        for table_name in sorted(self.inspector.get_table_names()):
            columns = self.inspector.get_columns(table_name)
            primary_key = set(self.inspector.get_pk_constraint(table_name).get("constrained_columns") or [])
            table_columns[table_name] = columns
            table_primary_keys[table_name] = primary_key

        for table_name in sorted(self.inspector.get_table_names()):
            columns = table_columns[table_name]
            primary_key = table_primary_keys[table_name]
            foreign_keys = self.inspector.get_foreign_keys(table_name)

            relationships = []
            for foreign_key in foreign_keys:
                referred_table = foreign_key.get("referred_table")
                constrained_columns = ", ".join(foreign_key.get("constrained_columns", []))
                referred_columns = ", ".join(foreign_key.get("referred_columns", []))
                relationships.append(
                    {
                        "to_table": referred_table,
                        "description": f"{constrained_columns} -> {referred_table}.{referred_columns}",
                        "inferred": False,
                    }
                )

            if not relationships:
                # 当数据库没有显式外键时，尝试根据 `*_id` 规则推断弱关系。
                relationships.extend(
                    self._infer_relationships(table_name, columns, table_columns, table_primary_keys)
                )

            tables.append(
                {
                    "table": table_name,
                    "description": self._table_description(table_name),
                    "aliases": self._table_aliases(table_name),
                    "columns": [
                        {
                            "name": column["name"],
                            "description": self._column_description(column),
                            "type": str(column["type"]),
                            "nullable": column.get("nullable", True),
                            "primary_key": column["name"] in primary_key,
                            "semantic_type": self._infer_semantic_type(column["name"], column["type"]),
                        }
                        for column in columns
                    ],
                    "metrics": [],
                    "relationships": relationships,
                    "join_hints": self._build_join_hints(table_name, relationships),
                    "row_count": self._row_count(table_name),
                    "sample_rows": self._sample_rows(table_name),
                }
            )

        return {"tables": tables}

    def _table_description(self, table_name):
        """优先使用数据库注释，否则回退到默认描述。"""
        try:
            comment = (self.inspector.get_table_comment(table_name) or {}).get("text")
        except Exception:
            comment = None
        return comment or f"Table {table_name}"

    def _column_description(self, column):
        """优先使用列注释，否则构造基础描述。"""
        comment = column.get("comment")
        if comment:
            return comment
        return f"Column {column['name']} with type {column['type']}"

    def _table_aliases(self, table_name):
        """为表名生成简单别名，提升检索召回率。"""
        aliases = {table_name.lower()}
        singular = table_name.lower()
        if singular.endswith("ies") and len(singular) > 3:
            aliases.add(singular[:-3] + "y")
        elif singular.endswith("s") and len(singular) > 3:
            aliases.add(singular[:-1])
        return sorted(aliases)

    def _infer_semantic_type(self, column_name, column_type):
        """根据列名和类型做轻量语义分类。"""
        normalized_name = column_name.lower()
        normalized_type = str(column_type).lower()

        if normalized_name.endswith("_at") or "date" in normalized_name or "time" in normalized_name:
            return "time"
        if "status" in normalized_name or "state" in normalized_name:
            return "status"
        if normalized_name.endswith("_id") or normalized_name == "id":
            return "id"
        if any(token in normalized_name for token in ["amount", "price", "total", "cost"]):
            return "amount"
        if "char" in normalized_type or "text" in normalized_type:
            return "dimension"
        return "attribute"

    def _build_join_hints(self, table_name, relationships):
        """为 SQL 生成器补充可读的 join 提示。"""
        return [
            f"{table_name} can join to {relationship['to_table']} via {relationship['description']}"
            for relationship in relationships
        ]

    def _infer_relationships(self, table_name, columns, table_columns, table_primary_keys):
        """在没有显式外键时，根据同名 `*_id` 推断关系。"""
        relationships = []
        current_column_names = {column["name"] for column in columns}

        for column_name in current_column_names:
            if not column_name.endswith("_id"):
                continue

            for other_table, other_columns in table_columns.items():
                if other_table == table_name:
                    continue
                other_primary_keys = table_primary_keys[other_table]
                other_column_names = {column["name"] for column in other_columns}
                if column_name in other_column_names and column_name in other_primary_keys:
                    relationships.append(
                        {
                            "to_table": other_table,
                            "description": f"{column_name} appears to reference {other_table}.{column_name}",
                            "inferred": True,
                        }
                    )

        unique_relationships = {}
        for relationship in relationships:
            unique_relationships[relationship["to_table"]] = relationship
        return list(unique_relationships.values())

    def _sample_rows(self, table_name):
        """读取少量样例行，帮助模型理解表内容。"""
        try:
            rows = self.connector.execute(
                f"SELECT * FROM {self._quoted_identifier(table_name)} LIMIT {self.connector.config.sample_limit}"
            )
            return [list(row) for row in rows]
        except Exception:
            return []

    def _row_count(self, table_name):
        """读取表行数，用于辅助理解表规模。"""
        try:
            rows = self.connector.execute(f"SELECT COUNT(*) FROM {self._quoted_identifier(table_name)}")
            return int(rows[0][0]) if rows else 0
        except Exception:
            return 0

    def _quoted_identifier(self, identifier):
        """使用当前方言的规则对标识符做安全引用。"""
        return self.connector.engine.dialect.identifier_preparer.quote_identifier(identifier)
