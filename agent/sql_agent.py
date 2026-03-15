"""SQL 代理。

该封装层的职责很薄：把上层编排器传入的参数转发给底层 SQL 生成器，并兼容
不同版本生成器可能存在的签名差异。
"""

from core.sql_generator import LLMBackedSQLGenerator


class SQLAgent:
    """对 SQL 生成与修复能力做一层兼容性包装。"""

    def __init__(self, generator=None):
        self.generator = generator or LLMBackedSQLGenerator()

    def generate(
        self,
        question,
        retrieved_context,
        intent,
        dialect="mysql",
        query_plan=None,
    ):
        """生成 SQL。

        这里保留多层 `TypeError` 兼容，是为了兼容历史测试桩和旧版生成器签名。
        """
        try:
            return self.generator.generate(
                question,
                retrieved_context,
                intent,
                dialect=dialect,
                query_plan=query_plan,
            )
        except TypeError:
            try:
                return self.generator.generate(
                    question,
                    retrieved_context,
                    intent,
                    dialect=dialect,
                    query_plan=query_plan,
                )
            except TypeError:
                try:
                    return self.generator.generate(
                        question,
                        retrieved_context,
                        intent,
                        dialect=dialect,
                    )
                except TypeError:
                    return self.generator.generate(
                        question,
                        retrieved_context,
                        intent,
                    )

    def fix(
        self,
        question,
        retrieved_context,
        bad_sql,
        error_message,
        dialect="mysql",
    ):
        """根据执行错误修复 SQL。"""
        try:
            return self.generator.fix(
                question,
                retrieved_context,
                bad_sql,
                error_message,
                dialect=dialect,
            )
        except TypeError:
            try:
                return self.generator.fix(
                    question,
                    retrieved_context,
                    bad_sql,
                    error_message,
                    dialect=dialect,
                )
            except TypeError:
                return self.generator.fix(
                    question,
                    retrieved_context,
                    bad_sql,
                    error_message,
                )
