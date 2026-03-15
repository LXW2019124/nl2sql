"""基于 LLM 的 SQL 生成器。"""

import os
import re

from openai import OpenAI

from core.env import load_env_file


def clean_sql(sql: str) -> str:
    """清理模型输出中的 Markdown 包裹，只保留 SQL 本体。"""
    sql = re.sub(r"```sql", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"```", "", sql)
    return sql.strip()


class LLMBackedSQLGenerator:
    """负责 SQL 生成和修复的模型客户端封装。"""

    def __init__(self, model="deepseek-reasoner", api_key=None, base_url=None):
        load_env_file()
        self.model = model or os.getenv("OPENAI_MODEL") or "deepseek-chat"
        self.client = OpenAI(
            api_key=api_key or os.getenv("OPENAI_API_KEY"),
            base_url=base_url or os.getenv("OPENAI_BASE_URL"),
        )

    def build_generate_prompt(
        self,
        question,
        retrieved_context,
        intent,
        dialect="mysql",
        query_plan=None,
    ):
        """构造 SQL 生成提示词。"""
        return f"""
You are a SQL expert working from retrieved database metadata.

Generate a correct {dialect} SQL query using only the retrieved tables, columns, relationships, and metric definitions.

Rules:
1. Output ONLY SQL.
2. No explanation.
3. Prefer explicit JOIN conditions.
4. Respect the business intent, filters, and time granularity.
5. Use only syntax compatible with {dialect}.
6. Do not query system tables unless the user explicitly asks about database metadata.
7. Treat the retrieved metadata as possible relevant tables and columns, not as a requirement to use everything.
8. You do not need to use every retrieved table if a subset is sufficient.
9. Prefer a correct solution that uses fewer tables when the answer can be produced without extra joins or filters.

Intent:
{intent}

Query plan:
{query_plan}

Retrieved metadata:
{retrieved_context}

Question:
{question}
"""

    def generate(
        self,
        question,
        retrieved_context,
        intent,
        dialect="mysql",
        query_plan=None,
    ):
        """生成 SQL。"""
        prompt = self.build_generate_prompt(
            question=question,
            retrieved_context=retrieved_context,
            intent=intent,
            dialect=dialect,
            query_plan=query_plan,
        )
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "You output only SQL."},
                {"role": "user", "content": prompt},
            ],
            temperature=0,
        )
        return clean_sql(response.choices[0].message.content.strip())

    def build_fix_prompt(
        self,
        question,
        retrieved_context,
        bad_sql,
        error_message,
        dialect="mysql",
    ):
        """构造 SQL 修复提示词。"""
        return f"""
The following SQL produced an error.

Question:
{question}

Retrieved metadata:
{retrieved_context}

Bad SQL:
{bad_sql}

Error:
{error_message}

Fix the SQL for {dialect}. Output ONLY corrected SQL.
"""

    def fix(
        self,
        question,
        retrieved_context,
        bad_sql,
        error_message,
        dialect="mysql",
    ):
        """根据数据库返回的错误信息修复 SQL。"""
        prompt = self.build_fix_prompt(
            question=question,
            retrieved_context=retrieved_context,
            bad_sql=bad_sql,
            error_message=error_message,
            dialect=dialect,
        )
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "You output only SQL."},
                {"role": "user", "content": prompt},
            ],
            temperature=0,
        )
        return clean_sql(response.choices[0].message.content.strip())
