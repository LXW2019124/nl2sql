"""查询改写器。

该模块保留 `rewrite` 作为统一入口，但内部拆成两个阶段：
1. `normalize`：轻量归一化，占位未来扩展点
2. `translate`：把中文问题转换成适合 English schema retrieval 的英文查询
"""

import os
import re

from openai import OpenAI

from core.env import load_env_file


def _contains_cjk(text):
    """判断文本中是否包含中文字符。"""
    return bool(re.search(r"[\u4e00-\u9fff]", text or ""))


def _clean_translation(text):
    """清理模型输出中的多余空白与 Markdown 包裹。"""
    cleaned = re.sub(r"```(?:text)?", "", text or "", flags=re.IGNORECASE)
    cleaned = re.sub(r"```", "", cleaned)
    return " ".join(cleaned.strip().split())


class QueryRewriter:
    """对自然语言问题做轻量归一，并在需要时翻译成英文检索问句。"""

    def __init__(
        self,
        synonym_map=None,
        client=None,
        model=None,
        api_key=None,
        base_url=None,
    ):
        load_env_file()
        self.synonym_map = synonym_map or {
            "customers": "users",
            "customer": "user",
            "daily": "day",
        }
        self.phrase_replacements = [
            (re.compile(r"\bcancel\s+rate\b"), "cancellation rate"),
        ]
        self.model = model or os.getenv("RETRIEVAL_TRANSLATION_MODEL") or os.getenv("OPENAI_MODEL") or "deepseek-chat"
        self.client = client or self._build_client(api_key=api_key, base_url=base_url)

    def _build_client(self, api_key=None, base_url=None):
        """按需构建翻译客户端。"""
        resolved_api_key = api_key or os.getenv("RETRIEVAL_TRANSLATION_API_KEY") or os.getenv("OPENAI_API_KEY")
        resolved_base_url = base_url or os.getenv("RETRIEVAL_TRANSLATION_BASE_URL") or os.getenv("OPENAI_BASE_URL")
        if not resolved_api_key:
            return None
        return OpenAI(api_key=resolved_api_key, base_url=resolved_base_url)

    def normalize(self, question):
        """执行当前阶段的轻量归一化。

        这里暂时只保留非常轻的英文同义词和短语归一，把更强的改写能力留给后续扩展。
        """
        normalized = " ".join((question or "").strip().split())
        lowered = normalized.lower()

        for pattern, replacement in self.phrase_replacements:
            lowered = pattern.sub(replacement, lowered)

        tokens = []
        for token in lowered.split():
            tokens.append(self.synonym_map.get(token, token))

        return " ".join(tokens)

    def _build_translation_prompt(self, normalized_question):
        """构造检索向翻译提示词。"""
        return f"""
Translate the following database question into concise English for schema retrieval.

Rules:
1. Output only one English query sentence.
2. Preserve numbers, years, dates, status values, and English identifiers.
3. Keep business roles such as buyer, seller, user, customer, order, registration date.
4. Do not explain, summarize, or add assumptions.
5. Prefer wording that will match English database schema and metadata.

Question:
{normalized_question}
"""

    def translate(self, normalized_question):
        """把问题翻译成英文检索问句。"""
        if not _contains_cjk(normalized_question):
            return normalized_question

        if self.client is None:
            # 没有可用翻译客户端时，回退到归一化结果，避免阻断检索流程。
            return normalized_question

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You output only concise English retrieval queries."},
                    {"role": "user", "content": self._build_translation_prompt(normalized_question)},
                ],
                temperature=0,
            )
            translated = _clean_translation(response.choices[0].message.content)
            return translated or normalized_question
        except Exception:
            return normalized_question

    def rewrite(self, question):
        """执行统一改写入口，并返回结构化结果。"""
        normalized_question = self.normalize(question)
        translated_question = self.translate(normalized_question)
        retrieval_query = translated_question
        return {
            "original_question": question,
            "normalized_question": normalized_question,
            "translated_question": translated_question,
            "retrieval_query": retrieval_query,
        }
