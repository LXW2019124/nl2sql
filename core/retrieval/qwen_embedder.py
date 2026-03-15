"""Qwen embedding 客户端封装。"""

import os

from openai import OpenAI

from core.env import load_env_file


class QwenEmbedder:
    """通过 OpenAI 兼容接口调用 Qwen embedding 模型。"""

    def __init__(
        self,
        client=None,
        api_key=None,
        base_url=None,
        model=None,
        dimensions=None,
    ):
        load_env_file()
        resolved_dimensions = int(dimensions or os.getenv("QWEN_EMBEDDING_DIMENSIONS") or 1024)
        self.client = client or OpenAI(
            api_key=api_key or os.getenv("QWEN_EMBEDDING_API_KEY") or os.getenv("DASHSCOPE_API_KEY"),
            base_url=base_url or os.getenv("QWEN_EMBEDDING_BASE_URL"),
        )
        self.model = model or os.getenv("QWEN_EMBEDDING_MODEL") or "text-embedding-v4"
        self.dimensions = resolved_dimensions
        self.size = resolved_dimensions

    def embed(self, text):
        """对单条文本生成 embedding。"""
        response = self.client.embeddings.create(
            model=self.model,
            input=text,
            dimensions=self.dimensions,
        )
        return response.data[0].embedding
