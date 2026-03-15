"""Qdrant 向量检索后端。"""

import hashlib
import os

from core.env import load_env_file
from core.retrieval.chunk_builder import build_chunks
from core.retrieval.qwen_embedder import QwenEmbedder


class HashEmbedder:
    """本地占位 embedding 实现。

    当没有配置真实 embedding API 时，仍然允许项目以“可运行但效果较弱”的形式工作。
    """

    def __init__(self, size=16):
        self.size = size

    def embed(self, text):
        """把文本稳定映射为固定长度向量。"""
        digest = hashlib.sha256((text or "").encode("utf-8")).digest()
        values = []
        for index in range(self.size):
            values.append(digest[index] / 255.0)
        return values


class QdrantVectorBackend:
    """负责索引 metadata chunk 并执行向量检索。"""

    def __init__(self, client=None, embedder=None, collection_name="metadata_vectors", url=None):
        load_env_file()
        self.client = client or self._build_client(url=url)
        self.embedder = embedder or self._build_embedder()
        self.collection_name = collection_name or os.getenv("QDRANT_COLLECTION_NAME") or "metadata_vectors"

    def _build_client(self, url=None):
        """创建 Qdrant 客户端。"""
        from qdrant_client import QdrantClient

        return QdrantClient(url=url or os.getenv("QDRANT_URL", "http://localhost:6333"))

    def _build_embedder(self):
        """优先使用真实 embedding API，否则回退到哈希 embedding。"""
        if os.getenv("QWEN_EMBEDDING_API_KEY") or os.getenv("DASHSCOPE_API_KEY"):
            return QwenEmbedder()
        return HashEmbedder()

    def index_tables(self, tables):
        """把表 metadata 编码成 chunk 并写入 Qdrant。"""
        self._ensure_collection()
        chunks = build_chunks({"tables": tables})
        points = []
        for index, chunk in enumerate(chunks):
            text = self._chunk_text(chunk)
            points.append(
                self._point(
                    point_id=index,
                    vector=self.embedder.embed(text),
                    payload={
                        "table": chunk.get("table"),
                        "chunk_type": chunk.get("chunk_type"),
                        "chunk_id": chunk.get("chunk_id"),
                        "columns": chunk.get("columns", []),
                        "text": text,
                    },
                )
            )

        self.client.upsert(collection_name=self.collection_name, points=points)

    def search(self, question, top_k):
        """检索与问题最相近的 chunk，并聚合回表级结果。"""
        query_vector = self.embedder.embed(question)
        if hasattr(self.client, "search"):
            hits = self.client.search(
                collection_name=self.collection_name,
                query_vector=query_vector,
                limit=top_k,
            )
        else:
            response = self.client.query_points(
                collection_name=self.collection_name,
                query=query_vector,
                limit=top_k,
            )
            hits = response.points
        grouped = {}
        for hit in hits:
            table = hit.payload.get("table")
            if not table:
                continue
            existing = grouped.get(table)
            if existing is None or hit.score > existing["vector_score"]:
                grouped[table] = {
                    "table": table,
                    "vector_score": hit.score,
                    "matched_chunk_type": hit.payload.get("chunk_type"),
                    "matched_chunk_id": hit.payload.get("chunk_id"),
                    "matched_chunk_columns": hit.payload.get("columns", []),
                }

        return sorted(grouped.values(), key=lambda item: (-item["vector_score"], item["table"]))[:top_k]

    def _ensure_collection(self):
        """在 collection 不存在时自动创建。"""
        if self.client.collection_exists(self.collection_name):
            return

        vectors_config = self._vector_params(size=self.embedder.size)
        self.client.create_collection(
            collection_name=self.collection_name,
            vectors_config=vectors_config,
        )

    def _vector_params(self, size):
        """兼容不同版本 qdrant-client 的向量参数结构。"""
        try:
            from qdrant_client import models

            return models.VectorParams(size=size, distance=models.Distance.COSINE)
        except Exception:
            return {"size": size, "distance": "Cosine"}

    def _point(self, point_id, vector, payload):
        """兼容不同版本 qdrant-client 的点对象结构。"""
        try:
            from qdrant_client import models

            return models.PointStruct(id=point_id, vector=vector, payload=payload)
        except Exception:
            return {"id": point_id, "vector": vector, "payload": payload}

    def _chunk_text(self, chunk):
        """把 chunk 各字段拼接成用于 embedding 的文本。"""
        return " ".join(
            [
                chunk.get("table", ""),
                chunk.get("chunk_type", ""),
                chunk.get("text", ""),
                " ".join(chunk.get("keywords", [])),
                " ".join(chunk.get("columns", [])),
                " ".join(chunk.get("neighbors", [])),
            ]
        ).strip()
