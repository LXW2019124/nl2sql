"""metadata 缓存。

数据库 schema 探查相对昂贵，因此项目允许把抽取到的 metadata 按数据库连接串做哈希缓存。
"""

import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path


class MetadataCache:
    """基于本地 JSON 文件的轻量缓存。"""

    def __init__(self, cache_dir):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def load(self, db_url):
        """按数据库连接串读取缓存。"""
        cache_path = self._cache_path(db_url)
        if not cache_path.exists():
            return None
        with open(cache_path, "r", encoding="utf-8") as handle:
            return json.load(handle)

    def save(self, db_url, payload):
        """保存缓存，并把日期、Decimal 等对象转成 JSON 兼容格式。"""
        cache_path = self._cache_path(db_url)
        with open(cache_path, "w", encoding="utf-8") as handle:
            json.dump(self._make_json_safe(payload), handle, ensure_ascii=False, indent=2)
        return cache_path

    def _cache_path(self, db_url):
        """为数据库连接串生成稳定的缓存文件名。"""
        digest = hashlib.sha256(db_url.encode("utf-8")).hexdigest()[:16]
        return self.cache_dir / f"{digest}.json"

    def _make_json_safe(self, value):
        """递归地把非常规对象转成 JSON 可序列化类型。"""
        if isinstance(value, dict):
            return {key: self._make_json_safe(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._make_json_safe(item) for item in value]
        if isinstance(value, tuple):
            return [self._make_json_safe(item) for item in value]
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return str(value)
        return value
