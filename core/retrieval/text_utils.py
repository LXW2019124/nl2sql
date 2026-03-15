"""检索层文本处理工具。"""

import re


TOKEN_PATTERN = re.compile(r"[a-zA-Z0-9_]+|[\u4e00-\u9fff]+")
STOPWORDS = {
    "a",
    "an",
    "and",
    "as",
    "by",
    "find",
    "for",
    "from",
    "in",
    "is",
    "of",
    "on",
    "the",
    "to",
    "with",
}


def normalize_token(token):
    """把 token 归一化成更利于匹配的形式。"""
    token = token.lower()
    if re.search(r"[\u4e00-\u9fff]", token):
        return token
    if token.endswith("ies") and len(token) > 3:
        return token[:-3] + "y"
    if token.endswith("s") and len(token) > 3:
        return token[:-1]
    return token


def tokenize(text):
    """按中英文混合规则切分文本并过滤停用词。"""
    tokens = []
    for raw in TOKEN_PATTERN.findall(text or ""):
        for part in raw.split("_"):
            normalized = normalize_token(part)
            if normalized and normalized not in STOPWORDS:
                tokens.append(normalized)
    return tokens
