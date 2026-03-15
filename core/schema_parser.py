"""题面 schema 抽取器。"""

import json
import os

from openai import OpenAI

from core.env import load_env_file
from core.prompts import SCHEMA_EXTRACTION_PROMPT


def _build_client():
    """根据环境变量创建 schema 抽取专用客户端。"""
    load_env_file()
    return OpenAI(
        api_key=os.getenv("SCHEMA_API_KEY") or os.getenv("OPENAI_API_KEY"),
        base_url=os.getenv("SCHEMA_BASE_URL") or os.getenv("OPENAI_BASE_URL"),
    )


def extract_schema(problem_text: str) -> dict:
    """从文本题面中抽取结构化 schema。"""
    client = _build_client()
    prompt = SCHEMA_EXTRACTION_PROMPT.format(problem_text=problem_text)

    response = client.chat.completions.create(
        model=os.getenv("SCHEMA_MODEL") or os.getenv("OPENAI_MODEL") or "deepseek-chat",
        messages=[
            {"role": "system", "content": "You output only JSON."},
            {"role": "user", "content": prompt},
        ],
        temperature=0,
    )

    content = response.choices[0].message.content

    # 保留原始输出打印，便于排查模型格式错误。
    print("Raw model output:")
    print(content)

    return json.loads(content)
