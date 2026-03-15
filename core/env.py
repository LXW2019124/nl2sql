"""环境变量加载工具。

项目没有依赖 `python-dotenv`，而是提供了一个极简 `.env` 解析器，保证：
1. 运行时零额外依赖。
2. 测试中可以显式指定 `.env` 路径。
3. 只覆盖未设置的环境变量，除非传入 `override=True`。
"""

import os
from pathlib import Path


def default_env_path():
    """返回默认的 `.env` 路径。"""
    return Path(__file__).resolve().parent.parent / ".env"


def load_env_file(path=None, override=False):
    """从 `.env` 文件中读取环境变量。

    参数:
    - path: 可选的 env 文件路径；未提供时读取项目根目录下的 `.env`
    - override: 为 `True` 时允许覆盖当前进程已有的环境变量
    """
    env_path = Path(path) if path else default_env_path()
    if not env_path.exists():
        return env_path

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]

        if override or key not in os.environ:
            os.environ[key] = value

    return env_path
