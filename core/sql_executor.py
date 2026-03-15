"""SQL 执行器。"""

import os

from sqlalchemy import create_engine, text

from core.env import load_env_file


def execute_sql(sql_query: str, db_url: str = None):
    """执行 SQL 并返回统一结果结构。"""
    load_env_file()
    db_url = db_url or os.getenv("DB_URL")
    if not db_url:
        return {"success": False, "error": "DB_URL is required for SQL execution"}

    engine = None
    try:
        engine = create_engine(db_url)
        with engine.connect() as connection:
            result = connection.execute(text(sql_query))
            return {"success": True, "results": result.fetchall()}
    except Exception as error:
        return {"success": False, "error": str(error)}
    finally:
        if engine is not None:
            engine.dispose()
