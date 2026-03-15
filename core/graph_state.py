"""LangGraph 状态定义。"""

from typing import Any, Dict, List, TypedDict


class SQLAgentState(TypedDict, total=False):
    """工作流在各节点之间传递的共享状态。

    `total=False` 表示节点只需返回本阶段新增/更新的字段，不要求一次填满所有键。
    """

    question: str
    execute: bool
    db_url: str
    dialect: str
    intent: Dict[str, Any]
    query_plan: Dict[str, Any]
    retrieved_context: List[Dict[str, Any]]
    retrieved_tables: List[str]
    retrieval_trace: Dict[str, Any]
    retrieval_backend: Dict[str, Any]
    sql_query: str
    phase_outputs: Dict[str, Any]
    execution_result: List[Any]
    error: str
    status: str
