"""项目主入口与可编程 API。

该文件只保留两类职责：
1. 提供 `run_question()` 供其它 Python 代码直接调用。
2. 在脚本方式运行时，直接执行默认求解并输出结果。
"""

import os
import sys

from agent.planner_agent import PlannerAgent
from agent.solver import SQLProblemSolver
from core.env import load_env_file


DEFAULT_QUESTION = "编写解决方案找出每个用户的注册日期和在 2019 年作为买家的订单总数。"


def print_retrieval_trace(trace):
    """以可读的方式输出检索链路的追踪信息。"""
    if not trace:
        return

    trace_input = trace.get("input", {})
    print("\nRetrieval trace:")
    print("Input question:", trace_input.get("question"))
    print("Normalized question:", trace_input.get("normalized_question"))
    print("Translated question:", trace_input.get("translated_question"))
    print("Retrieval query:", trace_input.get("retrieval_query"))
    print("Top K:", trace_input.get("top_k"))
    print("Query plan:", trace_input.get("query_plan"))

    for operation in trace.get("operations", []):
        step = operation.get("step")
        print(f"\n[{step}]")
        if "selection_strategy" in operation:
            print("Selection strategy:", operation["selection_strategy"])
        output = operation.get("output")
        if isinstance(output, list):
            for item in output:
                print(item)
        else:
            print(output)

    trace_output = trace.get("output", {})
    print("\nSelected tables:", trace_output.get("selected_tables", []))


def run_question(
    question=None,
    db_url=None,
    metadata_path=None,
    execute=True,
    use_llm_planner=True,
):
    """运行一次问题求解，并返回结构化结果。

    参数未显式传入时：
    - `question` 回退到 `QUESTION` 环境变量，再回退到内置示例问题
    - `db_url` 回退到 `DB_URL` 环境变量
    """
    load_env_file()

    resolved_question = question or os.getenv("QUESTION") or DEFAULT_QUESTION
    resolved_db_url = db_url or os.getenv("DB_URL")

    if not resolved_db_url and not metadata_path:
        raise ValueError("Either DB_URL or metadata_path is required.")

    solver = SQLProblemSolver(
        metadata_path=metadata_path,
        db_url=resolved_db_url,
        planner_agent=PlannerAgent(use_llm=use_llm_planner),
    )
    return solver.solve(resolved_question, execute=execute)


def render_result(result, show_trace=False):
    """把求解结果渲染成终端输出。"""
    retrieval_backend = result.get("retrieval_backend") or {}
    if retrieval_backend:
        print("\nRetrieval backend:")
        print(retrieval_backend)

    retrieved_context = result.get("retrieved_context") or []
    if retrieved_context:
        print("\nRetrieved metadata:")
        for item in retrieved_context:
            print(item.get("table"), item)

    if show_trace:
        print_retrieval_trace(result.get("retrieval_trace"))
if __name__ == "__main__":
    render_result(run_question(), show_trace=False)
    sys.exit(0)
