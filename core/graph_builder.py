"""LangGraph 工作流构建器。"""

from langgraph.graph import END, StateGraph

from core.graph_state import SQLAgentState


def build_sql_agent_graph(parse_intent_node, plan_query_node, retrieve_node, generate_sql_node, verify_node):
    """构建当前项目的线性 SQL 求解图。

    目前流程是严格串行的，后续如果要引入分支、并行节点或人工校验节点，可以从这里扩展。
    """
    # 协调器掌控总流程，并把具体任务委托给各阶段节点。
    graph = StateGraph(SQLAgentState)
    graph.add_node("parse_intent", parse_intent_node)
    graph.add_node("plan_query", plan_query_node)
    graph.add_node("retrieve_context", retrieve_node)
    graph.add_node("generate_sql", generate_sql_node)
    graph.add_node("verify_sql", verify_node)

    graph.set_entry_point("parse_intent")
    graph.add_edge("parse_intent", "plan_query")
    graph.add_edge("plan_query", "retrieve_context")
    graph.add_edge("retrieve_context", "generate_sql")
    graph.add_edge("generate_sql", "verify_sql")
    graph.add_edge("verify_sql", END)

    return graph.compile()
