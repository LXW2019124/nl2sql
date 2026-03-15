"""SQL 执行结果校验工具。"""


def normalize_value(value):
    """把 `None` 统一转换成可排序占位符，便于比较结果集。"""
    if value is None:
        return "__NULL__"
    return value


def normalize_rows(rows):
    """把任意结果行规整成元组列表。"""
    normalized = []
    for row in rows:
        if isinstance(row, (list, tuple)):
            normalized.append(tuple(normalize_value(v) for v in row))
        else:
            normalized.append((normalize_value(row),))
    return normalized


def validate_result(actual, expected, enforce_order=False):
    """比较实际结果与期望结果。

    - `expected is None` 表示不做强校验。
    - 默认忽略顺序，只比较集合内容。
    """
    if expected is None:
        return True

    actual_norm = normalize_rows(actual)
    expected_norm = normalize_rows(expected)

    if enforce_order:
        return actual_norm == expected_norm

    return sorted(actual_norm) == sorted(expected_norm)
