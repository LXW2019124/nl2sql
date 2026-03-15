"""集中存放模型提示词。"""

SCHEMA_EXTRACTION_PROMPT = """
You are a database schema extraction engine.

Extract all tables, columns, and sample data from the SQL problem description.

Rules:
1. Output STRICT JSON only.
2. No explanation.
3. Data types must be one of:
   - INTEGER
   - TEXT
   - REAL
   - DATE
4. Do not fabricate sample data. Use an empty array when sample data is not provided.
5. Extract expected output if provided in the problem.
Output format:

{{
  "tables": [
    {{
      "name": "...",
      "columns": [
        {{"name": "...", "type": "..."}}
      ],
      "sample_data": []
    }}
  ],
  "expected_output": [
    [value1, value2]
  ]
}}

Problem description:
---------------------
{problem_text}
"""
