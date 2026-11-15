"""Answer a plain-English question about the claims by writing and running SQL.

Claude, called through Amazon Bedrock, is given the table's schema and one
tool, run_sql. It writes a query, this code checks it and runs it on Athena,
and Claude reads the result and answers. Every query it ran is returned with
the answer, so a person can check the work.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field

import boto3

from ai.athena import QueryFailed, run_query
from ai.sql_guard import MAX_ROWS, UnsafeSQL, check

# The model must be one this AWS account is allowed to call on Bedrock.
MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-sonnet-4-6")
MAX_TOOL_CALLS = 6
MAX_TOKENS = 4000

SYSTEM_PROMPT = f"""You answer questions about US flood insurance claims by querying a database.

The data is FEMA's National Flood Insurance Program claims, one row per claim,
with a date of loss from 2009 onward. It is in one Athena (Trino SQL) table:

nfip_claims
  claim_id                    bigint   unique id of the claim
  year_of_loss                int      partition column; filter on it when the question names years
  date_of_loss                date
  as_of_date                  date     when FEMA last updated the claim
  state                       string   two-letter code, upper case, e.g. 'FL'
  county_fips                 string   five-digit county code
  zip_code                    string
  rated_flood_zone            string   FEMA flood zone code, e.g. 'AE', 'X'
  flood_event                 string   event name when FEMA assigned one, e.g. 'Hurricane Sandy'; often null
  cause_of_damage_code        string   FEMA code; you do not have its meanings
  occupancy_type_code         int      FEMA code; you do not have its meanings
  is_primary_residence        boolean
  original_construction_date  date     null when unknown
  policy_count                int      insured units covered by the claim
  building_property_value     double   US dollars
  building_damage_amount      double   US dollars
  contents_damage_amount      double   US dollars
  building_coverage           double   US dollars of insurance on the building
  contents_coverage           double   US dollars of insurance on contents
  paid_building               double   US dollars paid on the building claim
  paid_contents               double   US dollars paid on the contents claim
  paid_icc                    double   US dollars paid for increased cost of compliance
  total_paid                  double   paid_building + paid_contents + paid_icc, nulls counted as zero

How to work:
- Use the run_sql tool to get every number you report. Never answer from memory.
- Write one SELECT at a time. Only nfip_claims can be queried. Results are cut off at {MAX_ROWS} rows, so aggregate in SQL.
- If a query fails, read the error, fix the query and try again.
- If the table cannot answer the question, say so plainly. Do not guess the meaning of a code column.
- Report only what your queries returned. Do not add causes, event names or background from your own knowledge; if an explanation would help, run a query for it (for example, group by flood_event) or leave it out.
- Answer in a few plain sentences, without tables or bold text. Give the figures as the query returned them, in full (for example $9,078,567,037, not "about $9.08 billion"), with units, and say which years or filters you applied."""

TOOLS = [
    {
        "toolSpec": {
            "name": "run_sql",
            "description": (
                "Run one read-only SELECT statement against the nfip_claims table on "
                "Athena and get the rows back as JSON. Call it whenever you need a "
                "number or a list from the data."
            ),
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "sql": {"type": "string", "description": "A single Trino SELECT statement."}
                    },
                    "required": ["sql"],
                }
            },
        }
    }
]


@dataclass
class Answer:
    text: str
    queries: list[str] = field(default_factory=list)
    input_tokens: int = 0
    output_tokens: int = 0
    bytes_scanned: int = 0
    stopped_early: bool = False


def _run_tool(sql: str, answer: Answer, athena_client) -> tuple[str, bool]:
    """Check and run one statement. Returns (text for the model, is_error)."""
    try:
        safe_sql = check(sql)
    except UnsafeSQL as error:
        return f"Rejected before running: {error}.", True
    answer.queries.append(sql)
    try:
        result = run_query(safe_sql, athena_client)
    except QueryFailed as error:
        return f"Athena error: {error}", True
    answer.bytes_scanned += result.bytes_scanned
    payload = {"columns": result.columns, "rows": result.rows, "row_count": len(result.rows)}
    if len(result.rows) == MAX_ROWS:
        payload["note"] = f"cut off at {MAX_ROWS} rows; aggregate in SQL to see everything"
    return json.dumps(payload), False


def ask(question: str, bedrock_client=None, athena_client=None) -> Answer:
    bedrock = bedrock_client or boto3.client("bedrock-runtime")
    athena = athena_client or boto3.client("athena")
    messages = [{"role": "user", "content": [{"text": question}]}]
    answer = Answer(text="")
    tool_calls = 0

    while True:
        response = bedrock.converse(
            modelId=MODEL_ID,
            system=[{"text": SYSTEM_PROMPT}],
            messages=messages,
            toolConfig={"tools": TOOLS},
            inferenceConfig={"maxTokens": MAX_TOKENS},
        )
        answer.input_tokens += response["usage"]["inputTokens"]
        answer.output_tokens += response["usage"]["outputTokens"]
        message = response["output"]["message"]
        messages.append(message)

        if response["stopReason"] != "tool_use":
            answer.text = "".join(block.get("text", "") for block in message["content"]).strip()
            if response["stopReason"] != "end_turn":
                answer.stopped_early = True
            return answer

        results = []
        for block in message["content"]:
            if "toolUse" not in block:
                continue
            tool_use = block["toolUse"]
            tool_calls += 1
            if tool_calls > MAX_TOOL_CALLS:
                text, is_error = "Query limit reached. Answer with what you have.", True
            else:
                text, is_error = _run_tool(tool_use["input"].get("sql", ""), answer, athena)
            results.append(
                {
                    "toolResult": {
                        "toolUseId": tool_use["toolUseId"],
                        "content": [{"text": text}],
                        "status": "error" if is_error else "success",
                    }
                }
            )
        # Every tool result goes back in one message.
        messages.append({"role": "user", "content": results})
