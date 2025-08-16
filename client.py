from mcp.client.stdio import stdio_client
from mcp import ClientSession, StdioServerParameters
from mcp.client.sse import sse_client
import asyncio
import traceback
import os
import json
import httpx
import uuid
import requests
from typing import Any, Dict, Tuple, List, Optional, Callable
import inspect
from dotenv import load_dotenv, find_dotenv
import asyncio
import streamlit as st
load_dotenv(find_dotenv())

# Constants
SEMANTIC_MODEL_FILE = os.getenv("SEMANTIC_MODEL_FILE")
CORTEX_SEARCH_SERVICE = os.getenv("CORTEX_SEARCH_SERVICE")
SNOWFLAKE_ACCOUNT_URL = os.getenv("SNOWFLAKE_ACCOUNT_URL")
SNOWFLAKE_PAT = os.getenv("SNOWFLAKE_PAT")

# connecting via uv
server_params = StdioServerParameters(
    command = "uv",
    args=["run","server.py"]
)

session_state_messages = []

import logging

# logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.INFO)

# logger.info("This is a log message!")

def cortex_llm(cortex_tools, messages):    
    """Run the Cortex Complete function with the given query and obtain ToolCalling and Pass the ToolResult."""
    # Build your payload exactly as before
    payload = {
        "model": "claude-3-5-sonnet",
        "response_instruction": 
            """
            You are a helpful AI assistant for data analysis. 

            **CRITICAL: DDL Conversion Requirements**
            For ANY query related to SQL DDL conversion, correction, or troubleshooting, you MUST use the `ddl_conversion` tool. This includes:

            - **Initial DDL Conversion**: When user provides SQL Server DDL and wants Snowflake conversion
            → ALWAYS call `ddl_conversion` tool with the SQL Server DDL

            - **Error Correction**: When user reports errors with previously generated Snowflake DDL
            → ALWAYS call `ddl_conversion` tool with:
                - Original SQL Server DDL
                - The faulty Snowflake DDL
                - Error message provided
                - Any documentation context

            - **DDL Refinement**: When user wants to improve or modify existing Snowflake DDL
            → ALWAYS call `ddl_conversion` tool with appropriate parameters

            **DO NOT attempt to manually convert DDL syntax or fix DDL errors using your general knowledge.** The `ddl_conversion` tool is specifically designed for these tasks and has access to comprehensive conversion mappings and error resolution logic.

            If no tool is suitable or necessary for non-DDL queries, always try to be as helpful as possible using your general knowledge.

            Once you receive tool results, show ALL TOOL RESULTS in the final answer and then provide a coherent response that integrates the results into a final answer. 
            Do not just return the summary of the one or multiple tool results, but rather integrate them into a coherent answer.
            """,
        "tools": cortex_tools,
        "messages": messages,
        "stream": False
    }
    
    API_HEADERS = {
        "Authorization": f"Bearer {SNOWFLAKE_PAT}",
        "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
        "Content-Type": "application/json",
    }

    url = f"{SNOWFLAKE_ACCOUNT_URL}/api/v2/cortex/inference:complete"
    # Copy your API headers and add the SSE Accept
    headers = {
        **API_HEADERS,
        "Accept": "application/json" # For streaming response use "text/event-stream",
    }
    
    resp = requests.post(url, headers=headers, json=payload)
    resp.raise_for_status()
    result = resp.json()
    return result

def mcp_tool_dict_to_cortex_tool(tool):
    """
    Convert a tool dictionary (from MCP/FastMCP) to a Snowflake Cortex-compatible tool_spec dict.

    This function:
    - Accepts a dict representing a tool, including name, description, inputSchema, and annotations.
    - Extracts per-parameter descriptions from the annotations object (if present).
    - Removes any unnecessary "title" fields from input_schema.
    - Ensures every property in input_schema has a 'description' (using annotations or property title).
    - Returns a dict containing a 'tool_spec' key that matches the format required by the Cortex REST API.

    Args:
        tool (dict): A dictionary describing one tool as returned by MCP.

    Returns:
        dict: A dictionary with a 'tool_spec' key in Cortex REST API format.
    """
    # Get argument descriptions from annotations (if available)
    annotations = getattr(tool.get("annotations", {}), "__dict__", {}) if tool.get("annotations") else {}
    input_schema = dict(tool["inputSchema"])
    input_schema.pop("title", None)  # Optional, Cortex doesn't require this

    # Add descriptions from annotations to input_schema properties
    for pname, pinfo in input_schema.get("properties", {}).items():
        if "description" not in pinfo:
            if pname in annotations and annotations[pname]:
                pinfo["description"] = annotations[pname]
            elif "title" in pinfo:
                pinfo["description"] = pinfo["title"]

    return {
        "tool_spec": {
            "type": "generic",
            "name": tool["name"],
            "description": tool.get("description") or "",
            "input_schema": input_schema
        }
    }

def mcp_tools_to_cortex_tools(tools_list):
    """
    Convert a list of MCP tool objects or dicts into Cortex-compatible tool_spec dictionaries.

    Each entry in the list is transformed using mcp_tool_dict_to_cortex_tool().
    If an item is a non-dict object (e.g., a Tool instance), it is converted to a dict with vars().

    Args:
        tools_list (List[object|dict]): List of tool objects or dictionaries from MCP.

    Returns:
        List[dict]: List of dictionaries in Cortex 'tool_spec' format.
    """
    return [mcp_tool_dict_to_cortex_tool(vars(t) if not isinstance(t, dict) else t) for t in tools_list]

def get_tool_calls(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract tool calls from the API response.
    
    Args:
        resp (Dict[str, Any]): The API response dictionary.
        
    Returns:
        List[Dict[str, Any]]: A list of tool call dictionaries.
    """
    content_list = resp["choices"][0]["message"].get("content_list", [])
    tool_calls = [item for item in content_list if item.get("type") == "tool_use"]
    return tool_calls

def build_tool_results_list(tool_call_results):
    """
    Given a list of (tool_use_id, tool_name, result_text) tuples/dicts, 
    build content_list for Cortex reply.
    """
    content_list = []
    for res in tool_call_results:
        content_type = "text"
        content_text = res.get("text") or str(res["result"])
        content_list.append({
            "type": "tool_results",
            "tool_results": {
                "tool_use_id": res["tool_use_id"],
                "name": res["tool_name"],
                "content": [
                    {"type": content_type, "text": content_text}
                ]
            }
        })
    return content_list

async def call_all_tools(session, tool_calls):
    """
    For each tool_call in tool_calls, calls the tool via session.call_tool and collects the results.

    Args:
        session: MCP session object.
        tool_calls: List of dicts as returned by Cortex.

    Returns:
        List of tuples: (tool_call dict, response)
    """
    results = []
    for call in tool_calls:
        if call.get("type") == "tool_use":
            tool_use = call["tool_use"]
            tool_name = tool_use["name"]
            arguments = tool_use["input"]

            # Actually call the MCP tool!
            response = await session.call_tool(tool_name, arguments=arguments)
            # You may want to structure your result with the tool_use_id for Cortex
            results.append({
                "tool_use_id": tool_use["tool_use_id"],
                "tool_name": tool_name,
                "arguments": arguments,
                "response": response,
            })
    return build_tool_results_list(results)

async def build_cortex_tool_results_content_list(session, tool_calls):
    """
    For a list of tool call requests from a Cortex LLM response, execute each tool asynchronously via the MCP session,
    and build a content_list of tool_result blocks in the exact format required by the Snowflake Cortex REST API
    for tool-calling responses.

    Args:
        session: An active MCP client session, exposing an async call_tool() method.
        tool_calls (list): A list of dicts, where each dict contains a 'tool_use' block as produced by the Cortex LLM 
            (including tool_use_id, name, and input arguments).

    Returns:
        list: A list of 'tool_results' dicts, each matching the Cortex API "content_list" schema,
            suitable for use in a 'user' message in a follow-up Cortex conversation turn.

    The function:
        - Iterates over all tool_calls,
        - Executes the corresponding tool using session.call_tool(),
        - Extracts the textual result from the MCP tool response,
        - Constructs and appends a tool_results block (with type 'text') for each tool call.
    """

    content_list = []
    for call in tool_calls:
        tool_use = call['tool_use']
        result = await session.call_tool(tool_use['name'], arguments=tool_use['input'])
        result_vars = vars(result)
        if result_vars['content']:
            text_block = result_vars['content'][0]
            result_text = text_block.text if hasattr(text_block, "text") else str(text_block)
        else:
            result_text = ""
        content_list.append({
            "type": "tool_results",
            "tool_results": {
                "tool_use_id": tool_use['tool_use_id'],
                "name": tool_use['name'],
                "content": [{
                    "type": "text",
                    "text": result_text
                }]
            }
        })
    return content_list

_mcp_session = None

def initialize_mcp_session():
    async def get_mcp_session():
        global _mcp_session
        if _mcp_session is None:
            read, write = await stdio_client(server_params)
            session = ClientSession(read, write)
            await session.__aenter__()
            await session.initialize()
            _mcp_session = session
        return _mcp_session
    return asyncio.run(get_mcp_session())

async def run(input_query: str):
    try:
        print("Starting stdio_client...")
        async with stdio_client(server_params) as (read,write):
            print("client connected to server, creating session...")

            async with ClientSession(read, write) as session:
                print("Initializing session...")
                await session.initialize()
                print("Listing tools...")
                tools = await session.list_tools()
                tools_list = tools.tools
                cortex_tools = mcp_tools_to_cortex_tools(tools_list)
                print("cortex_tools :", cortex_tools)

                session_state_messages.append({"role": "user", "content": input_query})
                
                while True:
                    #print("**********Calling LLM***************")
                    llm_response = cortex_llm(cortex_tools, session_state_messages)
                    print("LLM response:", llm_response)

                    # Always add the LLM's response to the session message state
                    session_state_messages.append({
                        "role": "assistant",
                        "content_list": llm_response['choices'][0]['message']['content_list']
                    })
                    #print(f"session_state_messages: {session_state_messages}")

                    # --- Check if tool calls requested
                    tool_calls = get_tool_calls(llm_response)
                    print(f"tool_calls: {tool_calls}")

                    if not tool_calls or len(tool_calls) == 0:
                        # No tool call = FINAL ANSWER
                        print("LLM gave final answer, ending loop.")
                        break

                    # Otherwise, do all tool calls
                    print("Tools requested, executing...")
                    cortex_formatted_tool_results = await build_cortex_tool_results_content_list(session, tool_calls)
                    #print(f"cortex_formatted_tool_results: {cortex_formatted_tool_results}")

                    # Append tool results as user message, as required by Cortex
                    session_state_messages.append({
                        "role": "user",
                        "content_list": cortex_formatted_tool_results
                    })
                    #print(f"session_state_messages: {session_state_messages}")
                    # Loop continues, will call LLM again with updated message history

                # End of loop, last llm_response is the final answer
                # Optionally process llm_response or session_state_messages for output
        for msg in reversed(session_state_messages):
            if msg.get("role") == "assistant" and "content_list" in msg:
                for block in msg["content_list"]:
                    if block.get("type") == "text":
                        return block.get("text")
        return None


    except Exception as e:
        print("An error occurred:", e)
        traceback.print_exc()

if __name__ == "__main__":
    # input_query = "What is the total product gross revenue across the US?"
    # input_query = "Tell me an interesting fact about F1 cars"  
    # final_response = asyncio.run(run(input_query))
    # print("Final response:", final_response)

    st.set_page_config(page_title="Cortex Chatbot", page_icon="🤖")

    st.title("🔗 Cortex Tool-Calling Chatbot")

    # Keep chat history in session
    if "messages" not in st.session_state:
        st.session_state.messages = [
            {"role": "assistant", "content": "Hi! Ask me anything."}
        ]

    # Render chat history using the chat-specific UI
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # This handles the chat input at the bottom, perfect for a chatbot!
    if prompt := st.chat_input("Type your message..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        # Display the user's message as they type
        with st.chat_message("user"):
            st.markdown(prompt)
        # Agent "thinking" spinner
        with st.spinner("Thinking..."):
            response = asyncio.run(run(prompt))
            st.session_state.messages.append({"role": "assistant", "content": response})
            with st.chat_message("assistant"):
                st.markdown(response)