from typing import Any, Dict, Tuple, List, Optional
import httpx
from mcp.server.fastmcp import FastMCP
import os
import json
import uuid
from dotenv import load_dotenv, find_dotenv
import asyncio
import uuid
import httpx
load_dotenv(find_dotenv())

# Initialize FastMCP server
mcp = FastMCP("ddl_converter")

from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel
from snowflake.core import Root
from snowflake.snowpark import Session
import json
import sys
import logging
import warnings
warnings.filterwarnings("ignore")

load_dotenv(override=True)
# Configure logging to stderr
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='server.log',
    filemode='a'
)


logger = logging.getLogger(__name__)

class SnowflakeQuery(BaseModel):
    sql: str
    explanation: str

#global variable
session_state_messages = []

connection_parameters = {
    "account": "RYWNENJ-SAB31173",
    "user": "ILYAS",
    "password": "IlyasMaseed@260",
    "warehouse": "COMPUTE_WH",
    "role":"ACCOUNTADMIN",
    "database":"ILYAS_DB",
    "schema":"LLM_DEV"
}

session = Session.builder.configs(connection_parameters).create()

@mcp.tool(
    description="Executes a given query on snowflake and returns the status (either success or failure) and error message if any",
    annotations={"query": "The snowflake SQL query to execute."}
)
async def execute_snowflake_ddl(query:str) -> str:
    """Executes a given query on snowflake and returns the status (either success or failure) and error message if any.

    Args:
        query (str): The snowflake SQL query to execute.

    Returns:
        str: A message indicating the status ("Success" or "Failure") and an error message if any.
    """
    if not query.endswith(";"):
        query += ";"
    try:
        session.sql(query).collect()
        return "Success"
    except Exception as e:
        return f"Failure due to error : {str(e)}"

@mcp.tool(
    description="Function to search snowflake documentation for any information on datatypes, error messages etc.",
    annotations={"query": "The query to search in snowflake documentation."}
)
async def search_snowflake_documentation(query: str) -> str:
    """Searches Snowflake documentation for information related to the query.

    Args:
        query (str): The query to search in Snowflake documentation.

    Returns:
        str: The search results from Snowflake documentation.
    """
    root = Root(session)

    # fetch service
    my_service = (root
        .databases["SNOWFLAKE_DOCUMENTATION"]
        .schemas["SHARED"]
        .cortex_search_services["CKE_SNOWFLAKE_DOCS_SERVICE"]
    )

    # query service
    resp = my_service.search(
        query=query,
        columns=["CHUNK"],
        limit=3,
    )

    json_resp = json.loads(resp.to_json())

    context =  ""
    for result in json_resp["results"]:
        context += f"Chunk: {result['CHUNK']}\n"

    return context

@mcp.tool(
    description="Converts SQL Server DDL to Snowflake-compatible DDL or fixes faulty Snowflake DDL based on error messages and documentation context",
    annotations={
        "sqlserver_ddl": "The original SQL Server DDL statement to convert to Snowflake syntax",
        "previously_generated_snowflake_ddl": "Previously generated Snowflake DDL that needs correction or refinement",
        "documentation_context": "Relevant Snowflake documentation or context to guide the conversion/correction process",
        "error_message": "Error message encountered when executing the Snowflake DDL, used to identify and fix issues"
    }
)
async def ddl_conversion(sqlserver_ddl:str, 
                   previously_generated_snowflake_ddl:str, 
                   documentation_context:str, 
                   error_message:str) -> str:
    """
    Converts SQL Server DDL to Snowflake-compatible DDL or corrects existing Snowflake DDL.
    
    This tool supports two primary use cases:
    1. Initial conversion: Transforms SQL Server DDL syntax to Snowflake-compatible DDL
    2. Error correction: Takes faulty Snowflake DDL along with error messages and documentation 
       context to generate corrected DDL
    
    Args:
        sqlserver_ddl (str): The original SQL Server DDL statement to convert. This is the 
            primary input for initial conversions.
        previously_generated_snowflake_ddl (str): Previously generated Snowflake DDL 
            that encountered errors or needs improvement. Used in correction scenarios.
        documentation_context (str): Relevant Snowflake documentation, syntax guides, 
            or contextual information to inform the conversion or correction process.
        error_message (str): Error message returned by Snowflake when executing the 
            DDL. Used to identify specific issues and generate appropriate fixes.

    Returns:
        str: Snowflake-compatible DDL statement, either converted from SQL Server or 
             corrected based on the provided error context.
             
    Examples:
        # Initial conversion
        result = await ddl_conversion("CREATE TABLE dbo.Users (ID int IDENTITY(1,1) PRIMARY KEY)")
        
        # Error correction
        result = await ddl_conversion(
            sqlserver_ddl="...",
            previously_generated_snowflake_ddl="CREATE TABLE Users (ID NUMBER AUTOINCREMENT)",
            error_message="SQL compilation error: syntax error at 'AUTOINCREMENT'"
        )
    """
            
    if len(previously_generated_snowflake_ddl.strip()) == 0:
        previously_generated_snowflake_ddl = ""
    if len(documentation_context.strip()) == 0:
        documentation_context = "No additional documentation context provided."
    if len(error_message.strip()) == 0:
        error_message = "No error message provided." 
        
    prompt = f"""
        You are a SQL DDL conversion and correction assistant. Your primary responsibilities are:

        1. **Initial Conversion**: Convert SQL Server DDL statements into Snowflake-compatible DDL
        2. **Error Correction**: Fix faulty Snowflake DDL based on error messages and documentation context

        ## Data Type Conversion Mapping:
        int ⟶ INTEGER
        bigint ⟶ BIGINT
        smallint ⟶ SMALLINT
        tinyint ⟶ NUMBER(3, 0)
        bit ⟶ BOOLEAN
        decimal(p,s) ⟶ NUMBER(p,s)
        numeric(p,s) ⟶ NUMBER(p,s)
        money ⟶ NUMBER(19,4)
        smallmoney ⟶ NUMBER(10,4)
        float ⟶ FLOAT
        real ⟶ FLOAT
        datetime, datetime2, smalldatetime ⟶ TIMESTAMP_NTZ
        date ⟶ DATE
        time ⟶ TIME
        char(n) ⟶ VARCHAR
        varchar(n) ⟶ VARCHAR
        varchar(max) ⟶ VARCHAR
        nchar(n) ⟶ CHAR(n)
        nvarchar(n) ⟶ VARCHAR
        nvarchar(max) ⟶ VARCHAR
        text, ntext ⟶ STRING
        binary(n), varbinary(n) ⟶ BINARY
        varbinary(max), image ⟶ BINARY
        uniqueidentifier ⟶ STRING

        ## Constraint and Syntax Conversion:
        - PRIMARY KEY: Convert to PRIMARY KEY clause (column or table level)
        - FOREIGN KEY: Use standard foreign key syntax (note: constraints are not enforced in Snowflake)
        - DEFAULT value: Apply DEFAULT clauses as in SQL Server
        - IDENTITY(1,1): Replace with AUTOINCREMENT for column definition
        - CHECK: Use CHECK constraints as in SQL Server
        - Indexes: Omit INDEX definitions (Snowflake doesn't support explicit index creation)
        - Collation: Ignore any COLLATE statements
        - Comments: Convert to COMMENT clauses if present
        - Functions: Replace GETDATE() with CURRENT_TIMESTAMP() for defaults

        ## Processing Logic:

        **For Initial Conversion (when only SQL Server DDL is provided):**
        - Convert all DDL components following the above mappings
        - Remove or adjust elements that don't map to Snowflake
        - Mark any unconvertible features in comments

        **For Error Correction (when error message and/or previously generated DDL is provided):**
        - Analyze the error message to identify specific issues
        - Use documentation context to understand proper Snowflake syntax
        - Correct the previously generated Snowflake DDL based on error details
        - Apply fixes while maintaining the original intent of the conversion

        ## Output Requirements:
        - Return valid Snowflake DDL only
        - Provide response as separate key-value pairs: "sql" and "explanation"
        - Ensure SQL statement ends with semicolon (;)
        - Include brief explanation of changes made (especially for error corrections)
        - No extraneous comments in the SQL output

        ## Error Resolution Priority:
        When error message and documentation context are provided, prioritize:
        1. Syntax errors based on error message details
        2. Best practices from documentation context
        3. Snowflake-specific requirements and limitations
        4. Maintaining functional equivalence to original SQL Server DDL

        ---

        **Input Parameters:**
        SQL Server DDL: {sqlserver_ddl}
        Previously Generated Snowflake DDL: {previously_generated_snowflake_ddl}
        Documentation Context: {documentation_context}
        Error Message: {error_message}

    """
    logger.info(f"length of previously_generated_snowflake_ddl: {len(previously_generated_snowflake_ddl)}")
    # if len(previously_generated_snowflake_ddl.strip()) == 0:
    #     return """
    #         CREATE TABLE Employees (     
    #             EmployeeID INTEGER AUTOINCREMENT PRIMARY KEY,     
    #             FirstName VARCHAR(50) NOT NULL,     
    #             LastName VARCHAR(50) NOT NULL,     
    #             BirthDate DATE,     
    #             HireDate TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),     
    #             IsActive BOOLEAN DEFAULT 1 
    #         );
    #         """.replace("\n", " ").strip()

    openai = OpenAI(
        base_url="http://localhost:11434/v1",
        api_key="ollama"
    )

    messages = [{"role": "user", "content": prompt.format(sqlserver_ddl=sqlserver_ddl, 
                                                    previously_generated_snowflake_ddl=previously_generated_snowflake_ddl, 
                                                    documentation_context=documentation_context, 
                                                    error_message=error_message
                                                )}]
    response = openai.chat.completions.parse(model="mistral:7b", messages=messages, response_format=SnowflakeQuery)
    message = response.choices[0].message
    if message.parsed:
        return message.parsed.sql.replace("\n", " ").strip()
    return ""


if __name__ == "__main__":
    # Start the FastMCP server
    mcp.run(transport='stdio')
    #mcp.run(transport='streamable-http')
    
# uv run --with mcp mcp run server.py
    
# sqlserver_ddl = """CREATE TABLE Users (
#                         Id INT IDENTITY(1,1) PRIMARY KEY,
#                         UserName NVARCHAR(100) NOT NULL,
#                         CreatedAt DATETIME DEFAULT GETDATE(),
#                         IsActive BIT DEFAULT 1
#                     )
#                 """

# print(ddl_conversion(sqlserver_ddl))