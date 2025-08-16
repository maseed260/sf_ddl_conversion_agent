from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel
from snowflake.core import Root
from snowflake.snowpark import Session
import json
import warnings
warnings.filterwarnings("ignore")

load_dotenv(override=True)

class SnowflakeQuery(BaseModel):
    sql: str
    explanation: str

#global variable
session_state_messages = []

def execute_snowflake_ddl(query):
    if not query.endswith(";"):
        query += ";"
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
    try:
        session.sql(query).collect()
        return ("Success",None)
    except Exception as e:
        return ("Failure", str(e))

def ddl_conversion(sqlserver_ddl):
    prompt = f"""
        You are a SQL DDL conversion assistant. Your job is to convert SQL Server DDL statements into Snowflake-compatible DDL.
        Follow these mapping rules and instructions for columns, datatypes, and constraints:

        Data Type Conversion Mapping :
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

        Constraint and Syntax Conversion :
        PRIMARY KEY: Convert SQL Server primary keys to PRIMARY KEY clause (column or table level).
        FOREIGN KEY: Use standard foreign key syntax—note that in Snowflake, constraints are not enforced.
        DEFAULT value: Apply DEFAULT clauses as in SQL Server.
        IDENTITY(1,1): Replace with AUTOINCREMENT for the column definition.
        CHECK: Use CHECK constraints as in SQL Server.
        Indexes: Omit INDEX definitions; Snowflake doesn’t support creating them explicitly.
        Collation: Ignore any COLLATE statements.
        Comments: Convert to COMMENT clauses if present.
        Functions: Replace GETDATE() with CURRENT_TIMESTAMP() for defaults.

        Instructions:

        Convert all DDL components, following the above mappings.
        Output only valid Snowflake DDL—no explanations or extra comments.
        Clearly mark any unconvertible features or errors in a comment above the relevant part.
        Remove or adjust any elements from SQL Server that do not directly map to Snowflake, following best practices.
        
        Return the sql and explanation as separate key value pairs. Make sure the sql ends with ;
        
        Here is the SQL Server DDL : {sqlserver_ddl}
    """
    
    openai = OpenAI(
        base_url="http://localhost:11434/v1",
        api_key="ollama"
    )

    session_state_messages.append({"role": "user", "content": prompt.format(sqlserver_ddl=sqlserver_ddl)})

    retries = 0
    max_retries = 3
    error_dict = {"0":"""
                  CREATE TABLE Users (Id INTEGER AUTOINCREMENT PRIMARY KEY, UserName VARCHAR(100) NOT NULL, CreatedAt TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), IsActive bool DEFAULT true);
                  """,
                  "1":"""
                  CREATE TABLE Users (Id INTEGER AUTOINCREMENT PRIMARY KEY, UserName VARCHA(100) NOT NULL, CreatedAt TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), IsActive boolean DEFAULT true);
                  """,
                  "2":"""
                  CREATE TABLE Users (Id INTEGER AUTOINCREMENT PRIMARY KEY, UserName VARCHAR(100) NOT NULL, CreatedAt TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAM(), IsActive boolean DEFAULT true);
                  """,
                  "3":"""
                  CREATE TABLE Users (Id INTEGER AUTOINCREMENT PRIMARY KEY, UserName VARCHAR(100) NOT NULL, CreatedAt TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(), IsActive boolean DEFAULT true);
                  """}
    while retries < max_retries:
        response = openai.chat.completions.parse(model="mistral:7b", messages=session_state_messages, response_format=SnowflakeQuery)
        message = response.choices[0].message
        session_state_messages.append({"role": "assistant","content": message.content})
        if message.parsed:
            print("Snowflake sql: ",message.parsed.sql)
            print("Explanation: ", message.parsed.explanation)
            status, error_msg = execute_snowflake_ddl(message.parsed.sql)
            if status == "Success":
                print("DDL executed successfully.")
                return message.parsed.sql
            else:
                print(f"Retrying...")
                session_state_messages.append({"role": "user","content": error_msg})
                retries += 1
        else:
            print(message)
        
        ######## Debug only #########
        # message = error_dict[str(retries)]
        # session_state_messages.append({"role": "assistant","content": message})
        # if message:
        #     print("Snowflake sql: ",message)
        #     status, error_msg = execute_snowflake_ddl(message)
        #     if status == "Success":
        #         print("DDL executed successfully.")
        #         return message
        #     else:
        #         print(f"Retrying due to error {error_msg}")
        #         session_state_messages.append({"role": "user","content": message + "\n" + error_msg})
        #         retries += 1
        # else:
        #     print(message)
        
    print("Max retries reached. Could not convert DDL.")
    return message
    
    
sqlserver_ddl = """CREATE TABLE Users (
                        Id INT IDENTITY(1,1) PRIMARY KEY,
                        UserName NVARCHAR(100) NOT NULL,
                        CreatedAt DATETIME DEFAULT GETDATE(),
                        IsActive BIT DEFAULT 1
                    )
                """

print(ddl_conversion(sqlserver_ddl))
