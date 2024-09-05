import aioodbc
import asyncio
import pyodbc  # Import pyodbc for error handling
from flask import Flask, request, Response
import os
from dotenv import load_dotenv
import csv
from io import StringIO
import logging

# Load environment variables
load_dotenv()

# Set up logging to file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("api_log.log"),  # Log to a file
        logging.StreamHandler()  # Log to console
    ]
)

app = Flask(__name__)

# Load the API keys as a list from the .env file
API_KEYS = os.getenv("API_KEYS").split(',')

# Authentication function to check if the provided API key is valid
def authenticate(request):
    api_key = request.headers.get("x-api-key")  # Get the API key from the request headers
    if api_key in API_KEYS:
        logging.info(f"Authenticated API key: {api_key}")
        return True
    logging.warning(f"Unauthorized access attempt with API key: {api_key}")
    return False

# Set up an asynchronous ODBC connection
async def get_async_connection(loop):
    connection_string = (
        f"DRIVER={os.getenv('DB_DRIVER')};"
        f"SERVER={os.getenv('DB_SERVER')};"
        f"DATABASE={os.getenv('DB_DATABASE')};"
        f"UID={os.getenv('DB_UID')};"
        f"PWD={os.getenv('DB_PWD')};"
    )
    connection = await aioodbc.connect(dsn=connection_string, loop=loop)
    return connection

# Function to execute a single SQL query asynchronously
async def execute_query(cursor, sql_query):
    await cursor.execute(sql_query)
    return await cursor.fetchall()

# Route to handle multiple queries in one request
@app.route('/query', methods=['POST'])
async def query_data():
    api_key = request.headers.get("x-api-key")
    if not authenticate(request):
        return "Unauthorized access", 403  # Return 403 if the API key is invalid
    
    queries = request.json.get('queries')  # Get the list of SQL queries from the request body
    
    if not queries:
        logging.error(f"No SQL queries provided - API key: {api_key}")
        return "No SQL queries provided", 400
    
    # Ensure queries are sent as a list of SQL strings
    if not isinstance(queries, list):
        return "Invalid format for SQL queries", 400
    
    try:
        # Log the batch of queries being executed
        logging.info(f"Executing batch queries - API key: {api_key}")
        
        # Setup asyncio event loop for asynchronous execution
        loop = asyncio.get_event_loop()
        connection = await get_async_connection(loop)
        cursor = await connection.cursor()
        
        all_results = []
        for sql_query in queries:
            logging.info(f"Executing query: {sql_query} - API key: {api_key}")
            
            # Execute the raw SQL query
            rows = await execute_query(cursor, sql_query)
            if not rows:
                logging.info(f"No data returned for query: {sql_query} - API key: {api_key}")
            else:
                columns = [column[0] for column in cursor.description]
                all_results.append({
                    "columns": columns,
                    "rows": rows
                })
        
        # Create CSV output
        output = StringIO()
        writer = csv.writer(output)
        
        # Write the results of all queries to the output
        for result in all_results:
            writer.writerow(result["columns"])  # Write the header (column names)
            writer.writerows(result["rows"])    # Write the rows of data
        
        await cursor.close()
        await connection.close()
        
        logging.info(f"Batch queries executed successfully - API key: {api_key}")
        
        # Return the CSV content with the proper response headers
        return Response(output.getvalue(), mimetype='text/csv')
    
    except pyodbc.Error as e:  # Changed to pyodbc.Error
        logging.error(f"SQL syntax error or execution error: {str(e)} - API key: {api_key}")
        return f"SQL syntax error or execution error: {str(e)}", 400
    
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)} - API key: {api_key}")
        return f"An unexpected error occurred: {str(e)}", 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
