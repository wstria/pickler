import pyodbc
from flask import Flask, jsonify, request
import sys
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# Set up the ODBC connection
def get_connection():
    connection_string = (
        f"DRIVER={os.getenv('DB_DRIVER')};"
        f"SERVER={os.getenv('DB_SERVER')};"
        f"DATABASE={os.getenv('DB_DATABASE')};"
        f"UID={os.getenv('DB_UID')};"
        f"PWD={os.getenv('DB_PWD')};"
    )
    connection = pyodbc.connect(connection_string)
    return connection


# Route to execute a query and return the results as JSON
@app.route('/query', methods=['POST'])  # Changed to POST
def query_data():
    sql_query = request.json.get('sql')  # Get the SQL query from the request body
    
    if not sql_query:
        return jsonify({"error": "No SQL query provided"}), 400  # Error Handler
    
    try:
        # Attempt Connection
        connection = get_connection()
        cursor = connection.cursor()
        
        # Execute the query
        cursor.execute(sql_query)  # Execute Query
        rows = cursor.fetchall()  # Grab all Data
        
        if not rows:
            # No data returned
            return jsonify({"message": "Query executed successfully, but no data was returned."}), 200
        
        # Get column names
        columns = [column[0] for column in cursor.description]
        
        # Convert rows to list of dictionaries
        results = [dict(zip(columns, row)) for row in rows]
        
        # Close the connection
        cursor.close()
        connection.close()
        
        # Return results as JSON for easier formatting
        return jsonify(results)
    
    except pyodbc.Error as e:
        # Handle syntax errors in the SQL query
        return jsonify({"error": "SQL syntax error or execution error.", "details": str(e)}), 400
    
    except Exception as e:
        # Handle any other unexpected errors
        return jsonify({"error": "An unexpected error occurred.", "details": str(e)}), 500

if __name__ == '__main__':  # Main Function
    if len(sys.argv) > 1:
        sql_query = ' '.join(sys.argv[1:])
        
        try:
            connection = get_connection()
            cursor = connection.cursor()
            
            cursor.execute(sql_query)
            rows = cursor.fetchall()
            
            columns = [column[0] for column in cursor.description]
            for row in rows:
                result = dict(zip(columns, row))
                print(result)
            
            cursor.close()
            connection.close()
        
        except Exception as e:
            print(f"Error: {str(e)}")
    else:
        app.run(debug=True, host='0.0.0.0', port=5000)
