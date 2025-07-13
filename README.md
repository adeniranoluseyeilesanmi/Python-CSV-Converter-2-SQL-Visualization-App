# Python-CSV-Converter-2-SQL-Visualization-App
This script provides a comprehensive solution. Here's a breakdown:

1. Setup & Dependencies:

It uses pandas for efficient CSV reading and data handling.

sqlite3 (a built-in Python module) is used for the in-memory SQL database.

matplotlib is used for generating plots.

You'll need to install these if you haven't already: pip install pandas matplotlib

2. Core Functions:

sanitize_column_name(col_name): Cleans up column names from the CSV to make them valid SQL identifiers (e.g., replaces spaces with underscores).

infer_sqlite_type(series): This is a key function. It takes a pandas Series (a column from your data) and tries to determine the most appropriate SQLite data type (INTEGER, REAL, TEXT). It's more robust than just mapping pandas dtypes directly, especially for 'object' types that might contain numbers.

create_table_from_csv(csv_filepath):

Prompts the user for the CSV file path.

Reads the CSV into a pandas DataFrame. Pandas automatically infers data types for columns.

Sanitizes column names.

Derives a table name from the CSV filename (e.g., my_data.csv becomes my_data_table).

Connects to an in-memory SQLite database (meaning the database exists only while the script runs).

Dynamically constructs a CREATE TABLE SQL statement based on the sanitized column names and the inferred SQLite types.

Executes the CREATE TABLE statement.

Displays the created table name and the inferred types for each column.

Uses pandas to_sql() method to efficiently load the data from the DataFrame into the newly created SQLite table. This method handles type conversions and NaN values (converting them to NULL in SQL).

Returns the database connection, table name, and the initial DataFrame.

execute_sql_query(conn, query):

Takes a database connection and an SQL query string.

If the query is a SELECT statement, it uses pd.read_sql_query to fetch results into a DataFrame and prints them in a readable format.

For other SQL commands (like UPDATE, INSERT, DELETE), it executes them and prints the number of affected rows.

Includes error handling for SQLite errors.

visualize_data(conn, table_name, df_original):

Provides a sub-menu for choosing a visualization type.

Currently supports:

Bar Chart: Prompts for an X-axis (categorical) and Y-axis (numerical) column.

Histogram: Prompts for a numerical column and an optional number of bins.

Scatter Plot: Prompts for two numerical X and Y columns.

Pie Chart: Prompts for a categorical column for labels and a numerical column for values. It aggregates values by label.

Line Plot: Prompts for X and Y columns. Sorts data by X-axis for a proper line plot.

It fetches the current data from the SQLite table before plotting, so any SQL modifications are reflected.

Uses matplotlib.pyplot to generate and display the charts.

Includes basic validation for column existence and data types suitable for the chosen chart.

print_help(): Displays the available commands in the CLI.

main_cli(conn, table_name, df):

The main command-line interface loop.

Continuously prompts the user for commands.

Recognizes commands:

SQL <your_sql_query>: To execute any SQL query.

VISUALIZE: To enter the visualization sub-menu.

COLUMNS: To list columns and their current inferred types from the database.

DESCRIBE: Shows summary statistics for the table (like df.describe()).

HEAD [N]: Shows the first N rows (default 5).

HELP: To display the help message.

EXIT: To quit the application.

3. Main Execution (if __name__ == "__main__":)

Prompts the user for the CSV file path.

Calls create_table_from_csv to set up the database and load data.

If successful, it enters the main_cli loop.

Ensures the database connection is closed when the script exits (using a finally block).

How to Use:

Save the script: Save the code above as a Python file (e.g., csv_analyzer.py).

Install libraries: If you don't have them, open your terminal or command prompt and run:

Bash

pip install pandas matplotlib
Run the script:

Bash

python csv_analyzer.py
Enter CSV Path: The script will first ask you for the path to your CSV file.

Enter the path to your CSV file: /path/to/your/data.csv
Table Creation: It will then display the name of the table created in the in-memory database and the inferred column types.

Command-Line Interface: You'll see a prompt like:

Enter command for table 'your_data_table':
You can then use the commands:

SQL Queries:

Enter command for table 'your_data_table': SQL SELECT column1, column2 FROM your_data_table WHERE column1 > 10 LIMIT 5
Enter command for table 'your_data_table': SQL UPDATE your_data_table SET column2 = 'new_value' WHERE column1 = 5
Visualization:

Enter command for table 'your_data_table': VISUALIZE
This will take you to a sub-menu where you can choose the chart type and specify columns. Matplotlib will open a new window to display the chart.

Other commands: COLUMNS, DESCRIBE, HEAD, HELP, EXIT.

This script should provide a good starting point for your CSV analysis and data manipulation needs!
