# Python script for CSV analysis, SQL querying, and visualization.
# Dependencies: pandas, matplotlib
# Install them using: pip install pandas matplotlib

import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import os
import re

def sanitize_column_name(col_name):
    """Sanitizes a column name to be SQL-friendly."""
    # Remove leading/trailing whitespace
    name = col_name.strip()
    # Replace non-alphanumeric characters (except underscores) with underscores
    name = re.sub(r'[^\w_]', '_', name)
    # Ensure it doesn't start with a number (SQLite requirement for unquoted identifiers)
    if name and name[0].isdigit():
        name = "_" + name
    return name if name else "unnamed_column"

def infer_sqlite_type(series):
    """Infers SQLite data type from a pandas Series."""
    dtype = series.dtype
    # Check for actual NAs, not just string 'NA'
    if series.isnull().all(): # If all values are NaN/None
        return "TEXT" # Default to TEXT if no data to infer from
    
    # Attempt to convert to numeric if object type, to handle numbers read as strings
    if dtype == 'object':
        try:
            pd.to_numeric(series.dropna()) # Try converting non-NA values
            # If successful, check if they are integers or floats
            if (series.dropna().astype(float) % 1 == 0).all(): # Check if all numbers are whole
                 # Further check if original strings had decimal points like "1.0"
                if series.dropna().apply(lambda x: isinstance(x, str) and '.' in x).any():
                    return "REAL" # Treat "1.0" as REAL
                return "INTEGER"
            else:
                return "REAL"
        except (ValueError, TypeError):
            # Could not convert to numeric, so it's likely text
            pass # Fall through to TEXT

    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "REAL"
    elif pd.api.types.is_bool_dtype(dtype):
        return "INTEGER"  # SQLite uses 0 and 1 for booleans
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TEXT"  # Store datetimes as ISO format strings
    else:
        return "TEXT"   # Default for objects, strings, etc.

def create_table_from_csv(csv_filepath):
    """
    Reads a CSV, infers column types, creates an SQLite in-memory table,
    and loads data into it.
    Returns the database connection, table name, and pandas DataFrame.
    """
    try:
        df = pd.read_csv(csv_filepath, keep_default_na=True, na_filter=True)
    except FileNotFoundError:
        print(f"Error: CSV file not found at '{csv_filepath}'")
        return None, None, None
    except pd.errors.EmptyDataError:
        print(f"Error: CSV file '{csv_filepath}' is empty.")
        return None, None, None
    except Exception as e:
        print(f"Error reading CSV file '{csv_filepath}': {e}")
        return None, None, None

    if df.empty:
        print(f"Warning: CSV file '{csv_filepath}' was read but resulted in an empty DataFrame.")
        # We can still proceed to create an empty table if columns are defined
        if not df.columns.tolist():
            print("Error: CSV file has no columns.")
            return None, None, None

    # Sanitize column names for SQL compatibility
    original_columns = df.columns.tolist()
    sanitized_columns = [sanitize_column_name(col) for col in original_columns]
    df.columns = sanitized_columns
    
    # Create a unique table name from the CSV filename
    base_filename = os.path.splitext(os.path.basename(csv_filepath))[0]
    table_name = sanitize_column_name(base_filename) + "_table"
    if not table_name.strip("_"): # Handle cases where filename was only special chars
        table_name = "csv_data_table"


    # Create an in-memory SQLite database
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()

    # Infer column types and create table schema
    create_table_sql_parts = []
    column_definitions = []
    for col_name in df.columns:
        # Re-infer type AFTER potential conversions if column was all NA initially
        # or if it was object but convertible to numeric
        series_for_type_inference = df[col_name]
        sqlite_type = infer_sqlite_type(series_for_type_inference)
        column_definitions.append(f'"{col_name}" {sqlite_type}') # Quote column names

    if not column_definitions:
        print("Error: No columns defined for the table.")
        conn.close()
        return None, None, None
        
    create_table_sql = f"CREATE TABLE IF NOT EXISTS \"{table_name}\" ({', '.join(column_definitions)})"
    
    try:
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"\nSuccessfully created SQLite table named: '{table_name}'")
        print("Inferred column types:")
        for col_name in df.columns:
            print(f"  - '{col_name}': {infer_sqlite_type(df[col_name])}")

    except sqlite3.Error as e:
        print(f"SQLite error creating table '{table_name}': {e}")
        conn.close()
        return None, None, None

    # Load data into the table
    # Pandas to_sql handles type conversion fairly well, but our explicit types are good.
    # It will also handle NaNs by converting them to NULL in SQL.
    try:
        # df.to_sql will create the table if it doesn't exist, but we created it with specific types.
        # We'll use if_exists='append' and ensure it's empty first if we want to rely on our schema.
        # However, for simplicity and robustness with pandas' type handling:
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"Data from '{csv_filepath}' loaded into table '{table_name}'.")
    except Exception as e:
        print(f"Error loading data into table '{table_name}': {e}")
        # Table was created, but data loading failed. Could still proceed with an empty table.
        # For now, let's consider this a critical failure for data interaction.
        conn.close()
        return None, None, None
        
    return conn, table_name, df

def execute_sql_query(conn, query):
    """Executes an SQL query and prints the results."""
    try:
        # For SELECT queries, pandas can read them into a DataFrame for nice printing
        if query.strip().lower().startswith("select"):
            result_df = pd.read_sql_query(query, conn)
            if result_df.empty:
                print("Query executed successfully, no results to display.")
            else:
                print("Query Results:")
                print(result_df.to_string()) # .to_string() for better console display
        else:
            # For other queries (INSERT, UPDATE, DELETE, CREATE, etc.)
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            print(f"Query executed successfully. {cursor.rowcount} rows affected.")
    except sqlite3.Error as e:
        print(f"SQLite Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during SQL execution: {e}")

def visualize_data(conn, table_name, df_original):
    """Handles the visualization submenu."""
    if df_original is None or df_original.empty:
        print("No data available for visualization (original DataFrame is empty or None).")
        return

    # Fetch current data from the table in case SQL commands modified it
    try:
        df = pd.read_sql_query(f"SELECT * FROM \"{table_name}\"", conn)
    except Exception as e:
        print(f"Could not fetch data from table '{table_name}' for visualization: {e}")
        print("Using the initially loaded DataFrame instead (may not reflect SQL changes).")
        df = df_original # Fallback to the original df

    if df.empty:
        print(f"Table '{table_name}' is currently empty. Cannot visualize.")
        return

    print("\n--- Visualization Menu ---")
    print("Available columns for visualization:", ", ".join(df.columns))
    
    while True:
        print("\nChoose chart type:")
        print("  BAR       - Bar chart (1 categorical X, 1 numerical Y)")
        print("  HISTOGRAM - Histogram (1 numerical column)")
        print("  SCATTER   - Scatter plot (2 numerical columns)")
        print("  PIE       - Pie chart (1 categorical column for labels, 1 numerical for values)")
        print("  LINE      - Line plot (1 X-axis, 1 Y-axis, typically numerical or time-series)")
        print("  BACK      - Return to main menu")
        
        choice = input("Enter chart type: ").strip().upper()

        if choice == "BACK":
            break
        elif choice not in ["BAR", "HISTOGRAM", "SCATTER", "PIE", "LINE"]:
            print("Invalid chart type. Please choose from the list.")
            continue

        try:
            if choice == "BAR":
                x_col = input(f"Enter X-axis column name (categorical) from {df.columns.tolist()}: ").strip()
                y_col = input(f"Enter Y-axis column name (numerical) from {df.columns.tolist()}: ").strip()
                if x_col not in df.columns or y_col not in df.columns:
                    print("Invalid column name(s).")
                    continue
                if not pd.api.types.is_numeric_dtype(df[y_col]):
                    print(f"Y-axis column '{y_col}' must be numerical for a bar chart.")
                    continue
                
                # For bar charts, often we want to aggregate if x_col has duplicates
                # Example: Sum of y_col for each category in x_col
                # Or, if x_col is unique, it's a direct plot.
                # For simplicity, let's assume direct plot or user prepares data via SQL
                # A common use case is to plot counts or sums.
                # We'll plot the values as they are. If user wants aggregated bars, they should use SQL.
                
                # If X column is numeric, it might be better as a histogram or line.
                # If X column has too many unique values, bar chart might be cluttered.
                if df[x_col].nunique() > 30:
                    print(f"Warning: X-axis column '{x_col}' has many unique values ({df[x_col].nunique()}). Bar chart might be cluttered.")

                plt.figure(figsize=(10, 6))
                plt.bar(df[x_col].astype(str), df[y_col]) # Convert x_col to string for categorical display
                plt.xlabel(x_col)
                plt.ylabel(y_col)
                plt.title(f"Bar Chart: {y_col} by {x_col}")
                plt.xticks(rotation=45, ha="right")
                plt.tight_layout()
                plt.show()

            elif choice == "HISTOGRAM":
                col_name = input(f"Enter column name for histogram (numerical) from {df.columns.tolist()}: ").strip()
                if col_name not in df.columns:
                    print("Invalid column name.")
                    continue
                if not pd.api.types.is_numeric_dtype(df[col_name]):
                    print(f"Column '{col_name}' must be numerical for a histogram.")
                    continue
                bins = input("Enter number of bins (default 10, press Enter for default): ").strip()
                bins = int(bins) if bins.isdigit() else 10
                
                plt.figure(figsize=(10, 6))
                plt.hist(df[col_name].dropna(), bins=bins, edgecolor='black') # dropna for robustness
                plt.xlabel(col_name)
                plt.ylabel("Frequency")
                plt.title(f"Histogram of {col_name}")
                plt.tight_layout()
                plt.show()

            elif choice == "SCATTER":
                x_col = input(f"Enter X-axis column name (numerical) from {df.columns.tolist()}: ").strip()
                y_col = input(f"Enter Y-axis column name (numerical) from {df.columns.tolist()}: ").strip()
                if x_col not in df.columns or y_col not in df.columns:
                    print("Invalid column name(s).")
                    continue
                if not pd.api.types.is_numeric_dtype(df[x_col]) or \
                   not pd.api.types.is_numeric_dtype(df[y_col]):
                    print(f"Both columns ('{x_col}', '{y_col}') must be numerical for a scatter plot.")
                    continue
                
                plt.figure(figsize=(10, 6))
                plt.scatter(df[x_col], df[y_col])
                plt.xlabel(x_col)
                plt.ylabel(y_col)
                plt.title(f"Scatter Plot: {y_col} vs {x_col}")
                plt.grid(True)
                plt.tight_layout()
                plt.show()
            
            elif choice == "PIE":
                labels_col = input(f"Enter column for pie chart labels (categorical) from {df.columns.tolist()}: ").strip()
                values_col = input(f"Enter column for pie chart values (numerical) from {df.columns.tolist()}: ").strip()

                if labels_col not in df.columns or values_col not in df.columns:
                    print("Invalid column name(s).")
                    continue
                if not pd.api.types.is_numeric_dtype(df[values_col]):
                    print(f"Values column '{values_col}' must be numerical for a pie chart.")
                    continue
                if df[values_col].min() < 0:
                    print(f"Warning: Values column '{values_col}' contains negative numbers. Pie chart may not be appropriate or may error.")
                
                # Pie charts are best with a few categories. Aggregate if necessary.
                # Here, we'll assume the data is already aggregated or suitable.
                # If labels_col has many unique values, pie chart will be unreadable.
                # Often, one might group by labels_col and sum values_col.
                # For this implementation, we'll plot directly. User can use SQL to aggregate.
                
                # Handling case where labels_col might be numeric but treated as categories
                # df[labels_col].astype(str)
                
                # Sum values for the same label to avoid issues with matplotlib pie
                data_for_pie = df.groupby(labels_col)[values_col].sum()

                if data_for_pie.nunique() > 15:
                     print(f"Warning: Label column '{labels_col}' has many unique categories ({data_for_pie.nunique()}). Pie chart might be cluttered.")


                plt.figure(figsize=(10, 8))
                plt.pie(data_for_pie, labels=data_for_pie.index, autopct='%1.1f%%', startangle=90)
                plt.title(f"Pie Chart: {values_col} by {labels_col}")
                plt.axis('equal') # Equal aspect ratio ensures that pie is drawn as a circle.
                plt.tight_layout()
                plt.show()

            elif choice == "LINE":
                x_col = input(f"Enter X-axis column name from {df.columns.tolist()}: ").strip()
                y_col = input(f"Enter Y-axis column name (numerical) from {df.columns.tolist()}: ").strip()
                if x_col not in df.columns or y_col not in df.columns:
                    print("Invalid column name(s).")
                    continue
                if not pd.api.types.is_numeric_dtype(df[y_col]):
                    print(f"Y-axis column '{y_col}' must be numerical for a line plot.")
                    continue
                
                # If x_col is datetime, pandas plot handles it well.
                # If x_col is categorical, ensure it's sorted appropriately or convert.
                # For simplicity, we assume user knows if their X-axis is suitable.
                # Sorting by X-axis is crucial for a meaningful line plot.
                df_sorted = df.sort_values(by=x_col)

                plt.figure(figsize=(10, 6))
                plt.plot(df_sorted[x_col], df_sorted[y_col])
                plt.xlabel(x_col)
                plt.ylabel(y_col)
                plt.title(f"Line Plot: {y_col} over {x_col}")
                plt.xticks(rotation=45, ha="right")
                plt.grid(True)
                plt.tight_layout()
                plt.show()

        except Exception as e:
            print(f"Error during visualization: {e}")
            import traceback
            traceback.print_exc()


def print_help():
    print("\n--- Available Commands ---")
    print("SQL <your_sql_query>   - Execute an SQL query against the table.")
    print("                           Example: SQL SELECT * FROM your_table_name LIMIT 5")
    print("VISUALIZE              - Enter the data visualization menu.")
    print("COLUMNS                - List columns and their inferred types in the current table.")
    print("DESCRIBE               - Show summary statistics for numerical columns (like pandas describe).")
    print("HEAD [N]               - Show the first N rows of the table (default 5). Example: HEAD 10")
    print("HELP                   - Show this help message.")
    print("EXIT                   - Exit the application.")
    print("------------------------\n")

def main_cli(conn, table_name, df):
    """Main command-line interface loop."""
    print_help()
    while True:
        try:
            user_input = input(f"Enter command for table '{table_name}': ").strip()
            if not user_input:
                continue

            command_parts = user_input.split(maxsplit=1)
            command = command_parts[0].upper()

            if command == "EXIT":
                print("Exiting application.")
                break
            elif command == "SQL":
                if len(command_parts) > 1:
                    sql_query = command_parts[1]
                    execute_sql_query(conn, sql_query)
                else:
                    print("Please provide an SQL query after 'SQL'. Example: SQL SELECT * FROM ...")
            elif command == "VISUALIZE":
                visualize_data(conn, table_name, df)
            elif command == "COLUMNS":
                try:
                    current_df_from_db = pd.read_sql_query(f"SELECT * FROM \"{table_name}\" LIMIT 1", conn) # Fetch one row to get columns and types
                    print(f"\nColumns in table '{table_name}':")
                    for col in current_df_from_db.columns:
                         # Re-infer type from the current state of the table in DB
                        series_from_db = pd.read_sql_query(f'SELECT "{col}" FROM "{table_name}"', conn)[col]
                        print(f"  - '{col}': {infer_sqlite_type(series_from_db)}")
                except Exception as e:
                    print(f"Error fetching column information: {e}")
            elif command == "DESCRIBE":
                try:
                    current_df_from_db = pd.read_sql_query(f"SELECT * FROM \"{table_name}\"", conn)
                    if current_df_from_db.empty:
                        print("Table is empty, cannot describe.")
                    else:
                        print(f"\nSummary statistics for table '{table_name}':")
                        print(current_df_from_db.describe(include='all').to_string())
                except Exception as e:
                    print(f"Error generating description: {e}")
            elif command == "HEAD":
                num_rows = 5
                if len(command_parts) > 1 and command_parts[1].isdigit():
                    num_rows = int(command_parts[1])
                execute_sql_query(conn, f'SELECT * FROM "{table_name}" LIMIT {num_rows}')
            elif command == "HELP":
                print_help()
            else:
                print("Invalid command. Type 'HELP' for a list of commands.")
        except KeyboardInterrupt:
            print("\nExiting application (Ctrl+C detected).")
            break
        except Exception as e:
            print(f"An unexpected error occurred in the CLI: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    csv_file = input("Enter the path to your CSV file: ").strip()
    
    conn, table_name, df = create_table_from_csv(csv_file)

    if conn and table_name:
        try:
            main_cli(conn, table_name, df)
        finally:
            print(f"Closing database connection for '{table_name}'.")
            conn.close()
    else:
        print("Could not initialize database from CSV. Exiting.")

