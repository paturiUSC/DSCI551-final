import os 
import pandas as pd
import numpy as np
import re


# to - do 
    # select 
    # update 
    # insert/delete
        # remove/insert into whole dataset or partitioned tables 
    # insert 
        # only insert into partition if it's number of rows is less than the memory size
            # if it is more than the memory size, create a new partition based on the partition 
    

def displayUserMenu(): 
    print("What would you like to do? Please enter the corresponding number of the operation you would like to perform.")
    print("A. Select Data (including joins)")
    print("B. Insert Data")
    print("C. Update Data")
    print("D. Delete Data")

    user_input = input(">")
    user_input = user_input.lower()

    while (user_input != "a" and user_input != "b" and user_input != "c" and user_input !="d") or (not user_input): 
        user_input = input("Please enter a valid input of 'A', 'B', 'C', or 'D': ")
        user_input = user_input.lower()

    return user_input

def useDatabase():
    print("\nWould you like to perform an operation on the database?")
    user_input = input("Please enter 'yes' or 'no': ")

    user_input = user_input.lower()

    while (not user_input) or (user_input != "yes" and user_input != "no"):
        user_input = input("Please enter 'yes' or 'no': ")
        user_input = user_input.lower()

    return user_input == "yes"

def selectData(): 
    return ""

def updateData(): 
    return ""

def insertData(): 
    # Display the example prompts for the user
    print("\nHere are 2 example prompts to insert data into the database. Use a semicolon ';' to end the command.")
    print("Option 1 (inserting a table): Insert table table_name (a int, b str, c float);")
    print("Option 2 (inserting ONE row into a table): Insert row in table_name (col1, col2, col3) VALUES (val1, val2, val3);")
    print("For Option 2, make sure to have the correct datatype for each column value. A null value will be inserted for missing fields. Make sure 'row' is in the command ONLY for inserting a row and 'table' is in the command ONLY for inserting a new table.")
    
    print("\nExit out at anytime by typing 'exit;'")

    # Get and validate user input
    user_insert_input = input(">")

    while (not user_insert_input) or (user_insert_input[-1:] != ";"): 
        user_insert_input = input(">")
    
    if ("exit" in user_insert_input):
        return ""
    
    user_insert_input = user_insert_input.lower()

    if not os.path.exists("./DSCI551-final/Output-Data"):
            os.makedirs("./DSCI551-final/Output-Data")

    # Create a new table
    if "table " in user_insert_input: 
        pattern = r"Insert\s+table\s+(\w+)\s*\((.*?)\)"

        # Match the pattern to the user inpitted command
        match = re.match(pattern, user_insert_input, re.IGNORECASE)

        if match:
            # Find the name of the newly created table
            table_name = match.group(1)  

            # Split the column headers
            column_info = {}
            parameters_str = match.group(2)  
            parameters = [param.strip() for param in parameters_str.split(',')]
            for parameter in parameters: 
                column_info_list = parameter.split()
                column_info[column_info_list[0]] = column_info_list[1]

            # Show output to user 
            print("Here is the extracted table name", table_name)
            print("Here is the extracted table columns", column_info)

            # Create empty DataFrame with specified column names and data types
            new_table_df = pd.DataFrame(columns=column_info.keys())

            # Set data types for each user inputted column
            for column, dtype in column_info.items():
                new_table_df[column] = new_table_df[column].astype(dtype)
            
            # Export the DataFrame as a .csv to store the data
            new_table_file_name = './DSCI551-final/Output-Data/' + table_name + '.csv'
            new_table_df.to_csv(new_table_file_name, index=False)

            # Show output to user
            print("The table", table_name, "has been created and inserted into the database.")
            print("Here is the file path of the newly created table", new_table_file_name)

        else: 
            print("The table name and/or parameters could not be identified.")
            print("Please enter a valid insert statement similar to the provided examples.")

    # Insert a row of data 
    elif "row" in user_insert_input: 

        user_insert_input = user_insert_input.lower();
        pattern = r"insert\s+row\s+in\s+(\w+)\s+\((.*?)\)\s*values\s*\((.*?)\);"

        # Match the pattern to the user inputted command
        match = re.match(pattern, user_insert_input, re.IGNORECASE)

        if match:
            # Get the table name
            table_name = match.group(1)  
            # Get the column names and values
            column_names_str = match.group(2)  
            column_values_str = match.group(3)  

            # Split the column names and values strings into individual lists
            column_names = [col.strip() for col in column_names_str.split(',')]
            print(column_names)
            column_values = [val.strip() for val in column_values_str.split(',')]
            print(column_values)

            # Check to ensure that for each column name, there is a valid column value and vice-versa
            if (len(column_values) == 1 and len(column_names) == 1): 
                if len(column_values[0]) == 0: 
                    print("Please enter a valid value.")
                    return ""

            if (len(column_names) != len(column_values)):
                print("Please ensure every column name and value correspond to each other.")
                print("There were", str(len(column_names)), "column names entered and ", str(len(column_values)), "column values entered.")
                return ""

            # Check if the table exists 
            tables_path = "./DSCI551-final/Output-Data/"
            table_path = tables_path + table_name + ".csv"
            file_list = os.listdir(tables_path)

            if os.path.exists(table_path):
                file_df = pd.read_csv(table_path)
                data_types_dict = {col: dtype for col, dtype in file_df.dtypes.items()}
                print("Here is the info of the table.")
                print(file_df.info())
                table_columns = file_df.columns
                new_data = {}
                valid_columns = True

                # Iterate to identify if the columns entered by the user exist in the table. If not, do not modify the table and show the available table column names to the user.
                for list_index in range(len(column_names)):
                    add_data = True
                    column_name = column_names[list_index]
                    column_name = column_name.strip().lower()
                    if column_name in table_columns:
                        # Check if data types match of the column values 
                        value = column_values[list_index]

                        # Check if int or float or object
                        # Attempt to convert to an integer
                        try:
                            value = int(value)
                            print(f"Converted to integer: {value}")
                        except ValueError:
                            # If it's not a valid integer, try converting to a float
                            try:
                                value = float(value)
                                print(f"Converted to float: {value}")
                            except ValueError:
                                # If it can't be converted to either int or float, it's invalid
                                print("Invalid input. Please enter a valid number.")

                        print("The column dtype: ", file_df[column_name].dtype)
                        print("The value dtype: ", type(value))
                        if (file_df[column_name].dtype == type(value)): 
                            add_data = True
                        else: 
                            if file_df[column_name].dtype == float:
                                # int can be converted to float
                                if isinstance(value, int):
                                    add_data = True
                                else: 
                                    print("Not ok")
                                    print("Don't add.")
                                    add_data = False
                            else: 
                                add_data = False

                        # If column name exists and column value of the correct data type, then add that data
                        if add_data: 
                            new_data[column_names[list_index]] = column_values[list_index]
                        else: 
                            print("\nThe data could not be added due to mismatching data types/column names.")
                            print("Here are the column names and corresponding data types: ", data_types_dict)
                            return ""
                    else: 
                        print("The column", column_name, "does not exist in the database.")
                        valid_columns = False
                
                # Ensure all of the table's columns are accounted for. If not specified by the user, ensure these columns are filled in with null values.
                if not valid_columns: 
                    print("Here are the list of valid columns for table", table_name, ": ", table_columns)
                    return ""
                else: 
                    print("\nIf any, the remaining columns that the user did not specify will recieve null values. Those columns, if any, will be displayed through this console next. If there are none, then no output will be shown about columns recieving null values.")
                    for table_column in table_columns: 
                        if table_column not in new_data.keys(): 
                            new_data[table_column] = np.nan
                            print("Added null value for column", table_column)
                    
                    # Add the new data to the existing table
                    new_data_df = pd.DataFrame([new_data])
                    
                    placeholder_value = -99999
                    data_types_dict = {col: dtype for col, dtype in file_df.dtypes.items()}
                    for col in new_data_df.columns:
                        if col in data_types_dict.keys(): 
                            new_data_df[col] = new_data_df[col].fillna(placeholder_value).astype(data_types_dict[col]).where(new_data_df[col].notna(), np.nan)
                        
                    print("Here is the info of the data being added:")
                    print(new_data_df.info())

                    appended_file_df = pd.concat([file_df, new_data_df], ignore_index=True)

                    data_types_dict = {col: dtype for col, dtype in file_df.dtypes.items()}
                    for col in appended_file_df.columns:
                        if col in data_types_dict.keys(): 
                            appended_file_df[col] = appended_file_df[col].fillna(placeholder_value).astype(data_types_dict[col]).where(appended_file_df[col].notna(), np.nan)
                    
                    print("\n\nHere is the data being inserted into the table", new_data)

                    try:
                        # Attempt to write the modified DataFrame to the same CSV file/table
                        appended_file_df.to_csv(table_path, index=False)
                        print("DataFrame successfully written to CSV.")
                        print("Confirm with the table info.")
                        print(appended_file_df.info())
                    except Exception as e:
                        # Catch any exceptions
                        print("\nError inserting data. Try again!")
                        print(f"\nAn error occurred: {e}")

                    print("\nThe location of the table with the inserted data is", table_path)

                    print("\nSuccess!")
            else: 
                print("This is not a valid table in the database. Please enter a valid table name!")
                print("\nHere's the list of available tables:", file_list, "\n")
        else:
            print("The table name and/or parameters could not be identified.")
            print("Make sure there is a space after the table name and after 'values' (space before each set of paranthesis).")
            print("Please enter a valid insert statement similar to the provided examples.")

        return ""

    else: 
        print("Please enter a valid insert statement next time.")

def deleteData(): 
    # Display the example prompts for the user
    print("\nHere are 3 example prompts to delete data from the database. Use a semicolon ';' to end the command.")
    print("Option 1 (deleting all data from user-generated and pre-partitioned tables): Delete all;")
    print("Option 2 (deleting specific rows based on a comparison/rule AND make sure column name is right before the comparison): Delete rows where stock_price > than 50;")
    print("For Option 2, make sure to type out the comparison explicity: '=', '>', '>=', '<',  '<=', or 'like' (for strings).")
    print("Option 3 (deleting user-generated table): Delete all from table_name;\n")

    print("\nExit out at anytime by typing 'exit;'")

    # Get and validate user input
    user_delete_input = input(">")

    while (not user_delete_input) or (user_delete_input[-1:] != ";"): 
        user_delete_input = input(">")
    
    if ("exit" in user_delete_input):
        return ""
    
    user_delete_input = user_delete_input.lower()

    if not os.path.exists("./DSCI551-final/Output-Data"):
            os.makedirs("./DSCI551-final/Output-Data")

    directory_data_files = os.listdir("./DSCI551-final/Output-Data")

    if len(directory_data_files) == 0:
        print("No data in the database right now.")
        return ""

    # Delete all data
    if "all" in user_delete_input and "from" not in user_delete_input: 
        for file in directory_data_files: 
            file_path = "./DSCI551-final/Output-Data/" + file
            print("Deleted file", file_path)
            os.remove(file_path)
        print("Deleted all data!")
    
    # Delete all data from a specific table
    elif "all" in user_delete_input:
        table_name = input(">")
        while not os.path.exists("./DSCI551-final/Output-Data/" + table_name + ".csv"): 
            table_name = input("The entered table does not exist. Please enter a valid table name: ")
        file_path = "./DSCI551-final/Output-Data/" + table_name + ".csv"

        os.remove(file_path)

        print("Deleted all data from table", table_name)

    # Delete specific rows of data
    elif "=" in user_delete_input or ">" in user_delete_input or ">=" in user_delete_input or "<" in user_delete_input or "<=" in user_delete_input or "like" in user_delete_input:

        # Identify the relevant columns and corresponding comparison phrases 
        # Define the regular expression pattern to match the specified phrases
        pattern = r'(\b\w+\b)\s*(=|<|>|<=|>=|like)\s*(\b\w+\b)'

        # Find all matches in the input text
        matches = re.findall(pattern, user_delete_input, re.IGNORECASE)

        # Apply delete input on each file/table in the database
        for file in directory_data_files: 
            # Extract and print the matched phrases, along with words/values before and after
            df_filter_expression = None

            file_path = "./DSCI551-final/Output-Data/" + file

            file_df = pd.read_csv(file_path)
            original_length = len(file_df)
            column_headers = file_df.columns

            for match in matches:
                column = match[0]
                comparison_phrase = match[1]
                value = match[2]
                
                print(f"These are the identified key tokens from the command:'{column}' {comparison_phrase} '{value}'")

                # Construct the DataFrame filter expressions
                if column in column_headers:
                    # Construct a filter expression
                    if comparison_phrase == '=':
                        value = int(value)
                        condition = (file_df[column] == value)
                    elif comparison_phrase == '<':
                        value = int(value)
                        condition = (file_df[column] < value)
                    elif comparison_phrase == '>':
                        value = int(value)
                        condition = (file_df[column] > value)
                    elif comparison_phrase == '<=':
                        value = int(value)
                        condition = (file_df[column] <= value)
                    elif comparison_phrase == '>=':
                        value = int(value)
                        condition = (file_df[column] >= value)
                    elif comparison_phrase == 'like':
                        condition = file_df[column].str.contains(value, case=False)
                    
                    # Combine the filter expression conditions using logical 'and'
                    if df_filter_expression is None:
                        df_filter_expression = condition
                    else:
                        df_filter_expression = df_filter_expression & condition
                else: 
                    print("The column", column, "is not a valid column. So, not going to delete rows based on that condition.")

            # Apply the identified filter expression
            # Show output to user
            if df_filter_expression is not None:
                print("This is the identified filter expression.")
                print(df_filter_expression)

                print("Dropping...")
                filtered_file_df = file_df[~df_filter_expression]
                dropped_rows = original_length - len(filtered_file_df)
                
                print("These are how many rows got dropped/deleted:", str(dropped_rows))
                filtered_file_df.to_csv(file_path, index=False)
            else:
                print("No valid delete command found in your input.")

        print("\nDeleted specific rows.")

    else: 
        print("Please enter a valid delete statement next time.")

    print("\nDelete data operation complete!")

def menu_option_action(menu_option):
    if menu_option == "a": 
        print("You've chosen to select data.")
        selectData()
    elif menu_option == "b":
        print("You've chosen to insert data.")
        insertData()

    elif menu_option == "c":
        print("You've chosen to update data.")
        updateData()

        # update all 
        # update specific data
        '''
        UPDATE table_name
        SET column1 = value1, column2 = value2, ...
        WHERE condition;
        '''

    elif menu_option == "d":
        print("You've chosen to delete data.")
        deleteData()

    else: 
        print("Invalid menu option!")

if __name__ == "__main__":
    # column_headers = ['date', 'open', 'high', 'low', 'close', 'volume', 'Name'] 

    # See if the user wants to use the database
    use_database_input = useDatabase()

    # Get user input on what operation the user wants to perform on the database
    while use_database_input:
        menu_option = displayUserMenu()

        menu_option_action(menu_option)

        use_database_input = useDatabase()

    print("\nThank you for using the database!")



'''
an example query prompt will be displayed to the user. For instance, if the user chooses to insert data through the command line interface, the function insertData() will be called, and an example prompt such as “Insert the data stored in location extra_data.csv" will be displayed. Then, the user can input a similar prompt to insert additional data. This will be accomplished through the same methods used to process and partition the original input data. 
'''

'''
Include an option to exit at anytime by typing 'exit' within the select/insert/update/delete data functions
'''