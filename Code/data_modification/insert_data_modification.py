import os 
import pandas as pd
import numpy as np
import re
import psutil

# Function allowing the user to insert data into the table(s) in the database system 
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
    
    # user_insert_input = user_insert_input.lower()

    if not os.path.exists("./Output-Data"):
        os.makedirs("./Output-Data")

    # Create a new table
    if " table " in user_insert_input.lower(): 
        pattern = r"Insert\s+table\s+(\w+)\s*\((.*?)\)"

        # Match the pattern to the user inputted command
        match = re.match(pattern, user_insert_input, re.IGNORECASE)

        if match:
            # Find the name of the newly created table
            table_name = match.group(1)  
            table_name = table_name.lower()

            # Split the column headers
            column_info = {}
            parameters_str = match.group(2)  
            parameters = [param.strip() for param in parameters_str.split(',')]
            for parameter in parameters: 
                column_info_list = parameter.split()
                column_info[column_info_list[0]] = column_info_list[1]

            # Show output to user 
            print("\nHere is the extracted table name", table_name)
            print("Here is the extracted table columns", column_info)

            # Create empty DataFrame with specified column names and data types
            new_table_df = pd.DataFrame(columns=column_info.keys())

            # Set data types for each user inputted column
            for column, dtype in column_info.items():
                new_table_df[column] = new_table_df[column].astype(dtype)

            if not os.path.exists("./Output-Data/" + table_name):
                os.makedirs("./Output-Data/" + table_name)
            
            # Export the DataFrame as a .csv to store the data
            new_table_file_name = './Output-Data/' + table_name + '/chunk0.csv'
            new_table_df.to_csv(new_table_file_name, index=False)

            # Show output to user
            print("\nThe table", table_name, "has been created and inserted into the database.")
            print("Here is the file path of the newly created table", new_table_file_name)

        else: 
            print("The table name and/or parameters could not be identified.")
            print("Please enter a valid insert statement similar to the provided examples.")

    # Insert a row of data 
    elif "row" in user_insert_input: 

        raw_user_insert_input = user_insert_input
        user_insert_input = user_insert_input.lower()
        match = True

        if match:
            # Get the table name
            # Get the column names and values
            user_insert_input_split = user_insert_input.lower().split()
            in_index = user_insert_input_split.index("in")
            table_name = user_insert_input_split[in_index + 1]

            # Find the position of the first "(" and ")"
            first_open_parenthesis_index = user_insert_input.find("(")
            first_close_parenthesis_index = user_insert_input.find(")")

            # Find the position of the second "(" and ")"
            second_open_parenthesis_index = user_insert_input.find("(", first_open_parenthesis_index + 1)
            second_close_parenthesis_index = user_insert_input.find(")", first_close_parenthesis_index + 1)

            column_names_str = user_insert_input[first_open_parenthesis_index + 1 : first_close_parenthesis_index]
            column_values_str = raw_user_insert_input[second_open_parenthesis_index + 1 : second_close_parenthesis_index]

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
            tables_path = "./Output-Data/"
            table_path = tables_path + table_name 
            file_list = os.listdir(tables_path)

            # Put together the chunks of each table
            if (os.path.exists(table_path)):
                table_chunks = os.listdir(table_path)
                file_df = pd.read_csv(table_path + "/chunk0.csv")

                for chunk in table_chunks: 
                    if chunk != "chunk0.csv":
                        chunk_df = pd.read_csv(table_path + "/" + chunk)
                        file_df = pd.concat([file_df, chunk_df], ignore_index=True)

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
                            if (value.isdecimal()):
                                value = int(value)
                            else: 
                                value = float(value)
                            print(f"Converted to number: {value}")
                        except:
                            # If it's not a valid integer, try converting to a float
                            print(f"Not converted to number. Kept value as string: {value}")

                        print("The column dtype: ", file_df[column_name].dtype)
                        print("The value dtype: ", type(value))
                        if (file_df[column_name].dtype == type(value)): 
                            add_data = True
                        else: 
                            if file_df[column_name].dtype == "float64":
                                # int can be converted to float
                                if isinstance(value, int):
                                    add_data = True
                                else: 
                                    print("\nNot ok")
                                    print("Don't add.")
                                    add_data = False
                                    print("Datatype mismatch for the column", column_name)
                            else: 
                                if (str(file_df[column_name].dtype) == "object"): 
                                    add_data = True
                                else:  
                                    # If the column type is int, convert float values to int
                                    if (file_df[column_name] == "int64"):
                                        if (type(value) == float):
                                            value = int(value)
                                            add_data = True
                                        elif (type(value) == int):
                                            add_data=True    
                                        else: 
                                            add_data = False
                                            print("\nDatatype mismatch for the column", column_name)
                                    else: 
                                        add_data = False
                                        print("\nDatatype mismatch for the column", column_name)

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
                            # Add null value for columns for which data is not inputted by the user
                            new_data[table_column] = np.nan
                            print("Added null value for column", table_column)
                    
                    # Add the new data to the existing table
                    new_data_df = pd.DataFrame([new_data])
                    # Temporary placeholder value
                    placeholder_value = -99999
                    data_types_dict = {col: dtype for col, dtype in file_df.dtypes.items()}
                    for col in new_data_df.columns:
                        if col in data_types_dict.keys(): 
                            new_data_df[col] = new_data_df[col].fillna(placeholder_value).astype(data_types_dict[col]).where(new_data_df[col].notna(), np.nan)
                        
                    # Display info of the data to be inserted to the user
                    print("Here is the info of the data being added:")
                    print(new_data_df.info())

                    # Insert the data 
                    appended_file_df = pd.concat([file_df, new_data_df], ignore_index=True)

                    data_types_dict = {col: dtype for col, dtype in file_df.dtypes.items()}
                    for col in appended_file_df.columns:
                        if col in data_types_dict.keys(): 
                            appended_file_df[col] = appended_file_df[col].fillna(placeholder_value).astype(data_types_dict[col]).where(appended_file_df[col].notna(), np.nan)
                    
                    print("\n\nHere is the data being inserted into the table", new_data)

                    try:
                        # Attempt to write the modified DataFrame back to storage and resize the chunks 
                        appended_file_df.to_csv("./temp.csv", index=False)
                        available_ram_gb = psutil.virtual_memory()[1]/1000000000
                        print('RAM Available (GB):', available_ram_gb)
                        memory_size = int(available_ram_gb * 1000)
                        for i, chunk in enumerate(pd.read_csv("./temp.csv", chunksize=memory_size)):
                            new_file_name = './Output-Data/' + table_name + '/chunk{}.csv'.format(i)
                            chunk.to_csv(new_file_name, index=False)
                        os.remove("./temp.csv")

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
