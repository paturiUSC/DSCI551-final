import os 
import pandas as pd
import re

# Function allowing the user to update the data in each chunk of the table in the database system 
def updateData(): 
    # Display the example prompts for the user
    print("\nHere are 2 example prompts to update data in the database. Use a semicolon ';' to end the command.")
    print("Option 1 (updating a particular column value): IF (col4 < condition_value, col5 >= condition_value2) UPDATE table_name (col1 = new_value);")
    print("Option 2 (updating multiple values): IF (col4 = condition_value1) UPDATE table_name (col1 = new_value1, col2 = new_value2);")
    print("\nExit out at anytime by typing 'exit;'\n")

    # Get and validate user input
    user_update_input = input(">")

    while (not user_update_input) or (user_update_input[-1:] != ";"): 
        user_update_input = input(">")
    
    if ("exit" in user_update_input):
        print("\nExiting...")
        return ""

    if ("update" not in user_update_input.lower() or "if" not in user_update_input.lower()): 
        print("\nPlease enter a valid update statement, as provided in the examples above.")
        print("Must include an 'IF' and an 'UPDATE' in the statement at least.")
        return ""
    
    user_update_input = user_update_input.lower()

    if not os.path.exists("./Output-Data"):
        os.makedirs("./Output-Data")

    # Identify the conditions on when to update and which columns and values to update in each chunk
    user_update_input_split = user_update_input.split()
    in_index = user_update_input_split.index("update")
    table_name = user_update_input_split[in_index + 1]

    first_open_parenthesis_index = user_update_input.find("(")
    first_close_parenthesis_index = user_update_input.find(")")

    second_open_parenthesis_index = user_update_input.find("(", first_open_parenthesis_index + 1)
    second_close_parenthesis_index = user_update_input.find(")", first_close_parenthesis_index + 1)

    update_conditions_str = user_update_input[first_open_parenthesis_index + 1 : first_close_parenthesis_index]
    update_columns_str = user_update_input[second_open_parenthesis_index + 1 : second_close_parenthesis_index]

    # Split the column names and values strings into individual lists
    columns_to_update = [col.strip() for col in update_columns_str.split(',')]
    column_names_to_update = [col.split("=")[0].strip() for col in columns_to_update]
    column_values_to_update = [col.split("=")[1].strip() for col in columns_to_update]

    update_conditions = [col.strip() for col in update_conditions_str.split(',')]
    update_condition = False
    if len(update_conditions[0]) > 0:
        update_condition = True
        # Identify the filter conditions inputted by the user
        pattern = re.compile(r'\s*(<|>|=|>=|<=|LIKE)\s*')
        update_conditions_column_names = [re.split(pattern, item)[0] for item in update_conditions]
        update_conditions_operator = [re.split(pattern, item)[1] for item in update_conditions]
        update_conditions_column_values = [re.split(pattern, item)[2] for item in update_conditions]
    
    # Print the results of the identified table name, update conditions, and update columns and values to the user
    print("\n\nTable Name:", table_name)
    if update_condition:
        print("Conditions:")
        for index in range(len(update_conditions_column_names)):
            print(f"\tColumn: {update_conditions_column_names[index]}, Operator: {update_conditions_operator[index]}, Condition: {update_conditions_column_values[index]}")
    print("Updates:")
    for index in range(len(column_names_to_update)):
        print(f"\tColumn: {column_names_to_update[index]}, New Value: {column_values_to_update[index]}")
    print("\n")

    # Check if the table exists
    tables_path = "./Output-Data/"
    table_path = tables_path + str(table_name)
    
    if os.path.exists(table_path):
        # Read the .csv file and show the table information to the user 
        table_chunks = os.listdir(table_path)
        file_df = pd.read_csv(table_path + "/chunk0.csv")
        for chunk in table_chunks: 
            if chunk != "chunk0.csv":
                chunk_df = pd.read_csv(table_path + "/" + chunk)
                file_df = pd.concat([file_df, chunk_df], ignore_index=True)
        print("Here is the info of the table.")
        file_df.info()
    
        # Filter condition 
        df_filter_expression = None
        str_filter_expression = None

        # Check if all the user-inputted columns in the update statement are in the table
        table_columns = list(file_df.columns)
        for list_index in range(len(column_names_to_update)):
            update_data = True
            column_name = column_names_to_update[list_index]
            column_name = column_name.strip().lower()
            if column_name in table_columns:
                # Check if data types match of the column values 
                value = column_values_to_update[list_index]

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
                    update_data = True
                else: 
                    if file_df[column_name].dtype == "float64":
                        # int can be converted to float
                        if isinstance(value, int):
                            update_data = True
                        else: 
                            print("\nNot ok")
                            print("Don't add.")
                            update_data = False
                            print("Datatype mismatch for the column", column_name)
                    else: 
                        if (str(file_df[column_name].dtype) == "object"): 
                            update_data = True
                        else:  
                            # If the column type is int, convert float values to int
                            if (file_df[column_name] == "int64"):
                                if (type(value) == float):
                                    value = int(value)
                                    update_data = True
                                elif (type(value) == int):
                                    update_data=True    
                                else: 
                                    update_data = False
                                    print("\nDatatype mismatch for the column", column_name)
                            else: 
                                update_data = False
                                print("\nDatatype mismatch for the column", column_name)

            if not update_data:
                print("\nDatatype mismatch. Cannot update the values without changing the datatype of the entire column.")
                print("Here is the the table's datatype information.")
                print(file_df.info())
                return ""
            
        # Repeat the check for the update conditions and build the filter expression
        if update_condition:
            for list_index in range(len(update_conditions_column_names)):
                update_data = True
                column_name = update_conditions_column_names[list_index]
                column_name = column_name.strip().lower()
                if column_name in table_columns:
                    # Check if data types match of the column values 
                    value = update_conditions_column_values[list_index]

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
                        update_data = True
                    else: 
                        if file_df[column_name].dtype == "float64":
                            # int can be converted to float
                            if isinstance(value, int):
                                update_data = True
                            else: 
                                print("\nNot ok")
                                print("Don't add.")
                                update_data = False
                                print("Datatype mismatch for the column", column_name)
                        else: 
                            if (str(file_df[column_name].dtype) == "object"): 
                                update_data = True
                            else:  
                                # If the column type is int, convert float values to int
                                if (file_df[column_name] == "int64"):
                                    if (type(value) == float):
                                        value = int(value)
                                        update_data = True
                                    elif (type(value) == int):
                                        update_data=True    
                                    else: 
                                        update_data = False
                                        print("\nDatatype mismatch for the column", column_name)
                                else: 
                                    update_data = False
                                    print("\nDatatype mismatch for the column", column_name)

                if not update_data:
                    print("\nDatatype mismatch. Cannot update the values without changing the datatype of the entire column.")
                    print("Here is the the table's datatype information.")
                    print(file_df.info())
                    return ""

                # If the data types and column names match, build the filter expression to identify which rows to update
                operator = update_conditions_operator[index]
                if operator == '=':
                    try: 
                        value = float(value)
                        condition = (file_df[column_name] == value)
                        if str_filter_expression is None:
                            str_filter_expression = "(file_df[" + str(column_name) + "] == " + str(value) + ")"
                        else: 
                            str_filter_expression += "& (file_df[" + str(column_name) + "] == " + str(value) + ")"
                    except: 
                        print("When using the =  operator, must have a number.")
                elif operator == '<':
                    try:
                        value = float(value)
                        condition = (file_df[column_name] < value)
                        if str_filter_expression is None:
                            str_filter_expression = "(file_df[" + str(column_name) + "] < " + str(value) + ")"
                        else: 
                            str_filter_expression += "& (file_df[" + str(column_name) + "] < " + str(value) + ")"
                    except: 
                        print("When using the <  operator, must have a number.")
                elif operator == '>':
                    try:
                        value = float(value)
                        condition = (file_df[column_name] > value)
                        if str_filter_expression is None:
                            str_filter_expression = "(file_df[" + str(column_name) + "] > " + str(value) + ")"
                        else: 
                            str_filter_expression += "& (file_df[" + str(column_name) + "] > " + str(value) + ")"
                    except: 
                        print("When using the >  operator, must have a number.")
                elif operator == '<=':
                    try:
                        value = float(value)
                        condition = (file_df[column_name] <= value)
                        if str_filter_expression is None:
                            str_filter_expression = "(file_df[" + str(column_name) + "] <= " + str(value) + ")"
                        else: 
                            str_filter_expression += "& (file_df[" + str(column_name) + "] <= " + str(value) + ")"
                    except: 
                        print("When using the <=  operator, must have a number.")
                elif operator == '>=':
                    try:
                        value = float(value)
                        condition = (file_df[column_name] >= value)
                        if str_filter_expression is None:
                            str_filter_expression = "(file_df[" + str(column_name) + "] >= " + str(value) + ")"
                        else: 
                            str_filter_expression += "& (file_df[" + str(column_name) + "] >= " + str(value) + ")"
                    except: 
                        print("When using the >=  operator, must have a number.")
                elif operator == 'like':
                    condition = file_df[column_name].str.contains(value, case=False)
                    if str_filter_expression is None:
                        str_filter_expression = "(file_df[" + str(column_name) + "] LIKE " + str(value) + ")"
                    else: 
                        str_filter_expression += "& (file_df[" + str(column_name) + "] LIKE " + str(value) + ")"
                
                # Combine the filter expression conditions using logical 'and'
                if df_filter_expression is None:
                    df_filter_expression = condition
                else:
                    df_filter_expression = df_filter_expression & condition
        
        # If there is no condition on which rows to update, update the identified columns to the identified values on each chunk of the table
        if df_filter_expression is None:
            print("\nUpdating table " + table_name + "...")

            # Update chunk by chunk
            table_chunks = os.listdir(table_path)
            for chunk in table_chunks: 
                chunk_df = pd.read_csv(table_path + "/" + chunk)
                chunk_dict = chunk_df.to_dict(orient="list")
                print("Old values in chunk" + str(chunk) + ":")
                print(chunk_dict)

                for index in range(len(column_names_to_update)):
                    column_name = column_names_to_update[index]
                    new_column_value = column_values_to_update[index]

                    updated_values = [new_column_value for old_value in chunk_dict[column_name]]
                    print("\nNew updated values for the column", column_name, "in chunk", chunk)
                    print(updated_values)
                    chunk_dict[column_name] = updated_values

                chunk_df = pd.DataFrame(chunk_dict)
                chunk_df.to_csv(table_path + "/" + chunk, index=False)

                print("\n\nUpdated the values in chunk", chunk)
            
            print("\n\nFinished updating table", str(table_name))

        # If there is a condition on which rows to update, apply the filter expression on each chunk of the table, and update the identified columns to the identified values on each chunk of the table
        elif df_filter_expression is not None and update_condition: 
            # Now that the filter condition has been constructured, identify all the appropriate rows in the corresponding table/DataFrame
            # Show update output to user
            print("\nThis is the identified filter expression.")
            print("file_df[" + str_filter_expression + "]")

            print("\nThese are the identified rows:")
            total_rows = 0
            for chunk in table_chunks: 
                file_df = pd.read_csv(table_path + "/" + chunk)
                total_rows += len(file_df[df_filter_expression])

            # Identify the total number of rows in each chunk to be updated
            print("\nThere are", str(total_rows), "rows that have been identified in table", str(table_name), "to be updated.")
            if total_rows > 0:
                print("\nUpdating...")
                for index in range(len(column_names_to_update)):
                    column_name = column_names_to_update[index]
                    new_column_value = column_values_to_update[index]
                    for chunk in table_chunks: 
                        file_df = pd.read_csv(table_path + "/" + chunk)
                        file_df.loc[df_filter_expression.reset_index(drop=True), column_name] = new_column_value
                        file_df = file_df.applymap(lambda x: float(x) if isinstance(x, int) else x)
                        file_df.to_csv(table_path + "/" + chunk, index=False)
                        print("Updated", str(column_name), "to a value of", str(new_column_value), "in chunk", str(chunk))
            else: 
                print("\nNo rows meet the update conditions. No rows being updated.")
            print("\n\nFinished updating table", str(table_name), "based on the update conditions.")
        else: 
            print("\nThere was an error in identifying and building the filter expression. Please try again.")
            print("Please follow the syntax of using the keyword 'IF' followed by ' ()' with the condition on which rows to update inside the ().")
    else: 
        tables_path = "./Output-Data/"
        file_list = os.listdir(tables_path)
        print("This table does not exist in the database. Please include a valid table name.")
        print("Here is a list of valid table names, please try again.")
        print(file_list)
        return ""