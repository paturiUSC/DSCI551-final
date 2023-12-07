import os 
import pandas as pd
import re

# Function allowing the user to delete data in each chunk of the table, the entire table, or all the tables in the database system 
def deleteData(): 
    # Display the example prompts for the user
    print("\nHere are 3 example prompts to delete data from the database. Use a semicolon ';' to end the command.")
    print("Option 1 (deleting all data from user-generated and pre-partitioned tables): Delete all;")
    print("Option 2 (deleting specific rows based on a comparison/rule AND make sure column name is right before the comparison): Delete rows where stock_price > 50;")
    print("For Option 2, make sure to type out the comparison explicity: '=', '>', '>=', '<',  '<=', or 'like' (for strings).")
    print("Option 3 (deleting user-generated table): Delete all from table_name;\n")

    print("\nExit out at anytime by typing 'exit;'")

    # Get and validate user input
    user_delete_input = input(">")

    while (not user_delete_input) or (user_delete_input[-1:] != ";") or ("delete" not in user_delete_input.lower()): 
        user_delete_input = input(">")
    
    if ("exit" in user_delete_input):
        return ""
    
    user_delete_input = user_delete_input.lower()

    if not os.path.exists("./Output-Data"):
            os.makedirs("./Output-Data")

    # Identify the relevant tables and corresponding chunks of data
    directory_data_directories = os.listdir("./Output-Data")
    directory_data_files = []
    for directory in directory_data_directories:
        for file in os.listdir("./Output-Data/" + directory):
            directory_data_files.append("./Output-Data/" + directory + "/" + file)

    if len(directory_data_files) == 0:
        print("\nNo data in the database right now.")
        return ""

    # Delete all data
    if "all" in user_delete_input and "from" not in user_delete_input: 
        for file in directory_data_files: 
            file_path = "./Output-Data/" + file
            print("Deleted file", file_path)
            os.remove(file_path)
        print("Deleted all data!")
    
    # Delete all data from a specific table
    elif "all" in user_delete_input and "from" in user_delete_input:
        found = False
        # Validate that the table to be deleted exists
        for directory in directory_data_directories: 
            if directory in user_delete_input:
                table_name = directory
                print("\nTable name identfied:", str(table_name))
                found = True
        if not found:
            print("\nNo valid table name identified.")
            print("Here is a list of valid tables in the database you can delete.")
            print(directory_data_directories)
            return ""
        while not os.path.exists("./Output-Data/" + table_name): 
            table_name = input("The entered table does not exist. Please enter a valid table name: ")
        file_path = "./Output-Data/" + table_name
        table_files = os.listdir(file_path)
        # Delete the table chunk by chunk
        for file in table_files:
            os.remove(file_path + "/" + file)
            print("Removed the chunk named:", str(file))
        os.rmdir(file_path)

        print("\n\nDeleted all data from table", table_name)

    # Delete specific rows of data
    elif "=" in user_delete_input or ">" in user_delete_input or ">=" in user_delete_input or "<" in user_delete_input or "<=" in user_delete_input or "like" in user_delete_input:

        # Identify which table to delete from 
        table_name = input("Enter the table name to delete these rows from: ")
        # Check if this table exists 
        while (not os.path.exists("./Output-Data/" + table_name)):
            table_name = input("Enter a valid table name to delete these rows from: ")
        print("Great. The table", table_name, "does exist.")

        # Identify the relevant columns and corresponding comparison phrases 
        # Define the regular expression pattern to match the specified phrases
        pattern = r'(\b\w+\b)\s*(=|<|>|<=|>=|like)\s*(\b\w+\b)'

        # Find all matches in the input text
        matches = re.findall(pattern, user_delete_input, re.IGNORECASE)

        # Apply delete input on each table in the database chunk by chunk
        directory_data_files = os.listdir("./Output-Data/" + table_name)
        print(directory_data_files)
        for file in directory_data_files: 
            # Extract and print the matched phrases, along with words/values before and after
            df_filter_expression = None

            file_path = "./Output-Data/" + table_name + "/" + file

            file_df = pd.read_csv(file_path)
            original_length = len(file_df)
            column_headers = file_df.columns

            for match in matches:
                column = match[0]
                comparison_phrase = match[1]
                value = match[2]
                
                print(f"\nThese are the identified key tokens from the command:'{column}' {comparison_phrase} '{value}'")

                # Construct the DataFrame filter expressions
                if column in column_headers:
                    # Construct a filter expression
                    if comparison_phrase == '=':
                        try:
                            if (value.isdecimal()):
                                value = int(value)
                            else: 
                                value = float(value)
                            condition = (file_df[column] == value)
                        except: 
                            print("When using the =  operator, must have a number.")
                    elif comparison_phrase == '<':
                        try:
                            if (value.isdecimal()):
                                value = int(value)
                            else: 
                                value = float(value)
                            condition = (file_df[column] < value)
                        except: 
                            print("When using the <  operator, must have a number.")
                    elif comparison_phrase == '>':
                        try:
                            if (value.isdecimal()):
                                value = int(value)
                            else: 
                                value = float(value)
                            condition = (file_df[column] > value)
                        except: 
                            print("When using the >  operator, must have a number.")
                    elif comparison_phrase == '<=':
                        try:
                            if (value.isdecimal()):
                                value = int(value)
                            else: 
                                value = float(value)
                            condition = (file_df[column] <= value)
                        except: 
                            print("When using the <=  operator, must have a number.")
                    elif comparison_phrase == '>=':
                        try:
                            if (value.isdecimal()):
                                value = int(value)
                            else: 
                                value = float(value)
                            condition = (file_df[column] >= value)
                        except: 
                            print("When using the >=  operator, must have a number.")
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

                print("Dropping in " + file + " for table " + table_name + "...")
                filtered_file_df = file_df[~df_filter_expression]
                dropped_rows = original_length - len(filtered_file_df)
                
                print("These are how many rows got dropped/deleted in " + file + ":" , str(dropped_rows))
                filtered_file_df.to_csv(file_path, index=False)
            else:
                print("No valid delete command found in your input.")

        print("\nDeleted specific rows.")

    else: 
        print("Please enter a valid delete statement next time.")

    print("\nDelete data operation complete!")
