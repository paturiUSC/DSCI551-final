import os 
import pandas as pd
import re

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

def insertData(): 
    return ""

def updateData(): 
    return ""

def deleteData(column_headers): 
    # Display the example prompts for the user
    print("\nHere are 3 example prompts to delete data from the database. Use a semicolon ';' to end the command.")
    print("Option 1 (deleting all data from user-generated and pre-partitioned tables): Delete all;")
    print("Option 2 (deleting specific rows based on a comparison/rule AND make sure column name is right before the comparison): Delete rows where stock_price > than 50;")
    print("For Option 2, make sure to type out the comparison explicity: '=', '>', '>=', '<',  '<=', or 'like' (for strings).")
    print("Option 3 (deleting user-generated table): Delete all from table_name;\n")

    # Get and validate user input
    user_delete_input = input(">")

    while (not user_delete_input) or (user_delete_input[-1:] != ";"): 
        user_delete_input = input(">")
    
    user_delete_input = user_delete_input.lower()

    directory_data_files = os.listdir("./DSCI551-final/Output-Data")

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

        # Extract and print the matched phrases, along with words/values before and after
        df_filter_expression = None

        for file in directory_data_files: 
            file_path = "./DSCI551-final/Output-Data/" + file

            file_df = pd.read_csv(file_path)
            original_length = len(file_df)

            for match in matches:
                column = match[0]
                comparison_phrase = match[1]
                value = match[2]
                
                print(f"These are the identified key tokens from the command:'{column}' {comparison_phrase} '{value}'")

                if column in column_headers:
                    # Construct a filter expression
                    if comparison_phrase == '=':
                        value = int(value)
                        condition = (file_df[column] == value)
                        # print(condition)
                    elif comparison_phrase == '<':
                        value = int(value)
                        condition = (file_df[column] < value)
                        # print(condition)
                    elif comparison_phrase == '>':
                        value = int(value)
                        condition = (file_df[column] > value)
                        # print(condition)
                    elif comparison_phrase == '<=':
                        value = int(value)
                        condition = (file_df[column] <= value)
                        # print(condition)
                    elif comparison_phrase == '>=':
                        value = int(value)
                        condition = (file_df[column] >= value)
                        # print(condition)
                    elif comparison_phrase == 'like':
                        condition = file_df[column].str.contains(value, case=False)
                        # print(condition)
                    
                    # Combine conditions using logical 'and'
                    if df_filter_expression is None:
                        df_filter_expression = condition
                    # else:
                    #     df_filter_expression = df_filter_expression & condition
                else: 
                    print("The column", column, "is not a valid column. So, not going to delete rows based on that condition.")
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

def menu_option_action(menu_option, column_headers):
    if menu_option == "a": 
        print("You've chosen to select data.")
        selectData()
    elif menu_option == "b":
        print("You've chosen to insert data.")
        insertData()

        # insert specific data/rows
        # insert tables (stored as a separate file where table name is the name of the file)

    elif menu_option == "c":
        print("You've chosen to update data.")
        updateData()

        # update all 
        # update specific data

    elif menu_option == "d":
        print("You've chosen to delete data.")
        deleteData(column_headers)

        # delete all (including newly created table files)
        # delete specific data
        # delete tables you've created (which are stored in separate files)

    else: 
        print("Invalid menu option!")

if __name__ == "__main__":
    column_headers = ['date', 'open', 'high', 'low', 'close', 'volume', 'Name']

    # See if the user wants to use the database
    use_database_input = useDatabase()

    # Get user input on what operation the user wants to perform on the database
    while use_database_input:
        menu_option = displayUserMenu()

        menu_option_action(menu_option, column_headers)

        use_database_input = useDatabase()

    print("\nThank you for using the database!")



'''
an example query prompt will be displayed to the user. For instance, if the user chooses to insert data through the command line interface, the function insertData() will be called, and an example prompt such as “Insert the data stored in location extra_data.csv" will be displayed. Then, the user can input a similar prompt to insert additional data. This will be accomplished through the same methods used to process and partition the original input data. 
'''

'''
Include an option to exit at anytime by typing 'exit' within the select/insert/update/delete data functions
'''