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

def deleteData(): 
    return ""

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
    elif menu_option == "d":
        print("You've chosen to delete data.")
        deleteData()
    else: 
        print("Invalid menu option!")

if __name__ == "__main__":
    # See if the user wants to use the database
    use_database_input = useDatabase()

    # Get user input on what operation the user wants to perform on the database
    while use_database_input:
        menu_option = displayUserMenu()

        menu_option_action(menu_option)

        use_database_input = useDatabase()

    print("Thank you for using the database!")



'''
an example query prompt will be displayed to the user. For instance, if the user chooses to insert data through the command line interface, the function insertData() will be called, and an example prompt such as “Insert the data stored in location extra_data.csv" will be displayed. Then, the user can input a similar prompt to insert additional data. This will be accomplished through the same methods used to process and partition the original input data. 
'''

'''
Include an option to exit at anytime by typing 'exit' within the select/insert/update/delete data functions
'''