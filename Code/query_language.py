# Import the necessary functions
from data_modification.update_data_modification import updateData
from data_modification.insert_data_modification import insertData
from data_modification.delete_data_modification import deleteData
from data_input.input_data import inputData
from query_processing.query_processing_helper_functions import remove_files_in_directory
from query_processing.select_query import selectData

# Function to display the actions the user can choose to take on the database system 
def displayUserMenu():
    print("What would you like to do? Please enter the corresponding number of the operation you would like to perform.")
    print("A. Select Data (including joins)")
    print("B. Insert Data")
    print("C. Update Data")
    print("D. Delete Data")
    print("E. Input Data")

    # Get the user input
    user_input = input(">")
    user_input = user_input.lower()

    while (user_input != "a" and user_input != "b" and user_input != "c" and user_input !="d" and user_input != "e") or (not user_input): 
        user_input = input("Please enter a valid input of 'A', 'B', 'C', 'D', or 'E': ")
        user_input = user_input.lower()

    return user_input

# Function to allow the user to enter and interact with the database system
def useDatabase():
    print("\nWould you like to perform an operation on the database?")
    user_input = input("Please enter 'yes' or 'no': ")

    # Get the user input
    user_input = user_input.lower()

    while (not user_input) or (user_input != "yes" and user_input != "no"):
        user_input = input("Please enter 'yes' or 'no': ")
        user_input = user_input.lower()

    return user_input == "yes"

# Function to get the user input on which action to take
def menu_option_action(menu_option):
    if menu_option == "a": 
        print("You've chosen to select data.")
        # Clear the directory for the new sorted and chunked data
        remove_files_in_directory("./Temp-Results")
        try:
            selectData()
        except:
            print("\nError (potentially wrong action to execute your input). Please try again!")    
    elif menu_option == "b":
        print("You've chosen to insert data.")
        try:
            insertData()
        except:
            print("\nError (potentially wrong action to execute your input). Please try again!")    
    elif menu_option == "c":
        print("You've chosen to update data.")
        try:
            updateData()
        except:
            print("\nError (potentially wrong action to execute your input). Please try again!")    
    elif menu_option == "d":
        print("You've chosen to delete data.")
        try:
            deleteData()
        except:
            print("\nError (potentially wrong action to execute your input). Please try again!")    
    elif menu_option == "e": 
        print("You've chosen to input data.")
        try:
            inputData()
        except:
            print("\nError (potentially wrong action to execute your input). Please try again!")    
    else: 
        print("Invalid menu option!")

# Execute the database system program
if __name__ == "__main__":
    # See if the user wants to use the database
    use_database_input = useDatabase()

    # Get user input on what operation the user wants to perform on the database
    while use_database_input:
        menu_option = displayUserMenu()
        menu_option_action(menu_option)
        use_database_input = useDatabase()

    print("\nThank you for using the database!")
