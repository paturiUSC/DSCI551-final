import os 
import pandas as pd
import psutil

# Function to get the data in the inputted user file location
def inputData(): 
    print("\nInputting data to the database.\n")
    # Get the user input data 
    file_location = get_user_input_file()

    if (file_location != "exit"):
        # Download the data into a csv file
        # Partition the dataset and return the location + names of the partitioned files
        processed_and_partitioned_input_data = processAndPartitionInput(file_location)
        
        print("\n\n\n")
        print(processed_and_partitioned_input_data)
    else: 
        print("\nExited the database!")

# Function to get the input on the file location for the data to be entered into the database system
def get_user_input_file():
    print("Welcome to the database! For this database, please enter the file location of the data you would like to upload to the database.")
    print("Please ensure the data is structured. This database acts like a relational database handling structured data.")
    print("Please only input a .csv or .txt file. Please enter 'exit' to exit the system.\n")
    file_location = input(">")
    if file_location != "exit":
        while not os.path.exists(file_location) and (file_location != "exit"): 
            file_location = input("Please enter a valid file path location for the structued data: ")
        print("Thank you for entering the file location path. Accessing now ...")
    return file_location

# Function to get the column headers from the data file
def getColumnHeaders(file_df):
    file_columns = list(file_df.columns)
    file_columns = [column_name.lower() for column_name in file_columns]
    return file_columns

# Function to process and partition the user inputted data file
def processAndPartitionInput(file_location): 
    output_df_and_column_headers = []

    # Process a csv file
    if file_location[-3:] == "csv": 
        file_df = pd.read_csv(file_location)
        output_df_and_column_headers.append(file_df.head(5))
        # Get the column headers
        file_columns = getColumnHeaders(file_df)
        output_df_and_column_headers.append(file_columns)

        if not os.path.exists("./Output-Data"):
            os.makedirs("./Output-Data")
        
        partitioned_file_names = partitionInput(file_location)

        output_df_and_column_headers.append(partitioned_file_names)

    # Process a txt file
    elif file_location[-3:] == "txt":
        txt_file_delimiter = input("Enter the delimiter for the inputted txt file: ")
        while not txt_file_delimiter:
            txt_file_delimiter = input("Enter the delimiter for the inputted txt file: ")
        
        file_df = pd.read_csv(file_location, sep=txt_file_delimiter, engine='python')
        output_df_and_column_headers.append(file_df)
        # Get the column headers
        file_columns = getColumnHeaders(file_df)
        output_df_and_column_headers.append(file_columns)

        if not os.path.exists("./Output-Data"):
            os.makedirs("./Output-Data")
        
        partitioned_file_names = partitionInput(file_location)

        output_df_and_column_headers.append(partitioned_file_names)
    
    else: 
        print("Please enter a valid .csv or .txt file as the input data.")
        return ""

    return output_df_and_column_headers

# Helper function to partition the input data
def partitionInput(file_location): 
    partitionedDataFileNames = []

    # Output % usage of virtual_memory 
    print('\nRAM memory % used:', psutil.virtual_memory()[2])
    # Getting available virtual_memory in GB
    available_ram_gb = psutil.virtual_memory()[1]/1000000000
    print('RAM Available (GB):', available_ram_gb)
    memory_size = int(available_ram_gb * 1000)
    print("The determined chunk size is", memory_size, "and so there will be", memory_size ,"rows of data per partitioned dataset.\n\n")

    # Partition into different CSV files 
    table_name = file_location.split("/")[-1].split(".")[0]
    if not os.path.exists("./Output-Data/" + table_name):
            os.makedirs("./Output-Data/" + table_name)
    
    # Convert all integers to float values 
    df = pd.read_csv(file_location)
    df = df.applymap(lambda x: float(x) if isinstance(x, int) else x)
    df.columns = df.columns.str.lower()
    df.to_csv(file_location, index=False)

    for i, chunk in enumerate(pd.read_csv(file_location, chunksize=memory_size)):
        new_file_name = './Output-Data/' + table_name + '/chunk{}.csv'.format(i)
        chunk.to_csv(new_file_name, index=False)
        partitionedDataFileNames.append(new_file_name)
        print("Created", new_file_name)
     
    return partitionedDataFileNames
