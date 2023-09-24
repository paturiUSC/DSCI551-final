import pandas as pd
import os 

# Function to get the user input
def get_user_input_file():
    print("Welcome to the database! For this database, please enter the file location of the data you would like to upload to the database.")
    print("Please ensure the data is structured. This database acts like a relational database handling structured data.")
    print("Please only input a .csv file.")
    file_location = input(">")
    while not os.path.exists(file_location): 
        file_location = input("Please enter a file path location for the structued data: ")
    print("Thank you for entering the file location path. Accessing now ...")
    return file_location

# Function to process and partition the user inputted data file
def processAndPartitionInput(file_location): 
    output_df_and_column_headers = []

    # Process a csv file
    if file_location[-3:] == "csv": 
        file_df = pd.read_csv(file_location)
        output_df_and_column_headers.append(file_df)
        # Get the column headers
        file_columns = getColumnHeaders(file_df)
        output_df_and_column_headers.append(file_columns)

        if not os.path.exists("./DSCI551-final/Output-Data"):
            os.makedirs("./DSCI551-final/Output-Data")
        
        partitioned_file_names = partitionInput(file_location)

        output_df_and_column_headers.append(partitioned_file_names)

    # Process a txt file
    elif file_location[-3:] == "txt":
        txt_file_delimiter = input("Enter the delimiter for the inputted txt file: ")
        while txt_file_delimiter != "":
            txt_file_delimiter = input("Enter the delimiter for the inputted txt file: ")
        
        file_df = pd.read_csv(file_location, sep=txt_file_delimiter)
        output_df_and_column_headers.append(file_df)
        # Get the column headers
        file_columns = getColumnHeaders(file_df)
        output_df_and_column_headers.append(file_columns)

        if not os.path.exists("./DSCI551-final/Output-Data"):
            os.makedirs("./DSCI551-final/Output-Data")
        
        partitioned_file_names = partitionInput(file_location)

        output_df_and_column_headers.append(partitioned_file_names)
    
    else: 
        print("Please enter a valid .csv or .txt file as the input data.")
        return ""

    return output_df_and_column_headers

# Function to get the column headers from the data file
def getColumnHeaders(file_df):
    file_columns = list(file_df.columns)
    return file_columns

# Helper function to partition the input data
def partitionInput(file_location): 
    partitionedDataFileNames = []

    print("The determined chunk size is 2000 to have 2000 rows of data per partitioned dataset.")

    # partition into different CSV files 
    for i, chunk in enumerate(pd.read_csv(file_location, chunksize=50)):
        new_file_name = './DSCI551-final/Output-Data/chunk{}.csv'.format(i)
        chunk.to_csv(new_file_name, index=False)
        partitionedDataFileNames.append(new_file_name)
        print("Created", new_file_name)
    
    return partitionedDataFileNames

if __name__ == "__main__":
    # Get the user input data 
    file_location = get_user_input_file()

    # Download the data into a csv file
    # Partition the dataset and return the location + names of the partitioned files
    processed_and_partitioned_input_data = processAndPartitionInput(file_location)

    print(processed_and_partitioned_input_data)
