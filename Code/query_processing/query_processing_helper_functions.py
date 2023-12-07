import os 
import pandas as pd
import csv
import heapq

temp_results_directory = "../../Temp-Results"

# Function to get the first chunk of the temporary output in the query execution plan
def getFirstChunkOfTemporaryTable():
    found_chunk = True
    # If a chunk of the table exists, read it 
    try: 
        temp_chunks = os.listdir("./Temp-Results")
        temp_df = pd.read_csv("./Temp-Results/" + str(temp_chunks[0]))
        found_chunk = True
    except:
        print("\nError. No chunk for the temporary output table found, meaning there are no results to be aggregated.")
        print("\nExiting the select operation. Please try again!")
        found_chunk = False
    
    if found_chunk: 
        return ["true", temp_df]
    else: 
        return ["false", ""]

# Function to convert a value to its appropriate data type
def convertValueToAppropriateDataType(filter_condition_value):
    try:
        # Identify if the value is numerical
        if (filter_condition_value.isdecimal()):
            filter_condition_value = int(filter_condition_value)
        # If it's not a valid integer, try converting to a float
        else: 
            filter_condition_value = float(filter_condition_value)
        print(f"Converted to number: {filter_condition_value}")
    # Identified the value is not numerical
    except:
        print(f"Not converted to number. Kept value as string: {filter_condition_value}")
    
    return filter_condition_value

# Function to sort each chunk of the database 
def sort_csv(input_file, output_file, sort_column, descending_order):
    with open(input_file, 'r') as csvfile:
        # Read the data in each chunk of the temporary output
        csv_reader = csv.DictReader(csvfile)
        data = list(csv_reader)

        # Sort the read data in each chunk based on the order by condition inputted by the user
        data.sort(key=lambda row: float(row[sort_column]), reverse=descending_order)

    with open(output_file, 'w', newline='') as output_csv:
        # Write the sorted data in each chunk back to storage 
        fieldnames = csv_reader.fieldnames
        csv_writer = csv.DictWriter(output_csv, fieldnames=fieldnames)
        csv_writer.writeheader()
        csv_writer.writerows(data)

# Function to merge sort across all sorted runs 
# Treat each chunk as a data stream via a queue data structure
def sort_and_merge_chunks(input_files, output_file_pattern, sort_column, column_names, items_per_output_file, descending_order):
    # Generator expression to open each chunk and convert it into a data stream
    csv_readers = (csv.DictReader(open(file, 'r')) for file in input_files)

    # Merge sorted CSV chunks where each chunk of data is a data stream
    merged_reader = heapq.merge(*csv_readers, key=lambda row: float(row[sort_column]), reverse=descending_order)

    # Counter variables for sorting and output file naming purposes
    item_count = 0
    file_count = 0
    output_file = output_file_pattern.format(file_count)
    # Clear the directory for the new sorted and chunked data
    remove_files_in_directory("./Temp-Results")

    # Identify the sorted data that needs to be written back to storage
    output_storage = []
    temp_output_storage = []
    for row in merged_reader: 
        if (item_count < items_per_output_file):
            temp_output_storage.append(row)
            item_count += 1
        else: 
            output_storage.append(temp_output_storage)
            temp_output_storage = [row]
            item_count = 1
    if (len(temp_output_storage) > 0): 
        output_storage.append(temp_output_storage)
    
    # Write the merged and sort data back to storage
    for temp_output in output_storage:
        output_file = output_file_pattern.format(file_count)
        with open(output_file, 'w', newline='') as output_csv:
            csv_writer = csv.DictWriter(output_csv, fieldnames=column_names)
            csv_writer.writeheader()
            for row in temp_output:
                csv_writer.writerow(row)
        file_count +=1 

# Function to remove all the files in a directory
def remove_files_in_directory(directory_path):
    # Loop through directory files
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(f"Error deleting {file_path}: {e}")

# Function to load all the chunks into main memory
def load_entire_table(): 
    print("\nLoading each chunk into main memory to show the final output.")
    print("Each chunk of the table in storage is stored in the folder named Temp-Results.")
    select_df = [] 
    table_chunks = os.listdir("./Temp-Results")
    for table_chunk in table_chunks:
        with open("./Temp-Results/" + table_chunk, 'r') as csv_chunk:
            csv_reader = csv.DictReader(csv_chunk)
            # Convert each chunk of the table to a DataFrame so it can later be concatted to a larger DataFrame
            table_chunk_df = pd.DataFrame(csv_reader)
            select_df.append(table_chunk_df)

    # Concatenate all DataFrames of each chunk of the table into a larger DataFrame
    select_df = pd.concat(select_df, ignore_index=True)
    print()
    print(select_df.shape)
    print(select_df)

    return select_df