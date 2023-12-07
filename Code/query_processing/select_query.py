# Import libraries
import os 
import pandas as pd
import re
import psutil
import csv 
from collections import defaultdict
import math

# Import necessary functions
from query_processing.query_processing_helper_functions import getFirstChunkOfTemporaryTable, convertValueToAppropriateDataType, load_entire_table, sort_csv, sort_and_merge_chunks

# Function to process queries written by the user chunk by chunk of relevant tables in the database system 
def selectData(): 
    # Display the example prompts for the user
    # join, aggregate, filter, order, group
    print("\nHere are 3 example prompts to select data in the database. Use a semicolon ';' to end the command.")
    print("Option 1 (filtering, grouping, ordering): find (stock open) IF (open greater than 120) in (chunk0) and make sure to group based by (Name) and show output IN the ORDER OF (open ascending, close descending);")
    print("Option 2 (join): FIND (stock open_x and stock Name) if (stock open_x price greater than 120 and stock close_x price less than 140) IN (chunk0) AND in (chunk1) combined via (Name) and make sure to group based by (Name, open_x) and show output IN THE ORDER OF (open_x ascending);")
    print("Option 3 (aggregate): FIND (stock open sum) IN (chunk0) and in (chunk1) combined VIA (Name) and in (chunk2) coMbined ViA (open) and show output in the order of (open descending);")

    print("\nWhen joining tables, include _right after the attribute for the table on the right side of the JOIN. This applies for all attributes except the attributes used to join the tables.")
    print("\nCapitalization of attributes MATTERS!! Other capitalization should not.\n")
    
    print("\nTo group based on an attribute, use the 'BASED ON' keyword.")
    print("\nTo join, use the 'AND' and  'ON' keyword.")
    print("\nTo order by, use the 'WITH' keyword.")
    print("\nTo filter, use the 'IF' keyword.")
    print("\nThere are multiple different aggregation options: sum, mean, max, min, count. These must be typed out in full after the name of the attribute you are looking for. For example, 'FIND (stock high maximum)'.")
    print("Please ensure this is an aggregation when grouping by a certain characteristic.")
    print("Make sure to include spaces after each keyword like 'FIND'.")
    print("\nExit out at anytime by typing 'exit;'\n")

    # Get and validate user input
    user_select_input = input(">")

    while (not user_select_input) or (user_select_input[-1:] != ";"): 
        user_select_input = input(">")
    
    if ("exit" in user_select_input):
        print("\nExiting...")
        return ""
    
    if ("find" not in user_select_input.lower() and "in" not in user_select_input.lower()): 
        print("\nPlease enter a valid select statement, as provided in the examples above.")
        print("Must include an 'IF' and an 'UPDATE' in the statement at least.")
        return ""

    if not os.path.exists("./Output-Data"):
        os.makedirs("./Output-Data")

    # select 
    select_regex = r'find\s+\((.*?)\)\s'
    select_match = re.search(select_regex, user_select_input, re.IGNORECASE)
    if select_match:
        columns = select_match.group(1)
        print("Projection columns identified.")
        print(columns)
    else:
        columns = ""
        print("No projection columns.")
    
    # filter 
    print()
    filter_regex = r'IF\s+\((.*?)\)\s+IN'
    filter_match = re.search(filter_regex, user_select_input, re.IGNORECASE)

    if filter_match:
        filter_conditions = filter_match.group(1)
        print("Filter conditions identified.")
        print(filter_conditions)
    else:
        filter_conditions = ""
        print("No filter condition(s).")
    
    # join - table names 
    print()
    table_matches = re.findall(r'in\s+\(([^)]*)\)', user_select_input, re.IGNORECASE)
    if table_matches:
        print("Table name(s) identified.")
        print(table_matches)
    else:
        print("No table name(s).")
    
    # join - join conditions 
    print()
    combined_via_matches = re.findall(r'(?:combined\s+via)\s+\(([^)]*)\)', user_select_input, re.IGNORECASE)
    if combined_via_matches:
        print("Join condition(s) identified.")
        print(combined_via_matches)
    else:
        print("No join conditions")


    # group by
    print()
    group_by_regex = r'group based by\s+\(([^)]*)\)'
    group_by_matches = re.findall(group_by_regex, user_select_input, re.IGNORECASE)
    if group_by_matches:
        attributes = [attr.strip() for attr in group_by_matches[0].split(',')]
        print("Grouped by attributes identified.")
        print(attributes)
    else:
        print("No grouped by attributes.")
    
    # order by 
    print()
    order_by_regex = r'IN\s+THE\s+ORDER\s+OF\s+\(([^)]*)\);'
    order_by_matches = re.findall(order_by_regex, user_select_input, re.IGNORECASE)
    if order_by_matches:
        order_by_attributes = []
        for attr in order_by_matches[0].split(','):
            attr = attr.strip()
            if 'ascending' in attr.lower():
                order_by_attributes.append((attr.split()[0], 'ascending'))
            elif 'descending' in attr.lower():
                order_by_attributes.append((attr.split()[0], 'descending'))
        
        print("Order by attributes identified.")
        print(order_by_attributes)
    else:
        print("No order by attributes.")


    # Check if the table names in the select statement are valid
    valid_tables = True
    for table_name in table_matches: 
        tables_path = "./Output-Data/"
        table_name = str(table_name)
        table_path = tables_path + table_name 
        if not os.path.exists(table_path):
            print("Table", str(table_name), "is not a table in the database.")
            print("Please input a valid table name.")
            valid_tables = False
    
    if not valid_tables: 
        print("\nExited the select operation as valid table name(s) were not inputted.")
        print("These are the tables currently in the database.")
        print(os.listdir("./Output-Data"))
        return ""
    
    # If the inputted table names are valid, execute the join condition first to combine any tables
    select_df = None
    if combined_via_matches: 
        temporary_output_chunk_num = 0
        available_ram_gb = psutil.virtual_memory()[1]/1000000000
        print('RAM Available (GB):', available_ram_gb)
        memory_size = int(available_ram_gb * 1000)
        print("If join output results are outputted, they will be chunked so that each output buffer does not exceed this memory size:", str(memory_size))

        # Check if there are enough join conditions (1 join attribute per 2 tables)
        num_join_conditions = len(combined_via_matches)
        num_table_names = len(table_matches)
        if (num_join_conditions != num_table_names - 1): 
            print("\nExited the select operation as valid table name(s) were not inputted.")
            print("There must be 1 attribute for which to join 2 tables.")
            return ""
        else: 
            print()
            print(str(num_table_names), "tables identified.")
            print(str(num_join_conditions), "attributes to join the tables on identified.")

        # Check if the join can be performed
        join_valid = True
        tables_path = "./Output-Data/"
        for join_condition_attribute in combined_via_matches: 
            print(join_condition_attribute)
            for table_index in range(len(table_matches) - 1): 
                # Check if the first table has the join attribute 
                table_name = table_matches[table_index]
                # Assuming every table has at least one chunk allocated to it (even when empty)
                table_path = tables_path + table_name + "/chunk0.csv"
                file_df = pd.read_csv(table_path)
                # Case sentitive join attribute names
                join_condition_attribute = str(join_condition_attribute)
                file_df_columns = list(file_df.columns)
                if join_condition_attribute not in file_df_columns: 
                    print(str(join_condition_attribute), "is not an attribute in table", str(table_name))
                    join_valid = False 

                # Check if the second table has the join attribute
                table_name = table_matches[table_index + 1]
                # Assuming every table has at least one chunk allocated to it (even when empty)
                table_path = tables_path + table_name + "/chunk0.csv"
                file_df = pd.read_csv(table_path)
                # Case sentitive join attribute names
                join_condition_attribute = str(join_condition_attribute)
                file_df_columns = list(file_df.columns)
                if join_condition_attribute not in file_df_columns: 
                    print(str(join_condition_attribute), "is not an attribute in table", str(table_name))
                    join_valid = False 

                print("The join attribute", join_condition_attribute, "is valid.")
            
        if not join_valid: 
            print("\nThe join could not be performed. The column names in the join conditions do not exist.")
            return ""
        print("\nAll the join attributes are valid. Proceeding to the join.\n")

        # Join the tables
        tables_path = "./Output-Data/"

        if len(table_matches) > 1:
            for table_name_index in range(1, len(table_matches)):
                # Join the first two tables in the query 
                if table_name_index == 1:
                    join_results = []
                    inner_join_column_names = []

                    # Nested loop join, where the outer relation is the one with all the joined output
                    outer_table = tables_path + table_matches[table_name_index - 1]
                    table1_chunks = os.listdir(outer_table)
                    for table1_chunk in table1_chunks:
                        join_result_fieldnames = []
                        with open(outer_table + "/" + table1_chunk, 'r') as outer_table_chunk:
                            outer_reader = csv.DictReader(outer_table_chunk)
                            outer_chunk_data = list(outer_reader)
                        
                            inner_table = tables_path + table_matches[table_name_index]
                            table2_chunks = os.listdir(inner_table)
                            for table2_chunk in table2_chunks:
                                with open(inner_table + "/" + table2_chunk, 'r') as inner_table_chunk:
                                    inner_reader = csv.DictReader(inner_table_chunk)
                                    inner_chunk_data = list(inner_reader)

                                    for outer_row in outer_chunk_data:
                                        for inner_row in inner_chunk_data:
                                            join_condition_met = all(outer_row[join_col] == inner_row[join_col] for join_col in [combined_via_matches[table_name_index - 1]])
                                            if join_condition_met:
                                                joined_row = {col: outer_row[col] for col in outer_row}
                                                for col in inner_row:
                                                    if col in list(outer_row.keys()):
                                                        joined_row[col + "_right"] = inner_row[col]
                                                        if col not in inner_join_column_names:
                                                            inner_join_column_names.append(col + "_right")
                                                    else: 
                                                        joined_row[col] = inner_row[col]
                                                        if col not in inner_join_column_names:
                                                            inner_join_column_names.append(col)
                                                join_results.append(joined_row)
                                                if (len(join_result_fieldnames) == 0):
                                                    join_result_fieldnames = join_results[0].keys()
                                                # If the output buffer is full, write it to storage
                                                if (len(join_results) == int(memory_size)):
                                                    if not os.path.exists("./Temp-Results"):
                                                        os.makedirs("./Temp-Results")
                                                    with open("./Temp-Results/chunk" + str(temporary_output_chunk_num) + ".csv", 'w', newline='') as temp_chunk_output: 
                                                        fieldnames = list(join_results[0].keys())
                                                        writer = csv.DictWriter(temp_chunk_output, fieldnames=fieldnames)
                                                        writer.writeheader()
                                                        writer.writerows(join_results)
                                                    temporary_output_chunk_num += 1
                                                    join_results = []
                    if join_results:
                        if not os.path.exists("./Temp-Results"):
                            os.makedirs("./Temp-Results")
                        with open("./Temp-Results/chunk" + str(temporary_output_chunk_num) + ".csv", 'w', newline='') as temp_join_output_csv:
                            fieldnames = list(join_results[0].keys())
                            writer = csv.DictWriter(temp_join_output_csv, fieldnames=fieldnames)
                            writer.writeheader()
                            writer.writerows(join_results)
                else: 
                    # If there are more than two tables to join in the query, join the other tables using the logic here
                    join_results = []
                    inner_join_column_names = []
                    temporary_output_chunk_num = 0

                    # Nested loop join, where the outer relation is the one with all the joined output
                    outer_table = "./Temp-Results"
                    table1_chunks = os.listdir(outer_table)
                    for table1_chunk in table1_chunks:
                        join_result_fieldnames = []
                        with open(outer_table + "/" + table1_chunk, 'r') as outer_table_chunk:
                            outer_reader = csv.DictReader(outer_table_chunk)
                            outer_chunk_data = list(outer_reader)
                        
                            inner_table = tables_path + table_matches[table_name_index]
                            table2_chunks = os.listdir(inner_table)
                            for table2_chunk in table2_chunks:
                                with open(inner_table + "/" + table2_chunk, 'r') as inner_table_chunk:
                                    inner_reader = csv.DictReader(inner_table_chunk)
                                    inner_chunk_data = list(inner_reader)

                                    for outer_row in outer_chunk_data:
                                        for inner_row in inner_chunk_data:
                                            join_condition_met = all(outer_row[join_col] == inner_row[join_col] for join_col in [combined_via_matches[table_name_index - 1]])
                                            if join_condition_met:
                                                joined_row = {col: outer_row[col] for col in outer_row}
                                                for col in inner_row:
                                                    if col in list(outer_row.keys()):
                                                        joined_row[col + "_right"] = inner_row[col]
                                                        if col not in inner_join_column_names:
                                                            inner_join_column_names.append(col + "_right")
                                                    else: 
                                                        joined_row[col] = inner_row[col]
                                                        if col not in inner_join_column_names:
                                                            inner_join_column_names.append(col)
                                                join_results.append(joined_row)
                                                if (len(join_result_fieldnames) == 0):
                                                    join_result_fieldnames = join_results[0].keys()
                                                # If the output buffer is full, write it to storage
                                                if (len(join_results) == int(memory_size)):
                                                    if not os.path.exists("./Temp-Results"):
                                                        os.makedirs("./Temp-Results")
                                                    with open("./Temp-Results/chunk" + str(temporary_output_chunk_num) + ".csv", 'w', newline='') as temp_chunk_output: 
                                                        fieldnames = list(join_results[0].keys())
                                                        writer = csv.DictWriter(temp_chunk_output, fieldnames=fieldnames)
                                                        writer.writeheader()
                                                        writer.writerows(join_results)
                                                    temporary_output_chunk_num += 1
                                                    join_results = []
                        
                    # Save the joined results in a temporary csv (i.e. "output buffer")
                    if not os.path.exists("./Temp-Results"):
                        os.makedirs("./Temp-Results")
                    with open("./Temp-Results/chunk" + str(temporary_output_chunk_num) + ".csv", 'w', newline='') as temp_join_output_csv:
                        print("Writing the", str(temporary_output_chunk_num), "chunk of the join result.")
                        fieldnames = list(join_result_fieldnames)
                        writer = csv.DictWriter(temp_join_output_csv, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(join_results)
                    temporary_output_chunk_num += 1
        
        # Put together the results from the chunking
        temporary_csv_files = [file for file in os.listdir("./Temp-Results") if file.endswith('.csv')]
        select_df = pd.DataFrame()
        for temporary_csv_file in temporary_csv_files:
            print("Reading", str(temporary_csv_file))
            temp_df = pd.read_csv("./Temp-Results/" + temporary_csv_file)
            select_df = pd.concat([select_df, temp_df], ignore_index=True)

        # Show the join output
        print("\n\nHere is the shape of the final table after the joins:")
        print(select_df.shape)
        print("\n\n")
        print(select_df.head())
        select_df = select_df.head(5)
    else: 
        # If no join conditions were identified
        print("\nNo join conditions identifed. Proceeding to the next operator in the query pipeline.")
        print("Identified only one table entered in the select statement as no join condition with another table was inputted.")
        table_name = table_matches[0]
        print("The identified table name is " + str(table_name) + "\n\n\n")
        table_chunks = os.listdir("./Output-Data/" + table_name)
        temporary_output_chunk_num = 0
        for table_chunk in table_chunks: 
            temp_df = pd.read_csv("./Output-Data/" + str(table_name) + "/" + table_chunk)
            temp_df.to_csv("./Temp-Results/chunk" + str(temporary_output_chunk_num) + ".csv", index=False)
            temporary_output_chunk_num += 1
        print("\nSince there were no join conditions identified, the size of the temporary table remains the same as the original table named", str(table_name))
        print("The chunks of the table remain untouched in storage.")
        print("There are", str(temporary_output_chunk_num), "chunks of the table named", str(table_name) ,"stored in storage still.")


    # Filter the table
    if filter_match: 
        # Identify each filter condition 
        filter_conditions = filter_conditions.split(" and ")
        print("\nNeed to filter the join output now.")
        for filter_condition in filter_conditions: 
            # Check if the attribute to filter on exists. If not, exit out.
            filter_condition_components = filter_condition.split(" ")
            filter_condition_attribute = filter_condition_components[0]
            filter_condition_value = filter_condition_components[len(filter_condition_components) - 1]

            if "equal to" in filter_condition.lower() and "less than" not in filter_condition.lower() and "greater than" not in filter_condition.lower(): 
                filter_condition_operator = "="
            elif "less than" in filter_condition.lower() and "equal to" not in filter_condition.lower():
                filter_condition_operator = "<"
            elif "less than or equal to" in filter_condition.lower():
                filter_condition_operator = "<="
            elif "greater than" in filter_condition.lower() and "equal to" not in filter_condition.lower():
                filter_condition_operator = ">"
            elif "greater than or equal to" in filter_condition.lower():
                filter_condition_operator = ">="
            elif "like" in filter_condition.lower(): 
                filter_condition_operator = "LIKE"
            elif "not equal to" in filter_condition.lower(): 
                filter_condition_operator = "!="
            else: 
                print("\nCould not identify an appropriate filter condition operator like 'greater than' or 'less than'.")
                print("Please enter a valid filter condition operator.")
                return ""
            # Display the identified filter conditions to the user
            print("\nIdentified", str(filter_condition_attribute), "as the filter condition attribute.")
            print("Identified", str(filter_condition_value), "as the filter condition value.")
            print("Identified", str(filter_condition_operator), "as the filter condition operator.\n")

            # Get the available column names in the temporary output
            get_first_chunk = getFirstChunkOfTemporaryTable()
            first_chunk = get_first_chunk[0]
            if (first_chunk != "false"): 
                temp_df = get_first_chunk[1]
            else: 
                return ""

            if (filter_condition_attribute not in list(temp_df.columns)):
                print("\nThe filter condition attribute", str(filter_condition_attribute), "is not a valid attribute to filter on.")
                print("Here is a list of valid attributes to filter on.")
                print(list(temp_df.columns))
                return ""
            
            # Build the filter condition to apply on each chunk of the data in the query execution plan
            # Database system has the ability to handle both integers and decimals in terms of numerical values
            if filter_condition_operator == "=":
                filter_condition_value = convertValueToAppropriateDataType(filter_condition_value)
            elif filter_condition_operator == "<":
                filter_condition_value = convertValueToAppropriateDataType(filter_condition_value)
            elif filter_condition_operator == "<=":
                filter_condition_value = convertValueToAppropriateDataType(filter_condition_value)
            elif filter_condition_operator == ">":
                filter_condition_value = convertValueToAppropriateDataType(filter_condition_value)
            elif filter_condition_operator == ">=":
                filter_condition_value = convertValueToAppropriateDataType(filter_condition_value)
            elif filter_condition_operator == "!=":
               filter_condition_value = convertValueToAppropriateDataType(filter_condition_value)
            else: 
                print("\nNo appropriate filter condition operator found. Please try again. An example operator is 'greater than'")
                return ""
            
            # Loop through the chunks of the temporary results and filter out the rows for each filter condition
            temp_chunks = os.listdir("./Temp-Results")
            temporary_output_chunk_num = 0
            for temp_chunk in temp_chunks:
                temp_df = pd.read_csv("./Temp-Results/" + str(temp_chunk))
                select_dict = temp_df.to_dict(orient="records")
                
                filtered_select_dict = []
                for row in select_dict: 
                    if filter_condition_operator == "=":
                        if (row[filter_condition_attribute] == filter_condition_value):
                            filtered_select_dict.append(row)
                    elif filter_condition_operator == "<":
                        if (row[filter_condition_attribute] < filter_condition_value):
                            filtered_select_dict.append(row)
                    elif filter_condition_operator == "<=":
                        if (row[filter_condition_attribute] <= filter_condition_value):
                            filtered_select_dict.append(row)
                    elif filter_condition_operator == ">":
                        if (row[filter_condition_attribute] > filter_condition_value):
                            filtered_select_dict.append(row)
                    elif filter_condition_operator == ">=":
                        if (row[filter_condition_attribute] >= filter_condition_value):
                            filtered_select_dict.append(row)
                    elif filter_condition_operator == "!=":
                        if (row[filter_condition_attribute] != filter_condition_value):
                            filtered_select_dict.append(row)
                    elif filter_condition_operator == "LIKE":
                        if (str(filter_condition_value) in str(row[filter_condition_attribute])):
                            filtered_select_dict.append(row)
                    else: 
                        print("\nNo appropriate filter condition operator found. Please try again. An example operator is 'greater than'")
                        return ""
            
                # Write the filtered chunk back to storage
                print("\nDone filtering on the filter condition:", str(filter_condition))
                select_df_temp = pd.DataFrame(filtered_select_dict)
                select_df_temp.to_csv("./Temp-Results/" + str(temp_chunk), index=False)
                print("Wrote this filtered chunk, " + str(temp_chunk) + ", back to storage.")
        
        # Put together the results from the chunking
        temporary_csv_files = [file for file in os.listdir("./Temp-Results") if file.endswith('.csv')]
        select_df = pd.DataFrame()
        for temporary_csv_file in temporary_csv_files:
            try: 
                temp_df = pd.read_csv("./Temp-Results/" + temporary_csv_file)
                select_df = pd.concat([select_df, temp_df], ignore_index=True)
            except: 
                print("\nChunk named,", str(temporary_csv_file) + ",is empty. Removing it.")
                os.remove("./Temp-Results/" + str(temporary_csv_file))

        # Display the filtering output to the user
        print("\n\nHere is the shape of the final table after the filtering:")
        print(select_df.shape)
        print("\n\n")
        print(select_df.head())
        select_df = select_df.head(5)
        print("\nDone with the filtering process. Proceeding to the next operator in the query execution plan, the aggregation operator.")
    else: 
        print("\nNo filtering detected. Proceeding to the next operator in the query execution plan, the aggregation operator.")

    # Grouping
    if group_by_matches:
        print("\n\nIn the group by operator.")
        # Identify the columns in the temporary table output thus far in the query execution plan
        get_first_chunk = getFirstChunkOfTemporaryTable()
        first_chunk = get_first_chunk[0]
        if (first_chunk != "false"): 
            temp_csv = get_first_chunk[1]
        else: 
            return ""

        # Check if each of the group by attributes are valid columns in the current output
        for group_by_column in group_by_matches:
            if (group_by_column not in list(temp_csv.columns)):
                print("\nThe group by attribute", group_by_column, "is not a valid column.")
                print("Here is a list of valid column names to group by.")
                print(list(temp_csv.columns))
                return ""
        
        print("The group by attributes are valid.")

        # Check for aggregation functions 
        valid_select_conditions = {}
        select_conditions = str(select_match[1]).split("and")
        print(select_conditions)
        # Loop through the table columns to ensure valid column names were inputted 
        for select_condition in select_conditions: 
            print("\nThis is the select condition being dissected to look for an aggregation function.")
            print(select_condition.strip())

            select_condition = select_condition.strip()
            if "*" not in select_condition:
                # Identify the column first 
                identified_column = ""
                found_column = False
                get_first_chunk = getFirstChunkOfTemporaryTable()
                first_chunk = get_first_chunk[0]
                if (first_chunk != "false"): 
                    temp_csv = get_first_chunk[1]
                else: 
                    return ""
                select_df_columns = list(temp_csv.columns)
                for column in select_df_columns: 
                    if column in select_condition: 
                        identified_column = column
                        found_column = True
                
                if not found_column: 
                    print("\nNo valid column could be identified in the phrase:\n", select_condition)
                    print("These are the valid column names for this table so far. Capitalization does matter for column names.")
                    print(select_df_columns)
                    identified_column = None
                    return ""
                
                # Identify the aggregation function. There must be an aggregation function accompanying a group by.
                column_and_operation_phrase = select_condition.lower()
                if "sum" in column_and_operation_phrase: 
                    print("\nAggregating attribute", identified_column , "via sum")
                    valid_select_conditions[identified_column] = "sum"
                elif "mean" in column_and_operation_phrase:
                    print("\nAggregating attribute", identified_column , "via mean or average")
                    valid_select_conditions[identified_column] = "mean"
                elif "max" in column_and_operation_phrase:
                    print("\nAggregating attribute", identified_column , "via max")
                    valid_select_conditions[identified_column] = "max"
                elif "min" in column_and_operation_phrase:
                    print("\nAggregating attribute", identified_column , "via min")
                    valid_select_conditions[identified_column] = "min"
                elif "count" in column_and_operation_phrase:
                    print("\nAggregating attribute", identified_column , "via count")
                    valid_select_conditions[identified_column] = "count"
                else: 
                    print("\nNo valid aggregation operation identified.")
                    print("These are the valid aggregation functions:")
                    print("sum, mean, max, min, count")
                    # return ""

        # If only one group by condition identified
        if (valid_select_conditions):
            aggregation_attribute = list(valid_select_conditions.keys())[0]
            name_of_aggregation_operation = valid_select_conditions[aggregation_attribute]
            if (len(list(valid_select_conditions.keys())) == 1):
                group_by_attribute = group_by_matches[0]
                value_attribute = list(valid_select_conditions.keys())[0]
                print("\nGrouping by the singular group by attribute: ", group_by_attribute)
                
                # Summation
                if name_of_aggregation_operation == "sum":
                    temporary_output_chunk_num = 0
                    aggregated_data = defaultdict(int)
                    print("\nGrouping by the sum.")
                    chunk_list = os.listdir("./Temp-Results")
                    for chunk in chunk_list:
                        chunk_df = pd.read_csv("./Temp-Results/" + str(chunk))
                        chunk_dict = chunk_df.to_dict(orient="records")
                        print("Reading the chunk named:", str(chunk))
                        for row in chunk_dict:
                            try: 
                                group_value = row[group_by_attribute]
                                value_to_aggregate = float(row[value_attribute])
                                aggregated_data[group_value] += value_to_aggregate
                            except: 
                                print("\nCan only sum numerical values. Please try again.")
                        os.remove("./Temp-Results/" + str(chunk))
                        print("Finished reading the chunk named:", str(chunk))

                    print("\nFinished reading all the chunks and almost finished storing the output to the database in chunks.")
                    # Write the data to storage in chunks
                    # First chunk the dictionary 
                    dict_chunks = []
                    current_chunk = {}
                    available_ram_gb = psutil.virtual_memory()[1]/1000000000
                    memory_size = int(available_ram_gb * 1000)
                    for key, value in aggregated_data.items():
                        current_chunk[key] = value
                        if len(current_chunk) == int(memory_size):
                            dict_chunks.append(current_chunk.copy())
                            current_chunk.clear()
                    # Add the remaining items if any
                    if current_chunk:
                        dict_chunks.append(current_chunk)
                    
                    temporary_output_chunk_num = 0
                    for chunk in dict_chunks:
                        with open("./Temp-Results/chunk" + str(temporary_output_chunk_num) + ".csv", 'w', newline='') as output_csv:
                            csv_writer = csv.writer(output_csv)
                            # Write header
                            csv_writer.writerow([group_by_column, str(value_attribute) + '_sum'])
                            temporary_output_chunk_num += 1
                            for key, value in aggregated_data.items():
                                csv_writer.writerow([key, value])
                                print("Here is the group:", str(key))
                                print("Here is the sum:", str(value))
                            temporary_output_chunk_num += 1
                    print("\nFinished storing the output to the database.")


                # Mean
                elif name_of_aggregation_operation == "mean":
                    temporary_output_chunk_num = 0
                    aggregated_data = defaultdict(lambda: {'count': 0, 'sum': 0})
                    print("\nGrouping by the mean.")
                    chunk_list = os.listdir("./Temp-Results")
                    for chunk in chunk_list:
                        chunk_df = pd.read_csv("./Temp-Results/" + str(chunk))
                        chunk_dict = chunk_df.to_dict(orient="records")
                        print("Reading the chunk named:", str(chunk))
                        for row in chunk_dict:
                            try: 
                                group_value = row[group_by_attribute]
                                value_to_aggregate = float(row[value_attribute])
                                if value_to_aggregate is not None and group_value != '' and not math.isnan(value_to_aggregate):
                                    aggregated_data[group_value]['count'] += 1
                                    aggregated_data[group_value]['sum'] += value_to_aggregate
                            except: 
                                print("\nCan only sum numerical values when calculating the mean. Please try again.")
                        os.remove("./Temp-Results/" + str(chunk))
                        print("Finished reading the chunk named:", str(chunk))

                    print("\nFinished reading all the chunks and almost finished storing the output to the database in chunks.")
                    # Write the data to storage in chunks
                    # First chunk the dictionary 
                    dict_chunks = []
                    current_chunk = {}
                    for key, value in aggregated_data.items():
                        current_chunk[key] = value
                        if len(current_chunk) == int(memory_size):
                            dict_chunks.append(current_chunk.copy())
                            current_chunk.clear()
                    # Add the remaining items if any
                    if current_chunk:
                        dict_chunks.append(current_chunk)
                    
                    temporary_output_chunk_num = 0
                    for chunk in dict_chunks:
                        with open("./Temp-Results/chunk" + str(temporary_output_chunk_num) + ".csv", 'w', newline='') as output_csv:
                            csv_writer = csv.writer(output_csv)
                            # Write header
                            csv_writer.writerow([group_by_column, str(value_attribute) + '_mean'])
                            temporary_output_chunk_num += 1
                            for key, value in aggregated_data.items():
                                group_mean = value['sum'] / value['count'] if value['count'] > 0 else 0
                                csv_writer.writerow([key, group_mean])
                                print("Here is the group value:", str(key))
                                print("Here is the mean:", str(group_mean))
                            temporary_output_chunk_num += 1
                    print("\nFinished storing the output to the database.")


                # Count
                elif name_of_aggregation_operation == "count":
                    temporary_output_chunk_num = 0
                    aggregated_data = defaultdict(int)
                    print("\nGrouping by the count.")
                    chunk_list = os.listdir("./Temp-Results")
                    for chunk in chunk_list:
                        chunk_df = pd.read_csv("./Temp-Results/" + str(chunk))
                        chunk_dict = chunk_df.to_dict(orient="records")
                        print("Reading the chunk named:", str(chunk))
                        for row in chunk_dict:
                            group_value = row[group_by_attribute]
                            value_to_aggregate = row[value_attribute]
                            if value_to_aggregate is not None and group_value != '' and not math.isnan(value_to_aggregate):
                                aggregated_data[group_value] += 1
                            else: 
                                aggregated_data[group_value] += 0
                        os.remove("./Temp-Results/" + str(chunk))
                        print("Finished reading the chunk named:", str(chunk))

                    print("\nFinished reading all the chunks and almost finished storing the output to the database in chunks.")
                    # Write the data to storage in chunks
                    # First chunk the dictionary 
                    dict_chunks = []
                    current_chunk = {}
                    for key, value in aggregated_data.items():
                        current_chunk[key] = value
                        if len(current_chunk) == int(memory_size):
                            dict_chunks.append(current_chunk.copy())
                            current_chunk.clear()
                    # Add the remaining items if any
                    if current_chunk:
                        dict_chunks.append(current_chunk)
                    
                    temporary_output_chunk_num = 0
                    for chunk in dict_chunks:
                        with open("./Temp-Results/chunk" + str(temporary_output_chunk_num) + ".csv", 'w', newline='') as output_csv:
                            csv_writer = csv.writer(output_csv)
                            # Write header
                            csv_writer.writerow([group_by_column, str(value_attribute) + '_count'])
                            temporary_output_chunk_num += 1
                            for key, value in aggregated_data.items():
                                csv_writer.writerow([key, value])
                                print("Here is the group value:", str(key))
                                print("Here is the count:", str(value))
                            temporary_output_chunk_num += 1
                    print("\nFinished storing the output to the database.")


                # Maximum
                elif name_of_aggregation_operation == "max":
                    temporary_output_chunk_num = 0
                    aggregated_data = defaultdict(lambda: {'max_value': float('-inf')})
                    print("\nGrouping by the max.")
                    chunk_list = os.listdir("./Temp-Results")
                    for chunk in chunk_list:
                        chunk_df = pd.read_csv("./Temp-Results/" + str(chunk))
                        chunk_dict = chunk_df.to_dict(orient="records")
                        print("Reading the chunk named:", str(chunk))
                        for row in chunk_dict:
                            try: 
                                group_value = row[group_by_attribute]
                                value_to_aggregate = row[value_attribute]
                                if not math.isnan(float(value_to_aggregate)):
                                    aggregated_data[group_value]['max_value'] = max(aggregated_data[group_value]['max_value'], value_to_aggregate)
                                else: 
                                    aggregated_data[group_value]['max_value'] = None
                            except: 
                                print("\nCan only retrieve the maximum for numbers. Please try again!")
                        os.remove("./Temp-Results/" + str(chunk))
                        print("Finished reading the chunk named:", str(chunk))

                    print("\nFinished reading all the chunks and almost finished storing the output to the database in chunks.")
                    # Write the data to storage in chunks
                    # First chunk the dictionary 
                    dict_chunks = []
                    current_chunk = {}
                    for key, value in aggregated_data.items():
                        current_chunk[key] = value
                        if len(current_chunk) == int(memory_size):
                            dict_chunks.append(current_chunk.copy())
                            current_chunk.clear()
                    # Add the remaining items if any
                    if current_chunk:
                        dict_chunks.append(current_chunk)
                    
                    temporary_output_chunk_num = 0
                    for chunk in dict_chunks:
                        with open("./Temp-Results/chunk" + str(temporary_output_chunk_num) + ".csv", 'w', newline='') as output_csv:
                            csv_writer = csv.writer(output_csv)
                            # Write header
                            csv_writer.writerow([group_by_column, str(value_attribute) + '_max'])
                            temporary_output_chunk_num += 1
                            for key, value in aggregated_data.items():
                                csv_writer.writerow([key, value['max_value']])
                                print("Here is the group value:", str(key))
                                print("Here is the max:", str(value['max_value']))
                            temporary_output_chunk_num += 1
                    print("\nFinished storing the output to the database.")


                # Minimum
                elif name_of_aggregation_operation == "min":
                    temporary_output_chunk_num = 0
                    aggregated_data = defaultdict(lambda: {'min_value': float('-inf')})
                    print("\nGrouping by the min.")
                    chunk_list = os.listdir("./Temp-Results")
                    for chunk in chunk_list:
                        chunk_df = pd.read_csv("./Temp-Results/" + str(chunk))
                        chunk_dict = chunk_df.to_dict(orient="records")
                        print("Reading the chunk named:", str(chunk))
                        for row in chunk_dict:
                            try: 
                                group_value = row[group_by_attribute]
                                value_to_aggregate = row[value_attribute]
                                if not math.isnan(float(value_to_aggregate)):
                                    aggregated_data[group_value]['min_value'] = max(aggregated_data[group_value]['min_value'], value_to_aggregate)
                                else: 
                                    aggregated_data[group_value]['min_value'] = None
                            except: 
                                print("\nCan only retrieve the minimum for numbers. Please try again!")
                        os.remove("./Temp-Results/" + str(chunk))
                        print("Finished reading the chunk named:", str(chunk))

                    print("\nFinished reading all the chunks and almost finished storing the output to the database in chunks.")
                    # Write the data to storage in chunks
                    # First chunk the dictionary 
                    dict_chunks = []
                    current_chunk = {}
                    for key, value in aggregated_data.items():
                        current_chunk[key] = value
                        if len(current_chunk) == int(memory_size):
                            dict_chunks.append(current_chunk.copy())
                            current_chunk.clear()
                    # Add the remaining items if any
                    if current_chunk:
                        dict_chunks.append(current_chunk)
                    
                    temporary_output_chunk_num = 0
                    for chunk in dict_chunks:
                        with open("./Temp-Results/chunk" + str(temporary_output_chunk_num) + ".csv", 'w', newline='') as output_csv:
                            csv_writer = csv.writer(output_csv)
                            # Write header
                            csv_writer.writerow([group_by_column, str(value_attribute) + '_min'])
                            temporary_output_chunk_num += 1
                            for key, value in aggregated_data.items():
                                csv_writer.writerow([key, value['min_value']])
                                print("Here is the group value:", str(key))
                                print("Here is the max:", str(value['min_value']))
                            temporary_output_chunk_num += 1
                    print("\nFinished storing the output to the database.")
            else: 
                print("\nCan only do aggregation via grouping on one singular attribute, not multiple attributes. Please try again.")
                return ""
        # Show a preview of the temporary output table and proceed to the next operation
        get_first_chunk = getFirstChunkOfTemporaryTable()
        first_chunk = get_first_chunk[0]
        if (first_chunk != "false"): 
            temp_df = get_first_chunk[1]
        else: 
            return ""
        print("\n\nHere is a sample of the table after the grouping and aggregation:")
        print(temp_df.head(5))
        print("\n\nThe grouping and aggregation operation is now complete.")
        print("\nProceeding to the next operator in the query execution plan, the sorting operator.")
    else: 
        # If aggregation conditions without grouping identified
        print("\nNo group by identified. Proceeding to aggregation.")

        print("Here are the select conditions to be analyzed.")
        print(select_match)

        # Check for aggregation functions 
        valid_select_conditions = {}
        if "*" in user_select_input:
            select_conditions = ["*"]
        else: 
            try:
                select_conditions = str(select_match[1]).split("and")
                print(select_conditions)
            except: 
                print("\nCould not identify the select conditions in the query. Please try again!")
                return ""
        # Loop through the table columns to ensure valid column names were inputted 
        for select_condition in select_conditions: 
            if select_condition != "*":
                print("\nThis is the select condition being dissected to look for an aggregation function.")
                print(select_condition.strip())

                select_condition = select_condition.strip()

                # Identify the column first 
                identified_column = ""
                found_column = False
                get_first_chunk = getFirstChunkOfTemporaryTable()
                first_chunk = get_first_chunk[0]
                if (first_chunk != "false"): 
                    temp_csv = get_first_chunk[1]
                else: 
                    return ""
                select_df_columns = list(temp_csv.columns)
                for column in select_df_columns: 
                    if column in select_condition: 
                        identified_column = column
                        found_column = True
                
                if not found_column: 
                    print("\nNo valid column could be identified in the phrase:\n", select_condition)
                    print("These are the valid column names for this table so far. Capitalization does matter for column names.")
                    print(select_df_columns)
                    identified_column = None
                    return ""
                
                # Identify the aggregation conditions and build the aggregation operation to apply on each chunk of the data in the query execution plan
                column_and_operation_phrase = select_condition.lower()
                if "sum" in column_and_operation_phrase: 
                    print("\nAggregating attribute", identified_column , "via sum")
                    valid_select_conditions[identified_column] = "sum"
                elif "mean" in column_and_operation_phrase:
                    print("\nAggregating attribute", identified_column , "via mean or average")
                    valid_select_conditions[identified_column] = "mean"
                elif "max" in column_and_operation_phrase:
                    print("\nAggregating attribute", identified_column , "via max")
                    valid_select_conditions[identified_column] = "max"
                elif "min" in column_and_operation_phrase:
                    print("\nAggregating attribute", identified_column , "via min")
                    valid_select_conditions[identified_column] = "min"
                elif "count" in column_and_operation_phrase:
                    print("\nAggregating attribute", identified_column , "via count")
                    valid_select_conditions[identified_column] = "count"

        # If there are aggregation functions, execute them
        if valid_select_conditions:
            list_of_aggregation_columns = list(valid_select_conditions.keys())

            if (len(list_of_aggregation_columns) > 0): 
                print("Proceeding to the aggregation as aggregation columns were identified.")
                
                # Check to see if different aggregation operations being performed on the same column
                for aggregation_column in list_of_aggregation_columns: 
                    if (list_of_aggregation_columns.count(aggregation_column) > 1): 
                        print("\nThe column", str(aggregation_column), "can only have one aggregation operation performed to it. Please try again.")
                        return ""
            
                print("\nAggregating...")
                # Loop through each of the chunks of the data and perform the aggregation 
                aggregated_data = {}
                chunk_list = os.listdir("./Temp-Results")
                for chunk in chunk_list:
                    print("\nReading the chunk named:", str(chunk))
                    chunk_df = pd.read_csv("./Temp-Results/" + str(chunk))
                    chunk_dict = chunk_df.to_dict(orient='records')

                    # For each chunk, perform all the relevant aggregations
                    for aggregation_column in list_of_aggregation_columns:
                        for row in chunk_dict:
                            # Summation
                            if valid_select_conditions[aggregation_column] == "sum":
                                try: 
                                    value_to_aggregate = float(row[aggregation_column])
                                    if value_to_aggregate is not None and not math.isnan(value_to_aggregate):
                                        if aggregation_column + '_sum' not in list(aggregated_data.keys()):
                                            aggregated_data[aggregation_column + '_sum'] = value_to_aggregate
                                        else:
                                            aggregated_data[aggregation_column + '_sum'] += value_to_aggregate
                                except: 
                                    print("\nCan only sum numerical values when calculating the sum. Please try again.")
                            # Count
                            elif valid_select_conditions[aggregation_column] == "count":
                                value_to_aggregate = row[aggregation_column]
                                if value_to_aggregate is not None and not math.isnan(value_to_aggregate):
                                    if aggregation_column + '_count' not in list(aggregated_data.keys()):
                                        aggregated_data[aggregation_column + '_count'] = 1
                                    else:
                                        aggregated_data[aggregation_column + '_count'] += 1
                                else: 
                                    if aggregation_column + '_count' not in list(aggregated_data.keys()):
                                        aggregated_data[aggregation_column + '_count'] = 0
                                    else:
                                        aggregated_data[aggregation_column + '_count'] += 0
                            # Maximum 
                            elif valid_select_conditions[aggregation_column] == "max": 
                                try: 
                                    value_to_aggregate = row[aggregation_column]
                                    if not math.isnan(float(value_to_aggregate)):
                                        if aggregation_column + '_max_value' not in list(aggregated_data.keys()):
                                            aggregated_data[aggregation_column + '_max_value'] = value_to_aggregate
                                        else:
                                            aggregated_data[aggregation_column + '_max_value'] = max(aggregated_data[aggregation_column + '_max_value'], value_to_aggregate)
                                    else: 
                                        aggregated_data[aggregation_column + '_max_value'] = None
                                except: 
                                    print("\nCan only retrieve the maximum for numbers. Please try again!")
                            # Minimum
                            elif valid_select_conditions[aggregation_column] == "min": 
                                try: 
                                    value_to_aggregate = row[aggregation_column]
                                    if not math.isnan(float(value_to_aggregate)):
                                        if aggregation_column + '_min_value' not in list(aggregated_data.keys()):
                                            aggregated_data[aggregation_column + '_min_value'] = value_to_aggregate
                                        else:
                                            aggregated_data[aggregation_column + '_min_value'] = min(aggregated_data[aggregation_column + '_max_value'], value_to_aggregate)
                                    else: 
                                        aggregated_data[aggregation_column + '_min_value'] = None
                                except: 
                                    print("\nCan only retrieve the minimum for numbers. Please try again!")
                            # Mean 
                            elif valid_select_conditions[aggregation_column] == "mean": 
                                try: 
                                    value_to_aggregate = float(row[aggregation_column])
                                    if value_to_aggregate is not None and not math.isnan(value_to_aggregate):
                                        if aggregation_column + '_mean_count' not in list(aggregated_data.keys()):
                                            aggregated_data[aggregation_column + '_mean_count'] = 1
                                            aggregated_data[aggregation_column + '_total'] = value_to_aggregate
                                        else:
                                            aggregated_data[aggregation_column + '_mean_count'] += 1
                                            aggregated_data[aggregation_column + '_total'] += value_to_aggregate
                                except: 
                                    print("\nCan only sum numerical values when calculating the mean. Please try again.")
                    # Finished processing the chunk 
                    os.remove("./Temp-Results/" + chunk)
                    print("Finished reading the chunk named:", str(chunk))   

                # Finished all chunking 
                print("\nFinished reading all the chunks and almost finished storing the output to the database in chunks.")

                # Without grouping, aggregation should result in minimal data, thus eliminating the need to chunk the temporary output
                temporary_output_chunk_num = 0
                data_to_write = {}
                with open("./Temp-Results/chunk" + str(temporary_output_chunk_num) + ".csv", 'w', newline='') as output_csv:
                    csv_writer = csv.writer(output_csv)
                    # Write header
                    temporary_output_chunk_num += 1
                    unperformed_mean_calculations = []
                    for key, value in aggregated_data.items():
                        # Need to do separate calculation for means before writing to the output
                        if "_mean_count" in key.lower() or "_total" in key.lower(): 
                            unperformed_mean_calculations.append(key.lower())
                            for unperformed_mean_calculation_outer in unperformed_mean_calculations: 
                                column_name_outer = unperformed_mean_calculation_outer.split("_", 1)[0]
                                for unperformed_mean_calculation_inner in unperformed_mean_calculations:
                                    if (unperformed_mean_calculation_outer.lower() != unperformed_mean_calculation_inner.lower()):
                                        column_name_inner = unperformed_mean_calculation_inner.split("_", 1)[0]
                                        if (column_name_outer == column_name_inner): 
                                            # The columns match, so perform the mean calculation and write the output
                                            if ("_total" in column_name_outer): 
                                                counter = unperformed_mean_calculation_inner
                                                summator = unperformed_mean_calculation_outer
                                                column_name = unperformed_mean_calculation_inner.split("_mean_count", 1)[0]
                                            else: 
                                                summator = unperformed_mean_calculation_inner
                                                counter = unperformed_mean_calculation_outer
                                                column_name = unperformed_mean_calculation_outer.split("_mean_count", 1)[0]
                                            
                                            # Remove these columns in the list, so the mean calculation on the same column is not repeated
                                            mean = aggregated_data[summator] / aggregated_data[counter] if aggregated_data[counter] > 0 else 0
                                            data_to_write[column_name + "_mean"] = mean
                                            if unperformed_mean_calculation_outer in unperformed_mean_calculations:
                                                unperformed_mean_calculations.remove(unperformed_mean_calculation_outer)
                                            if unperformed_mean_calculation_inner in unperformed_mean_calculations:
                                                unperformed_mean_calculations.remove(unperformed_mean_calculation_inner)

                                            print("\nHere is the aggregation column name:", str(key))
                                            print("Here is the corresponding aggregation output:", str(value))
                        else: 
                            data_to_write[key] = value
                            print("\nHere is the aggregation column name:", str(key))
                            print("Here is the corresponding aggregation output:", str(value))
    
                    temporary_output_chunk_num += 1
                    csv_writer.writerow(list(data_to_write.keys()))
                    csv_writer.writerow(list(data_to_write.values()))

                print("\nFinished storing the aggregation output to the database.")
            else: 
                print("\nNo aggregation operations identified either.")
                print("Proceeding to the next operator in the query execution plan, the sorting operator.")
        else: 
            print("\nNo aggregation operations identified either.")
            print("Proceeding to the next operator in the query execution plan, the sorting operator.")

        # Show a preview of the temporary output table and proceed to the next operation
        get_first_chunk = getFirstChunkOfTemporaryTable()
        first_chunk = get_first_chunk[0]
        if (first_chunk != "false"): 
            temp_df = get_first_chunk[1]
        else: 
            return ""
        print("\n\nHere is a sample of the table after the aggregation without grouping:")
        print(temp_df.head(5))
    
    # Ordering
    # Check for ordering conditions. If they exist exist, execute the ordering operator.
    if order_by_matches: 
        print("\n\nIn the order by operator.")
        get_first_chunk = getFirstChunkOfTemporaryTable()
        first_chunk = get_first_chunk[0]
        if (first_chunk != "false"): 
            temp_df = get_first_chunk[1]
        else: 
            return ""
        select_df_columns = list(temp_df.columns)
        # Check for and identify all the ordering conditions
        ordering_dict = {}
        print("\nChecking for ordering conditions.")
        for order_by in order_by_attributes: 
            print("Identified the ordering condition:", order_by)
            attribute = order_by[0].strip()
            ordering = order_by[1].strip().lower()
            if (ordering == "ascending"): 
                print("Identified the ordering as ascending for", str(attribute))
                order = True
                ordering_dict[attribute] = order
            elif (ordering == "descending"): 
                print("Identified the ordering as descending for", str(attribute))
                order = False
                ordering_dict[attribute] = order
            else: 
                print("\nCould not identify a valid ordering. Please enter 'ascending' or 'descending' next to the attribute name in any capitalization.")
                print("Exiting, please try again.")
                return ""
        
        # Check if the column(s) on which to order by is/are even valid (i.e. exists in the temporary output table)
        for order_column_name in ordering_dict: 
            if (order_column_name not in select_df_columns):
                print("\n" + str(order_column_name) + " is not a valid column in the current table in the query execution plan.")
                print("Please try again. Here is a list of valid column names. Case sensitive.")
                print(select_df_columns)
                return ""
        
        # If they do exist, sort by reading in the data chunk by chunk
        print("\nOrdering...\n")
        for order_column_name in list(ordering_dict.keys()): 
            # First sort each chunk
            input_csv_files = os.listdir("./Temp-Results")
            temp_csv_file_names = ["./Temp-Results/" + csv_name for csv_name in input_csv_files]
            descending_order = not ordering_dict[attribute]
            for file in temp_csv_file_names:
                print("Sorting chunk named:", str(file))
                sort_csv(str(file), str(file), order_column_name, descending_order)
                print("Finished sorting chunk named:", str(file))
            
            # If no further merging needs to be performed, perform the final pass of merging
            print("\nPerforming the final pass of merging now that the number of sorted runs is less than the number of pages available in main memory.")
            available_ram_gb = psutil.virtual_memory()[1]/1000000000
            memory_size = int(available_ram_gb * 1000)
            sort_and_merge_chunks(temp_csv_file_names, "./Temp-Results/chunk{}.csv", order_column_name, select_df_columns, memory_size, descending_order)
            print("Finished sorting and merging the runs.")

    # Projection
    if select_match: 
        print("\n\nIn the selection/projection operator.")
        select_conditions = str(select_match[1]).split("and")
        print("\nThese are the identified select conditions.")
        print(select_conditions)
        validated_select_conditions = []
        get_first_chunk = getFirstChunkOfTemporaryTable()
        first_chunk = get_first_chunk[0]
        if (first_chunk != "false"): 
            temp_df = get_first_chunk[1]
        else: 
            return ""
        available_select_df_columns = list(temp_df.columns)
        # Loop through the table columns to ensure valid column names were inputted 
        contains_all_columns_project_asterik = "*" in select_conditions
        if contains_all_columns_project_asterik:
            print("\nAll the existing data has been projected.")
            print("Here is the head of the table in the query execution pipeline.")
            get_first_chunk = getFirstChunkOfTemporaryTable()
            first_chunk = get_first_chunk[0]
            if (first_chunk != "false"): 
                temp_df = get_first_chunk[1]
            else: 
                return ""
            print("\n\nThe selection process is complete!")
            print("\nHere is the head of the final output.")
            print(temp_df.head())
            load_entire_table()
            print("\n\nThe select operation is now complete. Hope you enjoyed using the database!")
            print("\nDone with the projection process.")
            return ""

        # If specific columns must be projected
        for select_condition in select_conditions: 
            print("\nThis is the select condition being checked.")
            print(select_condition.strip())

            select_condition = select_condition.strip()

            # Identify and validate the column to be projected first 
            identified_column = ""
            found_column = False
            get_first_chunk = getFirstChunkOfTemporaryTable()
            first_chunk = get_first_chunk[0]
            if (first_chunk != "false"): 
                temp_df = get_first_chunk[1]
            else: 
                return ""
            select_df_columns = list(temp_df.columns)
            for column in select_df_columns: 
                if column in select_condition: 
                    identified_column = column
                    if (identified_column != select_condition):
                        print("\nThe columns are not an exact match, but close enough, so will include it in the projection.")
                        print("The user inputted column is '" + str(select_condition) + "'")
                        print("The identified, similar column in the temporary table in the query execution plan is '" + str(identified_column) + "'")
                    found_column = True
                    validated_select_conditions.append(identified_column.strip())
                    break
                else: 
                    # Try to split the column to identify the appropriate column to project 
                    try: 
                        column_parts = column.split("_")
                        for column_part in column_parts: 
                            if column_part in select_condition:
                                identified_column = column
                                if (identified_column != select_condition):
                                    print("\nThe columns are not an exact match, but close enough, so will include it in the projection.")
                                    print("The user inputted column is '" + str(select_condition) + "'")
                                    print("The identified, similar column in the temporary table in the query execution plan is '" + str(identified_column) + "'")
                                found_column = True
                                validated_select_conditions.append(identified_column.strip())
                                break
                    except: 
                        print()
            
            if not found_column: 
                print("\nNo valid column could be identified in the phrase:\n", select_condition)
                print("These are the valid column names for this table so far. Capitalization does matter for column names (i.e. case-sensitive).")
                print(select_df_columns)
                identified_column = None
                return ""

        print("\nHere is a list of the columns that will be projected in the final output.")
        print(validated_select_conditions)

        # Loop through each chunk and project the selected columns 
        chunk_list = os.listdir("./Temp-Results")
        for index, chunk in enumerate(chunk_list): 
            print("Projecting the data for chunk named:", str(chunk))
            chunk_df = pd.read_csv("./Temp-Results/" + chunk)
            # Convert the temporary table in the query execution plan to a dictionary 
            select_dict = chunk_df.to_dict(orient='records')

            # Create a new dictionary/table with the columns identified for projection
            try:
                selected_data_dict = {column: [row[column] for row in select_dict] for column in validated_select_conditions}
            except: 
                print("\nError in projecting due to an error with the inputted columns.")
                print("Please try again. Here is a list of valid column names. Case sensitive.")
                print(available_select_df_columns)
                return ""
            
            # Remove the read-in chunk from storage 
            os.remove("./Temp-Results/" + chunk)
            # Write the projected data back to storage
            print("\nThe data has been projected.")

            # Convert the dictionary with the projected data for each chunk to a DataFrame
            projected_chunk_df = pd.DataFrame(selected_data_dict)
            # Write the projected data in each chunk back to storage
            projected_chunk_df.to_csv("./Temp-Results/" + chunk, index=False)
            print("\nThe chunk has also been written back to storage.")

            if index == 0: 
                print("\nHere is the head of the table in the query execution pipeline.")
                print(projected_chunk_df.head())    
    else: 
        print("\nNo projection columns have been identified. This is the end of the query execution plan.")
        print("The selection process is complete!")
    
    # Print the final output and exit the operation
    print("\n\nThe selection process is complete!")
    print("\nHere is the final output.")
    load_entire_table()
    print("\n\nThe select operation is now complete. Hope you enjoyed using the database!")
