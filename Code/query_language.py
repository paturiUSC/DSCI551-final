import os 
import pandas as pd
import numpy as np
import re
import psutil
import csv 


outer_table_name = "all_stocks_5yr"
chunk_num = 972
ordering_dict = {}

def construct_sort_key(row):
    return tuple(row[column_name] if ordering_dict[column_name] == 'asc' else -row[column_name] for column_name in ordering_dict)


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
    # Display the example prompts for the user
    # join, aggregate, filter, order, group
    print("\nHere are 3 example prompts to select data in the database. Use a semicolon ';' to end the command.")
    print("Option 1 (filtering, grouping, ordering): find (stock open) IF (stock open greater than - 120) in (chunk0) and make sure to group based by (Name) and show output IN the ORDER OF (open ascending, close descending);")
    print("Option 2 (join): FIND (stock open_x and stock Name) if (stock open_x price greater than - 120 and stock close_x price less than - 140) IN (chunk0) AND in (chunk1) combined via (Name) and make sure to group based by (Name, open_x) and show output IN THE ORDER OF (open_x ascending);")
    print("Option 3 (aggregate): FIND (stock open sum) IN (chunk0) and in (chunk1) combined VIA (Name) and in (chunk2) coMbined ViA (open) and show output in the order of (open descending);")

    print("\nWhen joining tables, include _x after the attribute for the table on the left side of the JOIN and _y after the attribute for the table on the right side of the JOIN. This applies for all attributes except the attributes used to join the tables.")
    print("\nCapitalization of attributes to group by, join on, select, etc. MATTERS!! Other capitalization should not.\n")
    
    print("\nTo group based on an attribute, use the 'BASED ON' keyword. Can only group by at most 2 attributes.")
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
    
    # user_select_input = user_select_input.lower()

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
        # check if there are enough join conditions (1 join attribute per 2 tables)
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
                if table_name_index == 1:
                    join_results = []
                    inner_join_column_names = []

                    # Nested loop join, where the outer relation is the one with all the joined output
                    outer_table = tables_path + table_matches[table_name_index - 1] 
                    table1_chunks = os.listdir(outer_table)
                    for table1_chunk in table1_chunks:
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
                    
                    # Save the joined results in a temporary csv (i.e. "output buffer")
                    with open("temp.csv", 'w', newline='') as temp_join_output_csv:
                        fieldnames = list(join_results[0].keys())
                        writer = csv.DictWriter(temp_join_output_csv, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(join_results)
                    print(join_results)
                else: 
                    # Now use the temp output table as the outer query after joining the first two relations if there are more than two relations that need to be joined
                    join_results = []

                    # Nested loop join, where the outer relation is the one with all the joined output
                    with open("temp.csv", 'r') as outer_relation:
                        outer_reader = csv.DictReader(outer_relation)
                        outer_data = list(outer_reader)
                    
                        inner_table = tables_path + table_matches[table_name_index]
                        table2_chunks = os.listdir(inner_table)
                        for table2_chunk in table2_chunks:
                            with open(inner_table + "/" + table2_chunk, 'r') as inner_table_chunk:
                                inner_reader = csv.DictReader(inner_table_chunk)
                                inner_chunk_data = list(inner_reader)

                                for outer_row in outer_data:
                                    for inner_row in inner_chunk_data:
                                        join_condition_met = all(outer_row[join_col] == inner_row[join_col] for join_col in [combined_via_matches[table_name_index - 1]])
                                        if join_condition_met:
                                            joined_row = {col: outer_row[col] for col in outer_row}
                                            for col in inner_row:
                                                joined_row[col + "_inner" + str(table_name_index - 1)] = inner_row[col]
                                            join_results.append(joined_row)
                                            # joined_row = {**outer_row, **inner_row}
                                            # join_results.append(joined_row)
                
                    # Save the joined results in a temporary csv (i.e. "output buffer")
                    with open("temp.csv", 'w', newline='') as temp_join_output_csv:
                        fieldnames = list(join_results[0].keys())
                        writer = csv.DictWriter(temp_join_output_csv, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(join_results)
                    print(join_results)
        
        # Remove the output buffer (temporary page holding the joined output results)
        select_df = pd.read_csv("temp.csv")
        os.remove("temp.csv")

        print("\n\nHere is the shape of the final table after the joins:")
        print(select_df.shape)
        print("\n\n")
        print(select_df.head())
    else: 
        print("\nNo join conditions identifed. Proceeding to the next operator in the query pipeline.")
        print("Identified only one table entered in the select statement as no join condition with another table was inputted.")
        table_name = table_matches[0]
        print("The identified table name is " + str(table_name) + "\n\n\n")


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

            if "equal to" in filter_condition.lower(): 
                filter_condition_operator = "="
            elif "less than" in filter_condition.lower():
                filter_condition_operator = "<"
            elif "less than or equal to" in filter_condition.lower():
                filter_condition_operator = "<="
            elif "greater than" in filter_condition.lower():
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

            print("\nIdentified", str(filter_condition_attribute), "as the filter condition attribute.")
            print("Identified", str(filter_condition_value), "as the filter condition value.")
            print("Identified", str(filter_condition_operator), "as the filter condition operator.\n")

            if (filter_condition_attribute not in list(select_df.columns)):
                print("\nThe filter condition attribute", str(filter_condition_attribute), "is not a valid attribute to filter on.")
                print("Here is a list of valid attributes to filter on.")
                print(list(select_df.columns))
                return ""
            
            # Loop through the dataframe and filter out the rows 
            select_dict = select_df.to_dict(orient="records")
            
            filtered_select_dict = []
            for row in select_dict: 
                if filter_condition_operator == "=":
                    try:
                        if (filter_condition_value.isdecimal()):
                            filter_condition_value = int(filter_condition_value)
                        else: 
                            filter_condition_value = float(filter_condition_value)
                        print(f"Converted to number: {filter_condition_value}")
                    except:
                        # If it's not a valid integer, try converting to a float
                        print(f"Not converted to number. Kept value as string: {filter_condition_value}")
                    if (row[filter_condition_attribute] == filter_condition_value):
                        filtered_select_dict.append(row)
                elif filter_condition_operator == "<":
                    try:
                        if (filter_condition_value.isdecimal()):
                            filter_condition_value = int(filter_condition_value)
                        else: 
                            filter_condition_value = float(filter_condition_value)
                        print(f"Converted to number: {filter_condition_value}")
                    except:
                        # If it's not a valid integer, try converting to a float
                        print(f"Not converted to number. Kept value as string: {filter_condition_value}")
                    if (row[filter_condition_attribute] < filter_condition_value):
                        filtered_select_dict.append(row)
                elif filter_condition_operator == "<=":
                    try:
                        if (filter_condition_value.isdecimal()):
                            filter_condition_value = int(filter_condition_value)
                        else: 
                            filter_condition_value = float(filter_condition_value)
                        print(f"Converted to number: {filter_condition_value}")
                    except:
                        # If it's not a valid integer, try converting to a float
                        print(f"Not converted to number. Kept value as string: {filter_condition_value}")
                    if (row[filter_condition_attribute] <= filter_condition_value):
                        filtered_select_dict.append(row)
                elif filter_condition_operator == ">":
                    try:
                        if (filter_condition_value.isdecimal()):
                            filter_condition_value = int(filter_condition_value)
                        else: 
                            filter_condition_value = float(filter_condition_value)
                        print(f"Converted to number: {filter_condition_value}")
                    except:
                        # If it's not a valid integer, try converting to a float
                        print(f"Not converted to number. Kept value as string: {filter_condition_value}")
                    if (row[filter_condition_attribute] > filter_condition_value):
                        filtered_select_dict.append(row)
                elif filter_condition_operator == ">=":
                    try:
                        if (filter_condition_value.isdecimal()):
                            filter_condition_value = int(filter_condition_value)
                        else: 
                            filter_condition_value = float(filter_condition_value)
                        print(f"Converted to number: {filter_condition_value}")
                    except:
                        # If it's not a valid integer, try converting to a float
                        print(f"Not converted to number. Kept value as string: {filter_condition_value}")
                    if (row[filter_condition_attribute] >= filter_condition_value):
                        filtered_select_dict.append(row)
                elif filter_condition_operator == "!=":
                    try:
                        if (filter_condition_value.isdecimal()):
                            filter_condition_value = int(filter_condition_value)
                        else: 
                            filter_condition_value = float(filter_condition_value)
                        print(f"Converted to number: {filter_condition_value}")
                    except:
                        # If it's not a valid integer, try converting to a float
                        print(f"Not converted to number. Kept value as string: {filter_condition_value}")
                    if (row[filter_condition_attribute] != filter_condition_value):
                        filtered_select_dict.append(row)
                elif filter_condition_operator == "LIKE":
                    if (str(filter_condition_value) in str(row[filter_condition_attribute])):
                        filtered_select_dict.append(row)
                else: 
                    print("\nNo appropriate filter condition operator found. Please try again. An example operator is 'greater than'")
                    return ""
            
            print("\nDone filtering on the filter condition:", str(filter_condition))
            print("Here is a sample of the new table so far.")
            print(select_df.head())

        print("\nDone with the filtering process. Proceeding to the next operator in the query execution plan, the aggregation operator.")
    else: 
        print("\nNo filtering detected. Proceeding to the next operator in the query execution plan, the aggregation operator.")

    if group_by_matches:
        print("\n\nIn the group by operator.")

        # Check if each of the group by attributes are valid columns in the current output
        for group_by_column in group_by_matches:
            if (group_by_column not in list(select_df.columns)):
                print("\nThe group by attribute", group_by_column, "is not a valid column.")
                print("Here is a list of valid column names to group by.")
                print(list(select_df.columns))
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

            # Identify the column first 
            identified_column = ""
            found_column = False
            select_df_columns = list(select_df.columns)
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
        aggregation_attribute = list(valid_select_conditions.keys())[0]
        name_of_aggregation_operation = valid_select_conditions[aggregation_attribute]
        if (len(list(valid_select_conditions.keys())) == 1):
            group_by_attribute = group_by_matches[0]
            value_attribute = list(valid_select_conditions.keys())[0]
            print("\nGrouping by the singular group by attribute: ", group_by_attribute)
            select_dict = select_df.to_dict(orient="records")
            grouped_data = {}
            for row in select_dict:
                category = row[group_by_attribute]
                value = row[value_attribute]
            
                result_dict = {group_by_attribute: [], name_of_aggregation_operation: []}
                if valid_select_conditions[aggregation_attribute] == "mean":
                    print("\nGrouping by the mean.")
                    if category not in grouped_data:
                        grouped_data[category] = {'Values': [value]}
                    else:
                        grouped_data[category]['Values'].append(value)

                    for category, values in grouped_data.items():
                        result_dict[group_by_attribute].append(category)
                        result_dict[name_of_aggregation_operation].append(sum(values[name_of_aggregation_operation]) / len(values[name_of_aggregation_operation]))
                elif valid_select_conditions[aggregation_attribute] == "sum":
                    print("\nGrouping by the sum.")
                    if category not in grouped_data:
                        grouped_data[category] = {name_of_aggregation_operation: value}
                    else:
                        grouped_data[category][name_of_aggregation_operation] += value
                    print(grouped_data)
                elif valid_select_conditions[aggregation_attribute] == "count":
                    print("\nGrouping by the count.")
                    if category not in grouped_data:
                        grouped_data[category] = {name_of_aggregation_operation: 1}
                    else:
                        grouped_data[category][name_of_aggregation_operation] += 1
                    print(grouped_data)
                elif valid_select_conditions[aggregation_attribute] == "max":
                    print("\nGrouping by the max.")
                    if category not in grouped_data:
                        grouped_data[category] = {name_of_aggregation_operation: value}
                    else:
                        grouped_data[category][name_of_aggregation_operation] = max(grouped_data[category][name_of_aggregation_operation], value)
                    
                    print(grouped_data)
     
                elif valid_select_conditions[aggregation_attribute] == "min":
                    print("\nGrouping by the min.")
                    if category not in grouped_data:
                        grouped_data[category] = {name_of_aggregation_operation: value}
                    else:
                        grouped_data[category][name_of_aggregation_operation] = min(grouped_data[category][name_of_aggregation_operation], value)

            # Now convert the grouped data back to a pdf 
            select_df = pd.DataFrame({group_by_attribute: list(grouped_data.keys()), name_of_aggregation_operation: [data[name_of_aggregation_operation] for data in grouped_data.values()]})
            print(select_df)
        else: 
            print("\nCan only do aggregation via grouping on one singular attribute, not multiple attributes. Please try again.")
            return ""
    else: 
        print("\nNo group by identified. Proceeding to aggregation.")

        # Check for aggregation functions 
        valid_select_conditions = {}
        select_conditions = str(select_match[1]).split("and")
        print(select_conditions)
        # Loop through the table columns to ensure valid column names were inputted 
        for select_condition in select_conditions: 
            print("\nThis is the select condition being dissected to look for an aggregation function.")
            print(select_condition.strip())

            select_condition = select_condition.strip()

            # Identify the column first 
            identified_column = ""
            found_column = False
            select_df_columns = list(select_df.columns)
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
            # else: 
            #     print("\nNo valid aggregation operation identified.")
            #     print("These are the valid aggregation functions:")
            #     print("sum, mean, max, min, count")
            #     return ""

        list_of_aggregation_columns = list(valid_select_conditions.keys())
        if (len(list_of_aggregation_columns) > 0): 
            print("Proceeding to the aggregation as aggregation columns were identified.")
            
            # Check to see if different aggregation operations being performed on the same column
            for aggregation_column in list_of_aggregation_columns: 
                if (list_of_aggregation_columns.count(aggregation_column) > 1): 
                    print("\nThe column", str(aggregation_column), "can only have one aggregation operation performed to it. Please try again.")
                    return ""
            
            # If the checks pass, perform the aggregation
            print("\nAggregating...")
            print("Here is the head of the table currently in the execution plan pipeline.\n\n")
            print(select_df.head())
            select_dict = select_df.to_dict(orient='records')
            aggregated_data = {}

            for aggregation_column in valid_select_conditions: 
                if valid_select_conditions[aggregation_column] == "sum": 
                    aggregated_data[aggregation_column + "_sum"] = sum(row[aggregation_column] for row in select_dict)
                    print("Aggregation of sum complete on the attribute", aggregation_column)
                elif valid_select_conditions[aggregation_column] == "mean": 
                    aggregated_data[aggregation_column + "_mean"] = sum(row[aggregation_column] for row in select_dict) / len(select_dict)
                    print("Aggregation of mean complete on the attribute", aggregation_column)
                elif valid_select_conditions[aggregation_column] == "max": 
                    aggregated_data[aggregation_column + "_max"] = max(row[aggregation_column] for row in select_dict)
                    print("Aggregation of max complete on the attribute", aggregation_column)
                elif valid_select_conditions[aggregation_column] == "min": 
                    aggregated_data[aggregation_column + "_min"] = min(row[aggregation_column] for row in select_dict)
                    print("Aggregation of min complete on the attribute", aggregation_column)
                elif valid_select_conditions[aggregation_column] == "count": 
                    length = 0
                    for row in select_dict: 
                        if pd.notnull(row[aggregation_column]): 
                            length +=1 
                    aggregated_data[aggregation_column + "_count"] = length
                    print("Aggregation of count complete on the attribute", aggregation_column)

            # Convert the aggregated data back to a DataFrame
            select_df = pd.DataFrame(aggregated_data, index=[0])

            print("\nHere is the head of the table currently in the execution plan pipeline after aggregation.\n\n")
            print(select_df.head())

        else: 
            print("\nNo aggregation operations identified either.")
            print("Proceeding to the next operator in the query execution plan, the sorting operator.")

    # Now check for ordering conditions
    # If ordering conditions exist, execute the ordering operator
    if order_by_matches: 
        print("\n\nIn the order by operator.")
        select_df_columns = list(select_df.columns)
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
        
        # Convert the temporary table in the query execution plan to a dictionary
        select_dict = select_df.to_dict(orient='records')

        # Loop through the order conditions 
        # ordering_tuple = ()
        for order_column_name in ordering_dict: 
            # Check if the column even is valid (i.e. exists)
            valid_column_names = list(select_df.columns)
            if (order_column_name not in valid_column_names):
                print("\n" + str(order_column_name) + " is not a valid column in the current table in the query execution plan.")
                print("Please try again. Here is a list of valid column names. Case sensitive.")
                print(valid_column_names)
                return ""

        print("\nOrdering...")
        ordered_data = sorted(select_dict, key=construct_sort_key)
        print("\nThe data has been sorted.")
        select_df = pd.DataFrame(ordered_data)
        print("Here is the head of the table in the query execution pipeline.")
        print(select_df.head())
        print("\nDone with the ordering process. Proceeding to the next operator in the query execution plan, the projection operator.")
        
    if select_match: 
        print("\n\nIn the selection/projection operator.")
        select_conditions = str(select_match[1]).split("and")
        print("\nThese are the identified select conditions.")
        print(select_conditions)
        validated_select_conditions = []
        available_select_df_columns = list(select_df.columns)
        # Loop through the table columns to ensure valid column names were inputted 
        for select_condition in select_conditions: 
            print("\nThis is the select condition being checked.")
            print(select_condition.strip())

            select_condition = select_condition.strip()

            # Identify the column first 
            identified_column = ""
            found_column = False
            select_df_columns = list(select_df.columns)
            for column in select_df_columns: 
                if column in select_condition: 
                    identified_column = column
                    if (identified_column != select_condition):
                        print("\nThe columns are not an exact match, but close enough, so will include it in the projection.")
                        print("The user inputted column is '" + str(select_condition) + "'")
                        print("The identified, similar column in the temporary table in the query execution plan is '" + str(identified_column) + "'")
                    found_column = True
                    validated_select_conditions.append(identified_column.strip())
            
            if not found_column: 
                print("\nNo valid column could be identified in the phrase:\n", select_condition)
                print("These are the valid column names for this table so far. Capitalization does matter for column names (i.e. case-sensitive).")
                print(select_df_columns)
                identified_column = None
                return ""
        
        print("\nHere is a list of the columns that will be projected in the final output.")
        print(validated_select_conditions)

        # Convert the temporary table in the query execution plan to a dictionary 
        select_dict = select_df.to_dict(orient='records')

        # Create a new dictionary/table with the columns identified for projection
        try:
            selected_data_dict = {column: [row[column] for row in select_dict] for column in validated_select_conditions}
        except: 
            print("\nError in projecting due to an error with the inputted columns.")
            print("Please try again. Here is a list of valid column names. Case sensitive.")
            print(available_select_df_columns)
            return ""
        
        print("\nThe data has been projected.")
        select_df = pd.DataFrame(selected_data_dict)
        print("Here is the head of the table in the query execution pipeline.")
        print(select_df.head())

        print("\nDone with the projection process.")
    else: 
        print("\nNo projection columns have been identified. This is the end of the query execution plan.")
    
    print("\n\nThe selection process is complete!")

    print("\nHere is the final output.")
    print(select_df)

    print("\nSaving the output table to the directory named Select-Output-Data/ if you would like to further explore it.")

    output_table_path = "./Select-Output-Data"
    table_name = "/output_results"
    if not os.path.exists(output_table_path):
        os.makedirs(output_table_path)
    
    print("\nSaving...")
    select_df.to_csv(output_table_path + table_name  + ".csv")
    available_ram_gb = psutil.virtual_memory()[1]/1000000000
    print('RAM Available (GB):', available_ram_gb)
    memory_size = int(available_ram_gb * 1000)
    for i, chunk in enumerate(pd.read_csv(output_table_path + table_name + ".csv", chunksize=memory_size)):
        if not os.path.exists(output_table_path + table_name):
            os.makedirs(output_table_path + table_name)
        new_file_name = output_table_path + table_name + '/chunk{}.csv'.format(i)
        chunk.to_csv(new_file_name, index=False)
    os.remove(output_table_path + table_name + ".csv")
    print("Saved.")
    print("\n\nThe select operation is now complete. Hope you enjoyed using the database!")

        # If two group by conditions identified
        # elif (len(list(valid_select_conditions.keys())) > 1):
        #     group_by_attribute = group_by_matches[0]
        #     sub_group_by_attribute = group_by_matches[1]
        #     value_attribute = list(valid_select_conditions.keys())[0]
        #     sub_value_attribute = list(valid_select_conditions.keys())[1]
        #     print("\nGrouping by the two group by attributes:", str(group_by_matches))
        #     select_dict = select_df.to_dict(orient="records")

        #     grouped_data = {}
        #     for row in select_dict:
        #         category = row[group_by_attribute]
        #         subcategory = row[sub_group_by_attribute]
        #         value = row[value_attribute]

        #         category_key = (category,)
        #         subcategory_key = (category, subcategory)

        #         if valid_select_conditions[aggregation_attribute] == "mean":
        #             print("\nOuter Grouping by the mean.")



        #         elif valid_select_conditions[aggregation_attribute] == "sum":
        #             print("\nGrouping by the sum.")
        #             if category not in grouped_data:
        #                 grouped_data[category] = {name_of_aggregation_operation: value}
        #             else:
        #                 grouped_data[category][name_of_aggregation_operation] += value
        #             print(grouped_data)
        #         elif valid_select_conditions[aggregation_attribute] == "count":
        #             print("\nGrouping by the count.")
        #             if category not in grouped_data:
        #                 grouped_data[category] = {name_of_aggregation_operation: 1}
        #             else:
        #                 grouped_data[category][name_of_aggregation_operation] += 1
        #             print(grouped_data)
        #         elif valid_select_conditions[aggregation_attribute] == "max":
        #             print("\nGrouping by the max.")
        #             if category not in grouped_data:
        #                 grouped_data[category] = {name_of_aggregation_operation: value}
        #             else:
        #                 grouped_data[category][name_of_aggregation_operation] = max(grouped_data[category][name_of_aggregation_operation], value)
                    
        #             print(grouped_data)
     
        #         elif valid_select_conditions[aggregation_attribute] == "min":
        #             print("\nGrouping by the min.")
        #             if category not in grouped_data:
        #                 grouped_data[category] = {name_of_aggregation_operation: value}
        #             else:
        #                 grouped_data[category][name_of_aggregation_operation] = min(grouped_data[category][name_of_aggregation_operation], value)









    # # Group By On The table
    # # Check if there is group by 
    # select_df_columns = [column for column in list(select_df.columns)]
    # verified_group_by_columns = []
    # column_and_aggregation = {}
    # print("\nTesting")
    # print(group_by_matches)
    # print(type(group_by_matches))
    # if group_by_matches: 
    #     group_by_matches = group_by_matches[0]
    #     print(group_by_matches)
    #     # Need to split the different group by attributes if there are multiple
    #     if "," in group_by_matches: 
    #         print("split")
    #         group_by_matches = group_by_matches.split(",")
    #     else: 
    #         group_by_matches = [group_by_matches]

    #     for group_by in group_by_matches:
    #         print("\n\nIdentifed group by attribute:\n", group_by)
    #         group_by = group_by.strip()
    #         if group_by in select_df_columns: 
    #             print("This attribute", group_by, "is a valid column in the table.")
    #             verified_group_by_columns.append(group_by.strip())
    #         else: 
    #             print("This attribute", group_by, "is not a valid column in the table.")
    #             print("Exiting. Please enter a valid attribute name.")
    #             print("Here is a list of valid attriubtes to group on:")
    #             print(select_df_columns)
    #             return ""
            
    #     print("\nThere must be a valid aggregation function accompanying the group by.")
    #     print("Checking for an aggregation function.")
    #     # Check for the aggregation function in the SELECT/projection condition
    #     select_conditions = str(select_match[1]).split("and")
    #     print(select_conditions)
    #     # Loop through the table columns to ensure valid column names were inputted 
    #     for select_condition in select_conditions: 
    #         print("\nThis is the select condition being dissected to look for an aggregation function.")
    #         print(select_condition.strip())

    #         column_and_operation_phrase = select_condition.strip()

    #         # Identify the appropriate column
    #         identified_column = ""
    #         found_column = False
    #         for column in select_df_columns: 
    #             if column in column_and_operation_phrase: 
    #                 identified_column = column
    #                 found_column = True
            
    #         if not found_column: 
    #             print("\nNo valid column could be identified in the phrase:\n", column_and_operation_phrase)
    #             print("These are the valid column names for this table so far. Capitalization does matter for column names.")
    #             print(select_df_columns)
    #             identified_column = None
    #             return ""
            
    #         column_and_operation_phrase = column_and_operation_phrase.lower()
    #         if "sum" in column_and_operation_phrase: 
    #             print("\nAggregating attribute", identified_column , "via sum")
    #             column_and_aggregation[identified_column] = "sum"
    #         elif "mean" in column_and_operation_phrase:
    #             print("\nAggregating attribute", identified_column , "via mean or average")
    #             column_and_aggregation[identified_column] = "mean"
    #         elif "max" in column_and_operation_phrase:
    #             print("\nAggregating attribute", identified_column , "via max")
    #             column_and_aggregation[identified_column] = "max"
    #         elif "min" in column_and_operation_phrase:
    #             print("\nAggregating attribute", identified_column , "via min")
    #             column_and_aggregation[identified_column] = "min"
    #         elif "std" in column_and_operation_phrase:
    #             print("\nAggregating attribute", identified_column , "via std")
    #             column_and_aggregation[identified_column] = "std"
    #         elif "var" in column_and_operation_phrase:
    #             print("\nAggregating attribute", identified_column , "via var")
    #             column_and_aggregation[identified_column] = "var"
    #         elif "count" in column_and_operation_phrase:
    #             print("\nAggregating attribute", identified_column , "via count")
    #             column_and_aggregation[identified_column] = "count"
    #         else: 
    #             print("\nNo valid aggregation operation identified.")
    #             print("\nNo valid aggregation operation identified.")
    #             print("These are the valid aggregation functions:")
    #             print("sum, mean, max, min, std, var, count")
    #             return ""
        
    #     # Once aggregation check passes, different group by if there is one group by or multiple group by attributes 
    #     column_and_aggregation_str = ', '.join(f"'{key}': {value}" if isinstance(value, list) else f"'{key}': '{value}'" for key, value in column_and_aggregation.items())
    #     # single group by attribute
    #     if (len(verified_group_by_columns) == 1): 
    #         print("\nGrouping and aggregating.")
    #         print("Here is the statement.")
    #         print(f"select_df = select_df.groupby('{verified_group_by_columns[0]}').agg({{{column_and_aggregation_str}}})")
    #         try:
    #             print()
    #             select_df = select_df.groupby(verified_group_by_columns[0]).agg(column_and_aggregation)
    #             print("\nGroup By Complete.")
    #             print("Here is the output of the group by.")
    #             print(select_df)
    #         except: 
    #             print("There was an error grouping. Please check capitalizations and valid attributes and aggregations please.")
    #             print("Exiting, please try again.")
    #             return ""

    #     # multiple group by attributes
    #     else: 
    #         print("\nGrouping and aggregating.")
    #         print("Here is the statement.")
    #         print(f"select_df = select_df.groupby('{verified_group_by_columns}').agg({{{column_and_aggregation_str}}})")
    #         try: 
    #             print(column_and_aggregation_str)
    #             print()
    #             select_df.groupby(verified_group_by_columns).agg(column_and_aggregation)
    #             print("\nGroup By Complete.")
    #             print("Here is the output of the group by.")
    #             print(select_df)
    #         except: 
    #             print("There was an error grouping. Please check capitalizations and valid attributes and aggregations please.")
    #             print("Exiting, please try again.")
    #             return ""
    # # Non Group By Aggregation 
    # # If there is no group by, need to check if there is still aggregation 
    # else: 
    #     print("\nNo GROUP BY identified. Moving onto aggregation.")
    #     # If there is no group by, aggregate on columns 
    #     column_and_aggregation = {}
    #     print("\nChecking for an aggregation function.")
    #     # Check for the aggregation function in the SELECT/projection condition
    #     select_conditions = str(select_match[1]).split("and")
    #     print(select_conditions)
    #     # Loop through the table columns to ensure valid column names were inputted 
    #     for select_condition in select_conditions: 
    #         print("\nThis is the select condition being dissected to look for an aggregation function.")
    #         print(select_condition.strip())

    #         column_and_operation_phrase = select_condition.strip()

    #         # Identify the appropriate column
    #         identified_column = ""
    #         found_column = False
    #         for column in select_df_columns: 
    #             if column in column_and_operation_phrase: 
    #                 identified_column = column
    #                 found_column = True
            
    #         if not found_column: 
    #             print("\nNo valid column could be identified in the phrase:\n", column_and_operation_phrase)
    #             print("These are the valid column names for this table so far. Capitalization does matter for column names.")
    #             print(select_df_columns)
    #             identified_column = None
    #             return ""
            
    #         column_and_operation_phrase = column_and_operation_phrase.lower()
    #         if "sum" in column_and_operation_phrase: 
    #             print("\nAggregating attribute", identified_column , "via sum")
    #             column_and_aggregation[identified_column] = "sum"
    #         elif "mean" in column_and_operation_phrase:
    #             print("\nAggregating attribute", identified_column , "via mean or average")
    #             column_and_aggregation[identified_column] = "mean"
    #         elif "max" in column_and_operation_phrase:
    #             print("\nAggregating attribute", identified_column , "via max")
    #             column_and_aggregation[identified_column] = "max"
    #         elif "min" in column_and_operation_phrase:
    #             print("\nAggregating attribute", identified_column , "via min")
    #             column_and_aggregation[identified_column] = "min"
    #         elif "std" in column_and_operation_phrase:
    #             print("\nAggregating attribute", identified_column , "via std")
    #             column_and_aggregation[identified_column] = "std"
    #         elif "var" in column_and_operation_phrase:
    #             print("\nAggregating attribute", identified_column , "via var")
    #             column_and_aggregation[identified_column] = "var"
    #         elif "count" in column_and_operation_phrase:
    #             print("\nAggregating attribute", identified_column , "via count")
    #             column_and_aggregation[identified_column] = "count"
    #         else: 
    #             print("\nNo valid aggregation operation identified.")
    #             print("\nNo valid aggregation operation identified.")
    #             print("These are the valid aggregation functions:")
    #             print("sum, mean, max, min, std, var, count")
    #             return ""
            
    #     print("\nAggregating.")
    #     print("Here is the statement.")
    #     print(f"select_df = select_df.agg({{{column_and_aggregation}}})")
    #     try:
    #         print()
    #         select_df = select_df.agg(column_and_aggregation)
    #         print("\nAggregation Complete.")
    #         print("Here is the output of the aggregation.")
    #         print(select_df)
    #     except: 
    #         print("\nThere was an error with the aggregation. Please check capitalizations and aggregation-attribute pairings please.")
    #         print("You can only use aggregations like sum on numerical attributes.")
    #         print("Exiting, please try again.")
    #         return ""
    
    # print("\nGrouping and Aggregation Complete")

    # # Ordering
    # select_df_columns = list(select_df.columns)
    # ordering_dict = {}
    # if order_by_matches: 
    #     print("\n\n\nChecking for ordering conditions.")
    #     for order_by in order_by_matches: 
    #         print("Identified the ordering condition:\n", order_by)
    #         order_by = order_by.split(" ")
    #         attribute = order_by[0].strip()
    #         ordering = order_by[1].strip().lower()
    #         print(attribute)
    #         print(ordering)
    #         if (ordering == "ascending"): 
    #             print("Identified the ordering as ascending.")
    #             order = True
    #         elif (ordering == "descending"): 
    #             print("Identified the ordering as descending.")
    #             order = False
    #         else: 
    #             print("\nCould not identify a valid ordering. Please enter 'ascending' or 'descending' next to the attribute name in any capitalization.")
    #             print("Exiting, please try again.")
    #             order = True
    #             return ""

    #         print("The attribute identified is:\n", attribute)
    #         identified_column = ""
    #         found_column = False
    #         for column in select_df_columns: 
    #             if column in column_and_operation_phrase: 
    #                 identified_column = column
    #                 found_column = True        

    #         if not found_column: 
    #             print("\nNo valid column could be identified in the phrase:\n", column_and_operation_phrase)
    #             print("These are the valid column names for this table so far. Capitalization does matter for column names.")
    #             print(select_df_columns)
    #             identified_column = None
    #             return ""

    #         ordering_dict[attribute] = order

    #     print("\nOrdering.")
    #     print("Here is the statement.")
    #     # sort_values differs for ordering by a single and multiple columns 
    #     if (len(ordering_dict) == 1):
    #         print(f"select_df = select_df.sort_values(by='{attribute}', ascending={ordering_dict[attribute]})")
    #         try:
    #             print()
    #             print(attribute)
    #             print(bool(ordering_dict[attribute]))
    #             select_df = select_df.sort_values(by=attribute, ascending=bool(ordering_dict[attribute]))
    #             print("\nOrdering Complete.")
    #             print("Here is the output of the ordering.")
    #             print(select_df)
    #         except: 
    #             try: 
    #                 select_df = select_df.sort_values(ascending=bool(ordering_dict[attribute]))
    #                 print("\nOrdering Complete.")
    #                 print("Here is the output of the ordering.")
    #                 print(select_df)
    #             except:
    #                 print("\nThere was an error with the ordering. Please check capitalizations and ascending/descending please.")
    #                 print("\nOne potential error could also be that there are too many order by attributes relative to the columns after the filtering, grouping, and aggregation.")
    #                 print("Exiting, please try again.")
    #                 return ""
    #     else: 
    #         ordering_keys = list(ordering_dict.keys())
    #         ordering_values = list(ordering_dict.values())
    #         print(f"select_df = select_df.sort_values(by={ordering_keys}, ascending={ordering_values})")
    #         try:
    #             print()
    #             select_df = select_df.sort_values(by=ordering_keys, ascending=ordering_values)
    #             print("\nOrdering Complete.")
    #             print("Here is the output of the ordering.")
    #             print(select_df)
    #         except: 
    #             try:
    #                 print()
    #                 select_df = select_df.sort_values(by=ordering_keys, ascending=ordering_values)
    #                 print("\nOrdering Complete.")
    #                 print("Here is the output of the ordering.")
    #                 print(select_df)
    #             except: 
    #                 print("\nThere was an error with the ordering. Please check capitalizations and ascending/descending please.")
    #                 print("\nOne potential error could also be that there are too many order by attributes relative to the columns after the filtering, grouping, and aggregation.")
    #                 print("There are", len(ordering_keys), "attributes to order by.")
    #                 print("However, the table only has", len(list(select_df.columns)), "columns.")
    #                 print("Exiting, please try again.")
    #                 return ""

    # # Projection
    # select_df_columns = list(select_df.columns)
    # if select_match: 
    #     print("\n\n\nProjecting.")

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
    # if (a < 5) update test_table VALUES (a = 50);
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
    
    # Print the results
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
        
        # Update the table
        if df_filter_expression is None:
            print("\nUpdating table " + table_name + "...")

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

        elif df_filter_expression is not None and update_condition:  # if (a < 9) update test_table VALUES (a = 50);
            # Now that the filter condition has been constructured, identify all the appropriate rows in the corresponding table/DataFrame
            # Show update output to user
            print("\nThis is the identified filter expression.")
            print("file_df[" + str_filter_expression + "]")

            print("\nThese are the identified rows:")
            total_rows = 0
            for chunk in table_chunks: 
                file_df = pd.read_csv(table_path + "/" + chunk)
                total_rows += len(file_df[df_filter_expression])

            print("\nThere are", str(total_rows), "rows that have been identified in table", str(table_name), "to be updated.")
            if total_rows > 0:
                print("\nUpdating...")
                for index in range(len(column_names_to_update)):
                    column_name = column_names_to_update[index]
                    new_column_value = column_values_to_update[index]
                    for chunk in table_chunks: 
                        file_df = pd.read_csv(table_path + "/" + chunk)
                        file_df.loc[df_filter_expression.reset_index(drop=True), column_name] = new_column_value
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
    
    user_insert_input = user_insert_input.lower()

    if not os.path.exists("./Output-Data"):
        os.makedirs("./Output-Data")

    # Create a new table
    if " table " in user_insert_input: 
        pattern = r"Insert\s+table\s+(\w+)\s*\((.*?)\)"

        # Match the pattern to the user inpitted command
        match = re.match(pattern, user_insert_input, re.IGNORECASE)

        if match:
            # Find the name of the newly created table
            table_name = match.group(1)  

            # Split the column headers
            column_info = {}
            parameters_str = match.group(2)  
            parameters = [param.strip() for param in parameters_str.split(',')]
            for parameter in parameters: 
                column_info_list = parameter.split()
                column_info[column_info_list[0]] = column_info_list[1]

            # Show output to user 
            print("Here is the extracted table name", table_name)
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
            print("The table", table_name, "has been created and inserted into the database.")
            print("Here is the file path of the newly created table", new_table_file_name)

        else: 
            print("The table name and/or parameters could not be identified.")
            print("Please enter a valid insert statement similar to the provided examples.")

    # Insert a row of data 
    elif "row" in user_insert_input: 

        user_insert_input = user_insert_input.lower()
        # pattern = r"insert\s+row\s+in\s+(\w+)\s+\((.*?)\)\s*values\s*\((.*?)\);"

        # Match the pattern to the user inputted command
        # match = re.match(pattern, user_insert_input, re.IGNORECASE)
        match = True

        if match:
            # # Get the table name
            # table_name = match.group(1)  
            # # Get the column names and values
            # column_names_str = match.group(2)  
            # column_values_str = match.group(3)  
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
            column_values_str = user_insert_input[second_open_parenthesis_index + 1 : second_close_parenthesis_index]

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

                # file_df = pd.read_csv(table_path)
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
                            new_data[table_column] = np.nan
                            print("Added null value for column", table_column)
                    
                    # Add the new data to the existing table
                    new_data_df = pd.DataFrame([new_data])
                    
                    placeholder_value = -99999
                    data_types_dict = {col: dtype for col, dtype in file_df.dtypes.items()}
                    for col in new_data_df.columns:
                        if col in data_types_dict.keys(): 
                            new_data_df[col] = new_data_df[col].fillna(placeholder_value).astype(data_types_dict[col]).where(new_data_df[col].notna(), np.nan)
                        
                    print("Here is the info of the data being added:")
                    print(new_data_df.info())

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

def deleteData(): 
    # Display the example prompts for the user
    print("\nHere are 3 example prompts to delete data from the database. Use a semicolon ';' to end the command.")
    print("Option 1 (deleting all data from user-generated and pre-partitioned tables): Delete all;")
    print("Option 2 (deleting specific rows based on a comparison/rule AND make sure column name is right before the comparison): Delete rows where stock_price > than 50;")
    print("For Option 2, make sure to type out the comparison explicity: '=', '>', '>=', '<',  '<=', or 'like' (for strings).")
    print("Option 3 (deleting user-generated table): Delete all from table_name;\n")

    print("\nExit out at anytime by typing 'exit;'")

    # Get and validate user input
    user_delete_input = input(">")

    while (not user_delete_input) or (user_delete_input[-1:] != ";"): 
        user_delete_input = input(">")
    
    if ("exit" in user_delete_input):
        return ""
    
    user_delete_input = user_delete_input.lower()

    if not os.path.exists("./Output-Data"):
            os.makedirs("./Output-Data")

    directory_data_directories = os.listdir("./Output-Data")
    directory_data_files = []
    for directory in directory_data_directories:
        for file in os.listdir("./Output-Data/" + directory):
            directory_data_files.append("./Output-Data/" + directory + "/" + file)

    if len(directory_data_files) == 0:
        print("No data in the database right now.")
        return ""

    # Delete all data
    if "all" in user_delete_input and "from" not in user_delete_input: 
        for file in directory_data_files: 
            file_path = "./Output-Data/" + file
            print("Deleted file", file_path)
            os.remove(file_path)
        print("Deleted all data!")
    
    # Delete all data from a specific table
    elif "all" in user_delete_input:
        table_name = input(">")
        while not os.path.exists("./Output-Data/" + table_name): 
            table_name = input("The entered table does not exist. Please enter a valid table name: ")
        file_path = "./Output-Data/" + table_name
        table_files = os.listdir(file_path)
        for file in table_files:
            os.remove(file_path + "/" + file)

        print("Deleted all data from table", table_name)

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

        # Apply delete input on each file/table in the database
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
                
                print(f"These are the identified key tokens from the command:'{column}' {comparison_phrase} '{value}'")

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
    # column_headers = ['date', 'open', 'high', 'low', 'close', 'volume', 'Name'] 

    # See if the user wants to use the database
    use_database_input = useDatabase()

    # Get user input on what operation the user wants to perform on the database
    while use_database_input:
        menu_option = displayUserMenu()

        menu_option_action(menu_option)

        use_database_input = useDatabase()

    print("\nThank you for using the database!")



'''
an example query prompt will be displayed to the user. For instance, if the user chooses to insert data through the command line interface, the function insertData() will be called, and an example prompt such as “Insert the data stored in location extra_data.csv" will be displayed. Then, the user can input a similar prompt to insert additional data. This will be accomplished through the same methods used to process and partition the original input data. 
'''

'''
Include an option to exit at anytime by typing 'exit' within the select/insert/update/delete data functions
'''