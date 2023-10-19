import os 
import pandas as pd
import numpy as np
import re


# to - do 
    # select 
        # Add * for projection
        # Join where the column name on the left and column name on the right could be different and join on multiple attributes
    # insert/delete
        # remove/insert into whole dataset or partitioned tables 
    # insert 
        # only insert into partition if it's number of rows is less than the memory size
            # if it is more than the memory size, create a new partition based on the partition 
        # int to float dtype conversion automatically when inserting null values in pandas
    

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
    
    print("\nTo group based on an attribute, use the 'BASED ON' keyword.")
    print("\nTo join, use the 'AND' and  'ON' keyword.")
    print("\nTo order by, use the 'WITH' keyword.")
    print("\nTo filter, use the 'IF' keyword.")
    print("\nThere are multiple different aggregation options: sum, average, maximum, mininum, std, var, count. These must be typed out in full after the name of the attribute you are looking for. For example, 'FIND (stock high maximum)'.")
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
        table_name = str(table_name).lower()
        table_path = tables_path + table_name + ".csv"
        if not os.path.exists(table_path):
            print("Table", str(table_name), "is not a table in the database.")
            print("Please input a valid table name.")
            valid_tables = False
    
    if not valid_tables: 
        print("\nExited the select operation as valid table name(s) were not inputted.")
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
        for join_condition_attribute in combined_via_matches: 
            print()
            for table_name in table_matches: 
                tables_path = "./Output-Data/"
                table_name = str(table_name).lower()
                table_path = tables_path + table_name + ".csv"
                file_df = pd.read_csv(table_path)
                join_condition_attribute = str(join_condition_attribute).lower()
                file_df_columns = [column.lower() for column in list(file_df.columns)]
                if join_condition_attribute not in file_df_columns: 
                    print(str(join_condition_attribute), "is not an attribute in table", str(table_name))
                    join_valid = False 
            
        if not join_valid: 
            print("The join could not be performed.")
            return ""

        # Join the tables
        # for join_condition_attribute in combined_via_matches: 
        tables_path = "./Output-Data/"
        table_name = (table_matches[0]).lower()
        table_path = tables_path + str(table_name) + ".csv"
        print("Getting table", str(table_name), "initially...")
        select_df = pd.read_csv(table_path)

        if len(table_matches) > 1:
            for table_name_index in range(1, len(table_matches)): 
                table_name = (table_matches[table_name_index]).lower()
                table_path = tables_path + table_name + ".csv"
                second_select_df = pd.read_csv(table_path)
                print("\nJoining table", table_name + "...")
                try: 
                    join_attribute = str(combined_via_matches[table_name_index - 1])
                    select_df = pd.merge(select_df, second_select_df, on=join_attribute)
                    print("Merging on the attribute", join_attribute)
                    print("Here is the shape of the new table thus far.")
                    print(select_df.shape)
                    print("The join is complete between the 2 tables...")
                except: 
                    print("The join failed.")
                    print("The error occured when joining on the attribute", combined_via_matches[table_name_index - 1], "and with the table", table_matches[table_name_index])
        print("\nHere is the shape of the final table after the joins:")
        print(select_df.shape)
        print(select_df.head())
    else: 
        tables_path = "./Output-Data/"
        table_name = (table_matches[0]).lower()
        table_path = tables_path + str(table_name) + ".csv"
        select_df = pd.read_csv(table_path)

    # Filter the table
    select_df_columns = [column.lower() for column in list(select_df.columns)]
    filter_expression = None
    str_filter_expression = None
    if filter_match: 
        filter_conditions = str(filter_match[1]).split("and")
        print(filter_conditions)
        # Loop through the table columns to ensure valid column names were inputted 
        for filter_condition in filter_conditions: 
            print("\nThis is the filter condition being dissected.")
            print(filter_condition.strip())

            filter_condition_components = filter_condition.strip().split("-")

            # The select condition must have a column name, a comparison phrase, and a value 
            # The value should be after the "-", hence why the condition should always be split into two 
            if (len(filter_condition_components) == 2):

                column_and_operation_phrase = filter_condition_components[0].strip()
                value = filter_condition_components[1].strip()

                # Identify the appropriate column
                identified_column = ""
                found_column = False
                for column in select_df_columns: 
                    if column in column_and_operation_phrase: 
                        identified_column = column
                        found_column = True
                
                if not found_column: 
                    print("\nNo valid column could be identified in the phrase:\n", column_and_operation_phrase)
                    print("These are the valid column names for this table so far. Capitalization does matter for column names.")
                    print(select_df_columns)
                    identified_column = None
                    return ""


                # Identify the appropriate comparison operator and build the filter expression
                column_and_operation_phrase = str(column_and_operation_phrase).lower()
                if "equal to" in column_and_operation_phrase:
                    value = float(value)
                    condition = (select_df[identified_column] == value)
                    if str_filter_expression is None:
                            str_filter_expression = "(select_df[" + str(identified_column) + "] == " + str(value) + ")"
                    else: 
                        str_filter_expression += "& (select_df[" + str(identified_column) + "] == " + str(value) + ")"
                elif "less than" in column_and_operation_phrase:
                    value = float(value)
                    condition = (select_df[identified_column] < value)
                    if str_filter_expression is None:
                            str_filter_expression = "(select_df[" + str(identified_column) + "] < " + str(value) + ")"
                    else: 
                        str_filter_expression += "& (select_df[" + str(identified_column) + "] < " + str(value) + ")"
                elif "greater than" in column_and_operation_phrase:
                    value = float(value)
                    condition = (select_df[identified_column] > value)
                    if str_filter_expression is None:
                            str_filter_expression = "(select_df[" + str(identified_column) + "] > " + str(value) + ")"
                    else: 
                        str_filter_expression += "& (select_df[" + str(identified_column) + "] > " + str(value) + ")"
                elif "less than or equal to" in column_and_operation_phrase:
                    value = float(value)
                    condition = (select_df[identified_column] <= value)
                    if str_filter_expression is None:
                            str_filter_expression = "(select_df[" + str(identified_column) + "] <= " + str(value) + ")"
                    else: 
                        str_filter_expression += "& (select_df[" + str(identified_column) + "] <= " + str(value) + ")"
                elif "greater than or equal to" in column_and_operation_phrase:
                    value = float(value)
                    condition = (select_df[identified_column] >= value)
                    if str_filter_expression is None:
                            str_filter_expression = "(select_df[" + str(identified_column) + "] >= " + str(value) + ")"
                    else: 
                        str_filter_expression += "& (select_df[" + str(identified_column) + "] >= " + str(value) + ")"
                elif "like" in column_and_operation_phrase:
                    condition = select_df[identified_column].str.contains(str(value), case=False)
                    if str_filter_expression is None:
                            str_filter_expression = "(select_df[" + str(identified_column) + "] LIKE " + str(value) + ")"
                    else: 
                        str_filter_expression += "& (select_df[" + str(identified_column) + "] LIKE " + str(value) + ")"
                else: 
                    condition = None
                    print("\nNo valid comparison phrase, like 'greater than' or 'like', could be identified in the select phrase.")
                    print("Please try again.")
                    print("Exiting the select operation.")
                    return ""
                    
                # Combine the filter expression conditions using logical 'and'
                if filter_expression is None:
                    filter_expression = condition
                else:
                    filter_expression = filter_expression & condition
                print("\nThe filter expression has been constructed.")
                print(filter_expression)
                print("\nThis is the filter expression:\n" + str_filter_expression)

            # If the condition is not split into two (column name + comparison phrase and the value)
            else: 
                print("\nThis select statement was not able to be dissected as it does not contain the key components of a column name, comparison phrase, and a value:")
                print(filter_condition)
                print("Thus, exiting the select operation. Please try again.")
                return ""
            
        # Filter the table
        if filter_expression is not None:
            print("\nThese are the identified rows:")
            print(select_df[filter_expression])
            print("There are", str(len(select_df[filter_expression])), "rows that have been identified in the table.")

            print("\nFiltering...")
            print("\n" + str_filter_expression)
            try: 
                select_df = select_df[filter_expression]
                print("Filtered!")
                print("The table now has", len(select_df) ,"rows")
                # If the table has 0 rows, there is no purpose in continuing to aggregations, grouping, ordering, and projection
                if (len(select_df) == 0): 
                    return "\nThe table has 0 rows, so cannot proceed further in the select operation."
            except: 
                print("There was an error filtering. Please try again.")
        else:
            print("No valid filter expression could be constructured from the inputted select command.")


    # Group By On The table
    # Check if there is group by 
    select_df_columns = [column for column in list(select_df.columns)]
    verified_group_by_columns = []
    column_and_aggregation = {}
    print("\nTesting")
    print(group_by_matches)
    print(type(group_by_matches))
    if group_by_matches: 
        group_by_matches = group_by_matches[0]
        print(group_by_matches)
        # Need to split the different group by attributes if there are multiple
        if "," in group_by_matches: 
            print("split")
            group_by_matches = group_by_matches.split(",")
        else: 
            group_by_matches = [group_by_matches]

        for group_by in group_by_matches:
            print("\n\nIdentifed group by attribute:\n", group_by)
            group_by = group_by.strip()
            if group_by in select_df_columns: 
                print("This attribute", group_by, "is a valid column in the table.")
                verified_group_by_columns.append(group_by.strip())
            else: 
                print("This attribute", group_by, "is not a valid column in the table.")
                print("Exiting. Please enter a valid attribute name.")
                print("Here is a list of valid attriubtes to group on:")
                print(select_df_columns)
                return ""
            
        print("\nThere must be a valid aggregation function accompanying the group by.")
        print("Checking for an aggregation function.")
        # Check for the aggregation function in the SELECT/projection condition
        select_conditions = str(select_match[1]).split("and")
        print(select_conditions)
        # Loop through the table columns to ensure valid column names were inputted 
        for select_condition in select_conditions: 
            print("\nThis is the select condition being dissected to look for an aggregation function.")
            print(select_condition.strip())

            column_and_operation_phrase = select_condition.strip()

            # Identify the appropriate column
            identified_column = ""
            found_column = False
            for column in select_df_columns: 
                if column in column_and_operation_phrase: 
                    identified_column = column
                    found_column = True
            
            if not found_column: 
                print("\nNo valid column could be identified in the phrase:\n", column_and_operation_phrase)
                print("These are the valid column names for this table so far. Capitalization does matter for column names.")
                print(select_df_columns)
                identified_column = None
                return ""
            
            column_and_operation_phrase = column_and_operation_phrase.lower()
            if "sum" in column_and_operation_phrase: 
                print("\nAggregating attribute", identified_column , "via sum")
                column_and_aggregation[identified_column] = "sum"
            elif "mean" in column_and_operation_phrase:
                print("\nAggregating attribute", identified_column , "via mean or average")
                column_and_aggregation[identified_column] = "mean"
            elif "max" in column_and_operation_phrase:
                print("\nAggregating attribute", identified_column , "via max")
                column_and_aggregation[identified_column] = "max"
            elif "min" in column_and_operation_phrase:
                print("\nAggregating attribute", identified_column , "via min")
                column_and_aggregation[identified_column] = "min"
            elif "std" in column_and_operation_phrase:
                print("\nAggregating attribute", identified_column , "via std")
                column_and_aggregation[identified_column] = "std"
            elif "var" in column_and_operation_phrase:
                print("\nAggregating attribute", identified_column , "via var")
                column_and_aggregation[identified_column] = "var"
            elif "count" in column_and_operation_phrase:
                print("\nAggregating attribute", identified_column , "via count")
                column_and_aggregation[identified_column] = "count"
            else: 
                print("\nNo valid aggregation operation identified.")
                print("\nNo valid aggregation operation identified.")
                print("These are the valid aggregation functions:")
                print("sum, mean, max, min, std, var, count")
                return ""
        
        # Once aggregation check passes, different group by if there is one group by or multiple group by attributes 
        column_and_aggregation_str = ', '.join(f"'{key}': {value}" if isinstance(value, list) else f"'{key}': '{value}'" for key, value in column_and_aggregation.items())
        # single group by attribute
        if (len(verified_group_by_columns) == 1): 
            print("\nGrouping and aggregating.")
            print("Here is the statement.")
            print(f"select_df = select_df.groupby('{verified_group_by_columns[0]}').agg({{{column_and_aggregation_str}}})")
            try:
                print()
                select_df = select_df.groupby(verified_group_by_columns[0]).agg(column_and_aggregation)
                print("\nGroup By Complete.")
                print("Here is the output of the group by.")
                print(select_df)
            except: 
                print("There was an error grouping. Please check capitalizations and valid attributes and aggregations please.")
                print("Exiting, please try again.")
                return ""

        # multiple group by attributes
        else: 
            print("\nGrouping and aggregating.")
            print("Here is the statement.")
            print(f"select_df = select_df.groupby('{verified_group_by_columns}').agg({{{column_and_aggregation_str}}})")
            try: 
                print(column_and_aggregation_str)
                print()
                select_df.groupby(verified_group_by_columns).agg(column_and_aggregation)
                print("\nGroup By Complete.")
                print("Here is the output of the group by.")
                print(select_df)
            except: 
                print("There was an error grouping. Please check capitalizations and valid attributes and aggregations please.")
                print("Exiting, please try again.")
                return ""
    # Non Group By Aggregation 
    # If there is no group by, need to check if there is still aggregation 
    else: 
        print("\nNo GROUP BY identified. Moving onto aggregation.")
        # If there is no group by, aggregate on columns 
        column_and_aggregation = {}
        print("\nChecking for an aggregation function.")
        # Check for the aggregation function in the SELECT/projection condition
        select_conditions = str(select_match[1]).split("and")
        print(select_conditions)
        # Loop through the table columns to ensure valid column names were inputted 
        for select_condition in select_conditions: 
            print("\nThis is the select condition being dissected to look for an aggregation function.")
            print(select_condition.strip())

            column_and_operation_phrase = select_condition.strip()

            # Identify the appropriate column
            identified_column = ""
            found_column = False
            for column in select_df_columns: 
                if column in column_and_operation_phrase: 
                    identified_column = column
                    found_column = True
            
            if not found_column: 
                print("\nNo valid column could be identified in the phrase:\n", column_and_operation_phrase)
                print("These are the valid column names for this table so far. Capitalization does matter for column names.")
                print(select_df_columns)
                identified_column = None
                return ""
            
            column_and_operation_phrase = column_and_operation_phrase.lower()
            if "sum" in column_and_operation_phrase: 
                print("\nAggregating attribute", identified_column , "via sum")
                column_and_aggregation[identified_column] = "sum"
            elif "mean" in column_and_operation_phrase:
                print("\nAggregating attribute", identified_column , "via mean or average")
                column_and_aggregation[identified_column] = "mean"
            elif "max" in column_and_operation_phrase:
                print("\nAggregating attribute", identified_column , "via max")
                column_and_aggregation[identified_column] = "max"
            elif "min" in column_and_operation_phrase:
                print("\nAggregating attribute", identified_column , "via min")
                column_and_aggregation[identified_column] = "min"
            elif "std" in column_and_operation_phrase:
                print("\nAggregating attribute", identified_column , "via std")
                column_and_aggregation[identified_column] = "std"
            elif "var" in column_and_operation_phrase:
                print("\nAggregating attribute", identified_column , "via var")
                column_and_aggregation[identified_column] = "var"
            elif "count" in column_and_operation_phrase:
                print("\nAggregating attribute", identified_column , "via count")
                column_and_aggregation[identified_column] = "count"
            else: 
                print("\nNo valid aggregation operation identified.")
                print("\nNo valid aggregation operation identified.")
                print("These are the valid aggregation functions:")
                print("sum, mean, max, min, std, var, count")
                return ""
            
        print("\nAggregating.")
        print("Here is the statement.")
        print(f"select_df = select_df.agg({{{column_and_aggregation}}})")
        try:
            print()
            select_df = select_df.agg(column_and_aggregation)
            print("\nAggregation Complete.")
            print("Here is the output of the aggregation.")
            print(select_df)
        except: 
            print("\nThere was an error with the aggregation. Please check capitalizations and aggregation-attribute pairings please.")
            print("You can only use aggregations like sum on numerical attributes.")
            print("Exiting, please try again.")
            return ""
    
    print("\nGrouping and Aggregation Complete")

    # Ordering
    select_df_columns = list(select_df.columns)
    ordering_dict = {}
    if order_by_matches: 
        print("\n\n\nChecking for ordering conditions.")
        for order_by in order_by_matches: 
            print("Identified the ordering condition:\n", order_by)
            order_by = order_by.split(" ")
            attribute = order_by[0].strip()
            ordering = order_by[1].strip().lower()
            print(attribute)
            print(ordering)
            if (ordering == "ascending"): 
                print("Identified the ordering as ascending.")
                order = True
            elif (ordering == "descending"): 
                print("Identified the ordering as descending.")
                order = False
            else: 
                print("\nCould not identify a valid ordering. Please enter 'ascending' or 'descending' next to the attribute name in any capitalization.")
                print("Exiting, please try again.")
                order = True
                return ""

            print("The attribute identified is:\n", attribute)
            identified_column = ""
            found_column = False
            for column in select_df_columns: 
                if column in column_and_operation_phrase: 
                    identified_column = column
                    found_column = True        

            if not found_column: 
                print("\nNo valid column could be identified in the phrase:\n", column_and_operation_phrase)
                print("These are the valid column names for this table so far. Capitalization does matter for column names.")
                print(select_df_columns)
                identified_column = None
                return ""

            ordering_dict[attribute] = order

        print("\nOrdering.")
        print("Here is the statement.")
        # sort_values differs for ordering by a single and multiple columns 
        if (len(ordering_dict) == 1):
            print(f"select_df = select_df.sort_values(by='{attribute}', ascending={ordering_dict[attribute]})")
            try:
                print()
                print(attribute)
                print(bool(ordering_dict[attribute]))
                select_df = select_df.sort_values(by=attribute, ascending=bool(ordering_dict[attribute]))
                print("\nOrdering Complete.")
                print("Here is the output of the ordering.")
                print(select_df)
            except: 
                try: 
                    select_df = select_df.sort_values(ascending=bool(ordering_dict[attribute]))
                    print("\nOrdering Complete.")
                    print("Here is the output of the ordering.")
                    print(select_df)
                except:
                    print("\nThere was an error with the ordering. Please check capitalizations and ascending/descending please.")
                    print("\nOne potential error could also be that there are too many order by attributes relative to the columns after the filtering, grouping, and aggregation.")
                    print("Exiting, please try again.")
                    return ""
        else: 
            ordering_keys = list(ordering_dict.keys())
            ordering_values = list(ordering_dict.values())
            print(f"select_df = select_df.sort_values(by={ordering_keys}, ascending={ordering_values})")
            try:
                print()
                select_df = select_df.sort_values(by=ordering_keys, ascending=ordering_values)
                print("\nOrdering Complete.")
                print("Here is the output of the ordering.")
                print(select_df)
            except: 
                try:
                    print()
                    select_df = select_df.sort_values(by=ordering_keys, ascending=ordering_values)
                    print("\nOrdering Complete.")
                    print("Here is the output of the ordering.")
                    print(select_df)
                except: 
                    print("\nThere was an error with the ordering. Please check capitalizations and ascending/descending please.")
                    print("\nOne potential error could also be that there are too many order by attributes relative to the columns after the filtering, grouping, and aggregation.")
                    print("There are", len(ordering_keys), "attributes to order by.")
                    print("However, the table only has", len(list(select_df.columns)), "columns.")
                    print("Exiting, please try again.")
                    return ""

    # Projection
    select_df_columns = list(select_df.columns)
    if select_match: 
        print("\n\n\nProjecting.")
        



    
    return ""

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
    
    if ("update" not in user_update_input.lower() and "if" not in user_update_input.lower()): 
        print("\nPlease enter a valid update statement, as provided in the examples above.")
        print("Must include an 'IF' and an 'UPDATE' in the statement at least.")
        return ""
    
    user_update_input = user_update_input.lower()

    if not os.path.exists("./Output-Data"):
        os.makedirs("./Output-Data")
    
    pattern = r"IF \((.*?)\) UPDATE (\w+) \((.*?)\);"
    match = re.search(pattern, user_update_input, re.IGNORECASE)
    if match:
        conditions_str, table_name, updates_str = match.groups()

        # Extract column names, comparison operators, new values, and conditions
        conditions = re.findall(r"(\w+)\s*([<>]=?|==|!=)\s*([0-9.]+)", conditions_str)
        updates = re.findall(r"(\w+)\s*=\s*([0-9.]+)", updates_str)

        # Print the results
        print("\n\nTable Name:", table_name)
        print("Conditions:")
        for column, operator, value in conditions:
            print(f"Column: {column}, Operator: {operator}, Condition: {value}")
        print("Updates:")
        for column, value in updates:
            print(f"Column: {column}, New Value: {value}")
        print("\n")

        # Check if the table exists
        tables_path = "./Output-Data/"
        table_path = tables_path + str(table_name) + ".csv"

        if os.path.exists(table_path):
            # Read the .csv file and show the table information to the user 
            file_df = pd.read_csv(table_path)
            print("Here is the info of the table.")
            file_df.info()

            # Filter condition 
            df_filter_expression = None
            str_filter_expression = None

            # Check if all the user-inputted columns in the update statement are in the table
            for column, operator, value in conditions: 
                condition = None
                if column in list(file_df.columns):
                    if operator == '=':
                        value = float(value)
                        condition = (file_df[column] == value)
                        if str_filter_expression is None:
                            str_filter_expression = "(file_df[" + str(column) + "] == " + str(value) + ")"
                        else: 
                            str_filter_expression += "& (file_df[" + str(column) + "] == " + str(value) + ")"
                    elif operator == '<':
                        value = float(value)
                        condition = (file_df[column] < value)
                        if str_filter_expression is None:
                            str_filter_expression = "(file_df[" + str(column) + "] < " + str(value) + ")"
                        else: 
                            str_filter_expression += "& (file_df[" + str(column) + "] < " + str(value) + ")"
                    elif operator == '>':
                        value = float(value)
                        condition = (file_df[column] > value)
                        if str_filter_expression is None:
                            str_filter_expression = "(file_df[" + str(column) + "] > " + str(value) + ")"
                        else: 
                            str_filter_expression += "& (file_df[" + str(column) + "] > " + str(value) + ")"
                    elif operator == '<=':
                        value = float(value)
                        condition = (file_df[column] <= value)
                        if str_filter_expression is None:
                            str_filter_expression = "(file_df[" + str(column) + "] <= " + str(value) + ")"
                        else: 
                            str_filter_expression += "& (file_df[" + str(column) + "] <= " + str(value) + ")"
                    elif operator == '>=':
                        value = float(value)
                        condition = (file_df[column] >= value)
                        if str_filter_expression is None:
                            str_filter_expression = "(file_df[" + str(column) + "] >= " + str(value) + ")"
                        else: 
                            str_filter_expression += "& (file_df[" + str(column) + "] >= " + str(value) + ")"
                    elif operator == 'like':
                        condition = file_df[column].str.contains(value, case=False)
                        if str_filter_expression is None:
                            str_filter_expression = "(file_df[" + str(column) + "] LIKE " + str(value) + ")"
                        else: 
                            str_filter_expression += "& (file_df[" + str(column) + "] LIKE " + str(value) + ")"
                    
                    # Combine the filter expression conditions using logical 'and'
                    if df_filter_expression is None:
                        df_filter_expression = condition
                    else:
                        df_filter_expression = df_filter_expression & condition
                
                else: 
                    print("The column,", column, ", identified in the update statement does not exist in table", str(table_name) + ".")
                    print("Please enter an update statement with valid column names.")
                    print("Here are a list of the columns in table", str(table_name) + ":")
                    print(list(file_df.columns))
                    return ""
                
            # Now that the filter condition has been constructured, identify all the appropriate rows in the corresponding table/DataFrame
            # Show update output to user
            if df_filter_expression is not None:
                print("\nThis is the identified filter expression.")
                print("file_df[" + str_filter_expression + "]")

                print("\nThese are the identified rows:")
                print(file_df[df_filter_expression])
                print("There are", str(len(file_df[df_filter_expression])), "rows that have been identified in table", str(table_name))

                print("\nUpdating...")
                for column, value in updates:
                    file_df.loc[df_filter_expression, column] = value
                    print("Updated", str(column), "to a value of", str(value))
                
                file_df.to_csv(table_path, index=False)
            else:
                print("No valid filter expression could be constructured from the inputted update command.")
    
            print("\nUpdate operation complete!")
    

        # Error message showing all table names in the database if table path does not exist
        else: 
            tables_path = "./Output-Data/"
            file_list = os.listdir(tables_path)
            print("This table does not exist in the database. Please include a valid table name.")
            print("Here is a list of valid table names, please try again.")
            print(file_list)
            return ""

    else: 
        print("\nThe table name and/or update parameters could not be identified.")
        print("Please enter a valid update statement similar to the provided examples.")

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
    if "table " in user_insert_input: 
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
            
            # Export the DataFrame as a .csv to store the data
            new_table_file_name = './Output-Data/' + table_name + '.csv'
            new_table_df.to_csv(new_table_file_name, index=False)

            # Show output to user
            print("The table", table_name, "has been created and inserted into the database.")
            print("Here is the file path of the newly created table", new_table_file_name)

        else: 
            print("The table name and/or parameters could not be identified.")
            print("Please enter a valid insert statement similar to the provided examples.")

    # Insert a row of data 
    elif "row" in user_insert_input: 

        user_insert_input = user_insert_input.lower();
        pattern = r"insert\s+row\s+in\s+(\w+)\s+\((.*?)\)\s*values\s*\((.*?)\);"

        # Match the pattern to the user inputted command
        match = re.match(pattern, user_insert_input, re.IGNORECASE)

        if match:
            # Get the table name
            table_name = match.group(1)  
            # Get the column names and values
            column_names_str = match.group(2)  
            column_values_str = match.group(3)  

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
            table_path = tables_path + table_name + ".csv"
            file_list = os.listdir(tables_path)

            if os.path.exists(table_path):
                file_df = pd.read_csv(table_path)
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
                            value = int(value)
                            print(f"Converted to integer: {value}")
                        except ValueError:
                            # If it's not a valid integer, try converting to a float
                            try:
                                value = float(value)
                                print(f"Converted to float: {value}")
                            except ValueError:
                                # If it can't be converted to either int or float, it's invalid
                                print("Invalid input. Please enter a valid number.")

                        print("The column dtype: ", file_df[column_name].dtype)
                        print("The value dtype: ", type(value))
                        if (file_df[column_name].dtype == type(value)): 
                            add_data = True
                        else: 
                            if file_df[column_name].dtype == float:
                                # int can be converted to float
                                if isinstance(value, int):
                                    add_data = True
                                else: 
                                    print("Not ok")
                                    print("Don't add.")
                                    add_data = False
                            else: 
                                add_data = False

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
                        # Attempt to write the modified DataFrame to the same CSV file/table
                        appended_file_df.to_csv(table_path, index=False)
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

    directory_data_files = os.listdir("./Output-Data")

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
        while not os.path.exists("./Output-Data/" + table_name + ".csv"): 
            table_name = input("The entered table does not exist. Please enter a valid table name: ")
        file_path = "./Output-Data/" + table_name + ".csv"

        os.remove(file_path)

        print("Deleted all data from table", table_name)

    # Delete specific rows of data
    elif "=" in user_delete_input or ">" in user_delete_input or ">=" in user_delete_input or "<" in user_delete_input or "<=" in user_delete_input or "like" in user_delete_input:

        # Identify the relevant columns and corresponding comparison phrases 
        # Define the regular expression pattern to match the specified phrases
        pattern = r'(\b\w+\b)\s*(=|<|>|<=|>=|like)\s*(\b\w+\b)'

        # Find all matches in the input text
        matches = re.findall(pattern, user_delete_input, re.IGNORECASE)

        # Apply delete input on each file/table in the database
        for file in directory_data_files: 
            # Extract and print the matched phrases, along with words/values before and after
            df_filter_expression = None

            file_path = "./Output-Data/" + file

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
                        value = float(value)
                        condition = (file_df[column] == value)
                    elif comparison_phrase == '<':
                        value = float(value)
                        condition = (file_df[column] < value)
                    elif comparison_phrase == '>':
                        value = float(value)
                        condition = (file_df[column] > value)
                    elif comparison_phrase == '<=':
                        value = float(value)
                        condition = (file_df[column] <= value)
                    elif comparison_phrase == '>=':
                        value = float(value)
                        condition = (file_df[column] >= value)
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