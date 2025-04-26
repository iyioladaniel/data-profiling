# This script calculates the count of common elements across a specified number of files (N),
# considering all possible combinations of files ranging from 1 to N.
# Usage: python3 ./overlapping_customer_analysis_name.py
# PS: the script runs for hashed files with "*.cleaned.hashed" pattern in current working directory

from itertools import combinations
import pandas as pd
import os
import glob

# obtain existing hashed files in provided directory
def inspect_file_path(file_path):
    # file_path = glob.glob(file_path + "/*cleaned.hashed")
    files = glob.glob("./*cleaned.hashed")
    print(files)
    return generate_possible_combinations(files)

# Function to create the all possible combinations of the files
def generate_possible_combinations(files):
    combinations_list = []
    for i in range(1,len(files)+1):
        computed_combinations = list(combinations(files,i))  
        combinations_list = combinations_list + computed_combinations
    print(combinations_list)  # [('A', 'B', 'C'), ('A','D'), ('A', 'C', 'D', B')]
    print(str(len(combinations_list))+ " combibations created") 

    return review_file_groups(combinations_list)
    
# Function to read files  
def read_file(file_path):
    with open(file_path, 'r') as file:
        # return set(file.read().split(','))
        return set(file.read().splitlines())

# Function to manage the analysis of file combinations grouped in the nested data structure from the generate_possible_combinations() method
def review_file_groups(combinations_list):
    for i in combinations_list:
        # To test output
        # print(str(i))
        for x,file in enumerate(i):
            if x==0:
                intersection_result = read_file(file)
                # To test output
                # print(intersection_result)
            else:
                content = read_file(file)
                # To test output
                # print(content)

                intersection_result = intersection_result.intersection(content)
                # To test output
                # print(intersection_result)

        update_data_structure(str(i),len(i),len(intersection_result))   
    # To test output 
    # print(intersection_result)
      
    return intersection_result     

# function to store the results of the intersection analysis
def update_data_structure(file_names,quantity,intersections):
    df.loc[len(df)] = [file_names,quantity,intersections]

# main function
def main():
    inspect_file_path(os.getcwd())
    # generate_possible_combinations(file_path)     
    print(df) 
    df.to_csv('Overlapping_Analysis.csv', index = True) 

# initiate program
if __name__ == "__main__":
    df = pd.DataFrame(columns=['Entities','Quantity','Intersections'])
    main()
