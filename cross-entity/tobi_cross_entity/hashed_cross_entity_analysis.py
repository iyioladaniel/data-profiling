# This script calculates the count of common elements across a specified number of files (N),
# considering all possible combinations of files ranging from 1 to N.

# Usage: python3 ./overlapping_customer_analysis_name.py
# PS: the file names are configured manually in the function possible_combinations()

from itertools import combinations
import pandas as pd

# Function to create the all possible combinations of the files
def possible_combinations():
    for i in range(1,5):
        
        # input files names into the items[] array
        # items = ["entity1".txt", "entity2.txt","entity3.txt", "entity4.txt","entity5.txt"]
        items = [
            "asset.bvn.sorted.unique.cleaned.hashed", 
            "registrars.bvn.sorted.unique.cleaned.hashed",
            "securities.bvn.sorted.unique.cleaned.hashed",
            "trustees.bvn.sorted.unique.cleaned.hashed"
            ]
        combs = list(combinations(items,i))  
        print(combs)  # [('A', 'B', 'C'), ('A','D'), ('A', 'C', 'D', B')]
        print(str(len(combs))+ " combibations created") 

    return handle_nested(combs)
    
# Function to read files  
def read_file(file_path):
    with open(file_path, 'r') as file:
        # return set(file.read().split(','))
        return set(file.read().splitlines())

# Function to manage the analysis of file combinations grouped in the nested data structure from the possible_combinations() method
def handle_nested(combs):
    for i in combs:
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
#     files_names = str(*file_names)e
#     quantity = quantity
#     intersections = intersections
    df.loc[len(df)] = [file_names,quantity,intersections]

# main function
def main():

    possible_combinations()     
    print(df) 
    df.to_csv('Overlapping_Analysis.csv', index = True) 

# initiate program
if __name__ == "__main__":
    df = pd.DataFrame(columns=['Entities','Quantity','Intersections'])
    main()