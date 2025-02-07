#!/bin/bash

# This script automates the moving files with .csv, .xlsx, .xls format from and into the 
# specified directories
# Usage: ./script_name.sh

function filepath_not_valid () { #function to confirm valid file path was input

    local path=$1
    if [ "$path" = "" ]; then
        return 0
    elif [ ! -d $path ]; then
        echo "path does not exist. Input existing path"
        return 0
    else
        echo "....valid path has been input"
        return 1
    fi

}

function move_file () { # function to move the file
    local source_directory="$1"
    local destination_directory="$2"
    # find all .csv , .xlsx , .xls files and  move them to the created directory
    find "$source_directory" -type f \( -name '*.csv' -o -name '*.xlsx' -o -name '*.xls' \) -exec mv {} "$destination_directory" \;

}



function user_prompt () { #function to generate user promps and call other functions based response input

    source_directory=""
    echo "Hello"

    while filepath_not_valid "$source_directory"; do
        echo "Kindly input the fullpath of the source directory"
        read source_directory
    done
    
    option=""
    while [[ $option != "y" &&  $option != "n" ]]; do
        echo "Does your destination directory exist
            input 'y' : yes 
            input 'n' : no "
        read option
    done
    response=$option

    if [ "$response" = "y" ]; then
        # destination_directory=""
        while filepath_not_valid "$destination_directory"; do
            echo "Kindly input the fullpath of the destination directory"
            read destination_directory
        done    

        move_file "$source_directory" "$destination_directory"
    elif [ "$response" = "n" ]; then

        destination_directory=""

        while filepath_not_valid "$create_dir"; do
            echo "Input fullpath of the destination directory to be created"
            read create_dir   
            mkdir -p "$create_dir"
        done 
        move_file "$source_directory" "$create_dir"
 
    fi


    # done    
}

user_prompt