#!/bin/bash

# This script automates the  profiling of .csv with the ydata profiling library
# Usage: ./script_name.sh
# code by Oluwatobi

function filepath_not_valid () { #function to confirm valid file path was input

    local path=$1
    if [ "$path" = "" ]; then
        echo "$path"
        return 0
    elif [[ ! -f "$path" ]]; then
        echo "file does not exist. Input existing path"
        return 0
    else
        echo "....valid path has been input"
        return 1
    fi

}

function directorypath_not_valid () { #function to confirm valid directory path was input

    local path=$1
    if [ "$path" = "" ]; then
        echo "$path"
        return 0
    elif [[ ! -d "$path" ]]; then
        echo "path does not exist. Input existing path"
        return 0
    else
        echo "....valid path has been input"
        return 1
    fi

}


function check_python () { #function to confirm .py type script was input
    local path=$1
    if [[ "$path" != *.py ]]; then

        echo "python script not provided"

        return 1
    else
        return 0
    fi
}

function profile(){ #function to batch process .csv files with ydata profiling
    source_directory="$1"
    destination_directory="$2"

    for file in $(find "$destination_directory" -type f); do
    if [ -f "$file" ] && [[ "$file" == *.csv ]]; then 
        echo $file
        python3 $source_directory $file 
    fi
done
}

function user_prompt () { #function to generate user promps and call other functions based response input
    # echo "Kindly input the fullpath of the source directory"
    # read source_directory
    source_directory=""
    echo "Hello"


    # Run both methods
    file_not_exist_status="0"
    py_status="1"

    while [[ "$file_not_exist_status" != 1 && "$py_status" != 0 ]]; do
    # while filepath_not_valid "$source_directory"; do
        echo "Kindly input the fullpath of the python script to profile all .csv"
        read source_directory

        filepath_not_valid "$source_directory"
        file_not_exist_status=$?

        check_python "$source_directory"
        py_status=$?
        echo "$file_not_exist_status,$py_status"

    done

    destination_directory=""

    echo "Kindly input the directory fullpath of the location of .csv files "
    read destination_directory

    directorypath_not_valid "$destination_directory"

    if [[ "$?" = 1 ]]; then 
        profile "$source_directory" "$destination_directory"
    else
        echo "valid directory not input"
    fi
     
}

user_prompt