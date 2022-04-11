import os
import csv
from pathlib import Path 
import glob
import pandas as pd
count = 0 # Global variable
cur_list = []


#Read each file for errors, Count Errors within each file
def analyze_file(f):
    global count
    global cur_list 
    look = open(f) # Opens Each file 
    for line in look:
        if "ERROR" in line:
            cur_list.append(line)
            count = count + 1

#Traverses through each folder
files = glob.glob(r'C:\Users\Keval\Desktop\AirflowDocker\logs\marketvol\**\**\*.log', 
                recursive = True)   
for file in files:
    analyze_file(file)   
    

#Print to Console Results
print("Total number of errors: ", count)
print("Here are all the errors: ")  
print('\n'.join(cur_list))
