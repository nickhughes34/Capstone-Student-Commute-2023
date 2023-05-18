from IPython.display import display
import pandas as pd
from tqdm import tqdm
import time

import io
import msoffcrypto
import getpass

def main():
    '''
        This function that reads data from an excel
        file and parses the data correctly. Then
        it will save the correct values to a new file.
    '''
    ##### Filenames and headers.
    # Names need to be changed depending on file name, sheet name, output name.
    # eg. input_filename = 'Fall 2022 and Winter 2023 Data Commute Assessment Capstone v1.xlsx'
    input_filename = input("Enter input file name (eg. Fall 2022 and Winter 2023 Data Commute Assessment Capstone v1.xlsx): ")
    
    # eg. sheet_name = "Winter Student Data v1"
    sheet_name = input("Enter excel sheet name (eg. Winter Student Data v1): ")
    
    # eg. output_filename = 'Winter_Data_Output.xlsx'
    output_filename = input("Enter output filename (eg. Winter_Data_Output.xlsx): ")

    headers = ["Unique ID", "Postal Code", "Parking Permit", "Residence",
               "Monday", "Tuesday",
               "Wednesday", "Thursday", "Friday",
               "Begin Time", "End Time", "Section Campus Code"]
    ##### Filenames and headers.

    print("Program might take 3-6 mins.")
    print("Decrypting...")
    decrypted_workbook = io.BytesIO()
    with open(input_filename, 'rb') as file:
        office_file = msoffcrypto.OfficeFile(file)
        office_file.load_key(password=getpass.getpass("Enter Password: "))
        office_file.decrypt(decrypted_workbook)

    # Reads in Data and only uses certain headers, changes NaN to zero and makes it a dataframe.
    print("Reading File...")
    user_data = pd.read_excel(decrypted_workbook, sheet_name=sheet_name,usecols=headers)

    # Drops Online and Other Destinations
    print("Dropping Online and Others...")
    user_data = user_data[user_data["Section Campus Code"] != "UOW"]
    user_data = user_data[user_data["Section Campus Code"] != "UOO"]
    user_data = user_data[user_data["Section Campus Code"] != "UOG"]
    user_data = user_data[user_data["Section Campus Code"] != "O"]

    # Drops Parking Pass and Residents
    print("Dropping Parking Pass and Residents...")
    user_data = user_data[user_data["Parking Permit"] != "Y"]
    user_data = user_data[user_data["Residence"] != "Y"]

    # Drops Null Postal Codes
    print("Droping Blank Postal Code...")
    user_data = user_data[user_data["Postal Code"].isnull() == False]

    # Permanently changes the pandas settings.
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)

    # Gets unique Student ID to group people by.
    unique_id = user_data["Unique ID"].unique()
    splitDataFrames = [[]] * len(unique_id)

    for position, id in enumerate(tqdm(unique_id, desc="Splitting Frames...")):
        splitDataFrames[position] = user_data[user_data["Unique ID"] == id]
        #time.sleep(0.0005)

    # CREATE FUNCTION THAT TAKES SMALLEST AND LARGEST TIME FOR EACH DAY.
    days = {"Monday": "M", "Tuesday": "T", "Wednesday": "W", "Thursday": "R", "Friday": "F"}
    #newDataFrames = [[]] * len(unique_id)

    # Loops throughout the dataframes for each student and gets min and max time for each day.
    new_user_frame = pd.DataFrame()
    for frame_index, frame in enumerate(tqdm(splitDataFrames, desc="Adding Min & Max...")): # For each student.
        for day in days: # For each day
            iteration = frame[frame[day] == days[day]] # Gets every row in the same day.
            if not iteration.empty:
                # Sets min and max to new frame.
                new_iteration = pd.DataFrame()
                new_iteration = pd.concat([iteration.head(1)])
                new_iteration["Begin Time"] = iteration["Begin Time"].min()
                new_iteration["End Time"] = iteration["End Time"].max()

                # Changes The weekday headers into one day header.
                new_iteration["Monday"] = day

                # Fix Postal Codes
                postal = new_iteration["Postal Code"].values[0]
                if len(postal) == 6:
                    new_iteration["Postal Code"] = " ".join([postal[:3], postal[3:]])

                new_user_frame = pd.concat([new_user_frame, new_iteration])

    # Update header name and drops other days.
    print("Saving...")
    new_user_frame = new_user_frame.rename({'Monday': 'Day'}, axis = 1)
    new_user_frame = new_user_frame.drop(["Tuesday", "Wednesday", "Thursday", "Friday"], axis = 1)
    # Save to new excel file for usage later.
    new_user_frame.to_excel(output_filename, index=False)
    print("Completed!")

# Runs program if its the main file.
if __name__ == "__main__":
    main()
