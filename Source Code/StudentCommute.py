
import requests
import json
import time
import math
import datetime as dt
from datetime import datetime

from IPython.display import display
import pandas as pd
from tqdm import tqdm

import psycopg2

import colorama
from colorama import Fore

def get_api_key():
    '''
        Asks the user for API 
        key for google cloud account
        and returns the input.
    '''
    print("Enter API key from Google Maps Platform, see website below to get key:")
    print("Click 'get started' and 'APIs' then fill out information then click 'APIs' again and copy key.")
    print("https://mapsplatform.google.com/")
    api_key = input("Enter API key: ")
    return api_key

def get_echo_time():
    '''
        Asks the user for UTC 
        echo time at Monday at 6AM
        and returns the input.
    '''
    print("Enter Echo Time in UTC at 0:00AM for an upcoming Sunday. See website below to get code:")
    print("https://www.timeanddate.com/date/timezoneduration.html?d1=1&m1=1&y1=1970&d2=03&m2=04&y2=2023&h1=0&i1=0&s1=0&h2=06&i2=0&s2=0&")
    echo_time = input("Enter UTC Echo Time: ")
    echo_time = echo_time.replace(",", "")
    echo_time = int(echo_time)
    return echo_time

def get_postgresql_credentials():
    '''
        Asks the user for database 
        credentials and then 
        returns the inputs.
    '''
    print("Enter your database credentials below, if you do not have postgres installed follow the youtube guide below:")
    print("https://www.youtube.com/watch?v=0n41UTkOBb0")
    datebase_name = input("Enter Database name (defualt is postgres): ")
    username = input("Enter username (defualt is postgres): ")
    password = input("Enter password: ")
    return datebase_name, username, password

def hours_to_seconds(echo_time, today, a_time, d_time=""):
    '''
        Passes in arrival and departure time
        in hours and converts then into seconds.
        Returns the values as seconds.
    '''
    # https://www.timeanddate.com/date/timezoneduration.html?d1=1&m1=1&y1=1970&d2=01&m2=01&y2=2023&h1=0&i1=0&s1=0&h2=0&i2=0&s2=0&
    # Change data values depending on when the time of usage (change end date to current year and current month + 1 and sunday)
    year_2023 = echo_time
    day = 86400
    hour_difference = 14400 # 4 hour time differnce between GMT and UTC
    monday = year_2023 + (day)
    tuesday = year_2023 + (day * 2)
    wednesday = year_2023 + (day * 3)
    thursday = year_2023 + (day * 4)
    friday = year_2023 + (day * 5)
    days = {"Monday" : monday, "Tuesday" : tuesday, "Wednesday" : wednesday, "Thursday" : thursday, "Friday" : friday}

    # Converts the 24 hour time to seconds
    a_hours = math.floor(a_time / 100) # Gets rounded down whole number of hours
    a_minutes = a_time % 100

    #d_hours = math.floor(d_time / 100) # Gets rounded down whole number of hours
    #d_minutes = d_time % 100

    # Calculates arrival and departure for the day in seconds
    for day in days:
        if today == day:
            arrival_time_seconds = days[today] + (a_hours * 60 * 60) + (a_minutes * 60) + hour_difference
            #departure_time_seconds = days[today] + (d_hours * 60 * 60) + (d_minutes * 60) + hour_difference
            break # Exits loops after day is found

    #return arrival_time_seconds, departure_time_seconds
    return arrival_time_seconds

def api_query(Origin, Destination, time, key):
    '''
        This function querys Google Maps API
        for transit time and returns it in
        JSON format.
    '''

    r = requests.get('https://maps.googleapis.com/maps/api/directions/json?' +
                     'origin=' + Origin +
                     '&mode=transit' +
                     '&transit_mode=bus' +
                     #'&transit_routing_preference=fewer_transfers' +
                     '&destination=' + Destination +
                     time +
                     '&key=' + key)
    return r.json()

def show_results(direction_data, type):
    '''
        This function will print the
        results of the query to the
        console.
    '''
    if direction_data['status'] == 'OK':
        print("Overview for", type + ":\n")
        print("   Distance | ", direction_data['routes'][0]['legs'][0]['distance']['text'])
        print("   Duration | ", direction_data['routes'][0]['legs'][0]['duration']['text'])
        print("Destination | ", direction_data['routes'][0]['legs'][0]['end_address'])
        print("      Start | ", direction_data['routes'][0]['legs'][0]['start_address'])
        print("  Start Lat | ", direction_data['routes'][0]['legs'][0]['start_location']['lat'])
        print("  Start Lng | ", direction_data['routes'][0]['legs'][0]['start_location']['lng'])

        # Times and Stops
        for count, step in enumerate(direction_data['routes'][0]['legs'][0]['steps']):
            print("\nStop", count + 1, ":")
            if step["travel_mode"] == "TRANSIT":
                print('     Depart |', step['transit_details']['departure_time']['text'])
                print('       Stop |', step['transit_details']['arrival_stop']['name'])
                print('        Bus |', step['transit_details']['line']['short_name'],
                      step['transit_details']['headsign'])
                print('    Arrival |', step['transit_details']['arrival_time']['text'])
                
                # Finds the wait time between bus stops
                if count == 0 or (count == 1 and direction_data['routes'][0]['legs'][0]['steps'][0]["travel_mode"] == 'WALKING'):
                    depart_current = ""
                    print('       Wait | 0 minutes')
                    arrival_previous = step['transit_details']['arrival_time']['value']
                else:
                    depart_current = step['transit_details']['departure_time']['value']
                    wait_time = math.ceil((depart_current - arrival_previous) / 60)
                    print('       Wait |', wait_time, "minutes")
                    arrival_previous = step['transit_details']['arrival_time']['value']

            if step["travel_mode"] == "WALKING":
                print("   Distance | ", step['distance']['text'])
                print("   Duration | ", step['duration']['text'])

        print("\n\n")
        # print(json.dumps(direction_data, indent=2))
    else:
        print(direction_data['status'])

def save_to_sql(first, row, index, direction_data, cursor):
    '''
        This function save the user
        data to the sql database.
        (CVS input + Google API request)
    '''
    
    if first == True:
        # Query 1 to save to Table 1 (Student Entry Table)
        '''print(Fore.BLUE + "\nQuery 1 will execute here!")
        print(row["Unique ID"], 
              row["Postal Code"].split(" ")[0], 
              row["Section Campus Code"], 
              direction_data['routes'][0]['legs'][0]['start_location']['lat'],
              direction_data['routes'][0]['legs'][0]['start_location']['lng'])'''
        try:
            query = '''INSERT INTO student_entry (student_id, postal_code, campus_code, latitude, longitude) 
                       VALUES ({}, '{}', '{}', {}, {})'''.format(row["Unique ID"], 
                                                                 row["Postal Code"].split(" ")[0], 
                                                                 row["Section Campus Code"], 
                                                                 direction_data['routes'][0]['legs'][0]['start_location']['lat'],
                                                                 direction_data['routes'][0]['legs'][0]['start_location']['lng'])
            cursor.execute(query)
        except Exception as e:
            print("ERROR: \n", e)
        first = False
    
    # Query 2 to save to Table 2 (Day of Week Table)
    '''print(Fore.RED + "\nQuery 2 will execute here!")
    # Converts the 24 hour time to seconds
    a_hours = math.floor(row["Begin Time"] / 100) # Gets rounded down whole number of hours
    a_minutes = row["Begin Time"] % 100
    print("   Index ID | ", index)
    print(" Student ID | ", row["Unique ID"])
    print("    Weekday | ", row["Day"])
    print(" Start Time | ", row["Begin Time"])
    print(" Start Time | ", (a_hours * 60 * 60) + (a_minutes * 60) + 18000) # 5 hour difference in time.
    print("   Duration | ", direction_data['routes'][0]['legs'][0]['duration']['text'])
    print("   Distance | ", direction_data['routes'][0]['legs'][0]['distance']['text'])'''
    try:
        a_hours = math.floor(row["Begin Time"] / 100) # Gets rounded down whole number of hours
        a_minutes = row["Begin Time"] % 100
        query = '''INSERT INTO day_of_week (week_id, student_id, day_of_week, start_time, commute_time, distance) 
                   VALUES ({}, {}, '{}', {}, {}, {})'''.format(index, 
                                                               row["Unique ID"], 
                                                               row["Day"], 
                                                               (a_hours * 60 * 60)  + (a_minutes * 60) + 18000, # 5 hour difference in time.
                                                               direction_data['routes'][0]['legs'][0]['duration']['value'],
                                                               direction_data['routes'][0]['legs'][0]['distance']['value'])
        cursor.execute(query)
    except Exception as e:
        print("ERROR: \n", e)

    # Times and Stops
    for count, step in enumerate(direction_data['routes'][0]['legs'][0]['steps']):
        if step["travel_mode"] == "TRANSIT":            
            # Finds the wait time between bus stops
            if count == 0 or (count == 1 and direction_data['routes'][0]['legs'][0]['steps'][0]["travel_mode"] == 'WALKING'):
                depart_current = ""
                wait_time = 0
                arrival_previous = step['transit_details']['arrival_time']['value']
            else:
                depart_current = step['transit_details']['departure_time']['value']
                wait_time = math.ceil((depart_current - arrival_previous) / 60)
                arrival_previous = step['transit_details']['arrival_time']['value']

            # Converts Unix epoch time to HH:MM:SS format
            d_time = step['transit_details']['departure_time']['value']
            depart_value = dt.datetime.fromtimestamp(d_time)
            a_time = step['transit_details']['arrival_time']['value']
            arrival_value = dt.datetime.fromtimestamp(a_time)

            # Query 3 to save to Table 3 (Busses Taken Table)
            '''print(Fore.YELLOW + "\nQuery 3 will execute here!")
            print("   Index ID | ", index)
            print("     Depart | ", step['transit_details']['departure_time']['value'])
            print("    Arrival | ", step['transit_details']['arrival_time']['value'])
            print("       Stop | ", step['transit_details']['arrival_stop']['name'])
            print("  Bus Taken | ", step['transit_details']['line']['short_name'], step['transit_details']['headsign'])
            print("  Wait Time | ", wait_time)'''

            try:
                query = '''INSERT INTO buses_taken (week_id, depart, arrival, stop, bus_route, wait_time) 
                        VALUES ({}, '{}', '{}', '{}', '{}', {})'''.format(index,
                                                                      depart_value.strftime('%H:%M:%S'),
                                                                      arrival_value.strftime('%H:%M:%S'),
                                                                      step['transit_details']['arrival_stop']['name'],
                                                                      step['transit_details']['line']['short_name'] + step['transit_details']['headsign'],
                                                                      wait_time)
                cursor.execute(query)
            except Exception as e:
                print("ERROR: \n", e)

    return first

def arrival_and_depart(apiKey, echo_time, origin, destination, day, arrival_time, departure_time=""):
    '''
        This function calls the api_query function
        then calls the show results function as well
        as returning the results of the query.
    '''
    # Calcualtes hour time into seconds
    arrival_time_seconds, departure_time_seconds = '',''
    try:
        #arrival_time_seconds, departure_time_seconds = hours_to_seconds(day, arrival_time, departure_time)
        arrival_time_seconds = hours_to_seconds(echo_time, day, arrival_time)
    except Exception as e:
        print("ERROR: \n", e)

    # For Departure and Arrival Times
    if arrival_time_seconds != "":
        direction_data = api_query(origin, destination, '&arrival_time=' + str(arrival_time_seconds), apiKey)
        #show_results(direction_data, "arrival")
    else:
        print("No arrival time")

    '''if departure_time_seconds != "":
        direction_data2 = api_query(destination, origin, '&departure_time=' + str(departure_time_seconds), apiKey)
        #show_results(direction_data2, "departure")
    else:
        print("No departure time")'''

    #return direction_data, direction_data2
    return direction_data

def main():
    '''
        This function calls the arrival and depart
        function which does the main query request
        and saves the results to the database.
    '''

    table1 = '''CREATE TABLE IF NOT EXISTS student_entry (
    student_id INT PRIMARY KEY,
    postal_code VARCHAR(3) NOT NULL,
    campus_code VARCHAR(3) NOT NULL,
    latitude DECIMAL NOT NULL,
    longitude DECIMAL NOT NULL)'''

    table2 = '''CREATE TABLE IF NOT EXISTS day_of_week(
    week_id INT PRIMARY KEY,
    student_id INT NOT NULL,
    day_of_week VARCHAR NOT NULL,
    start_time INT NOT NULL,
    commute_time INT NOT NULL,
    distance INT NOT NULL,
    FOREIGN KEY (student_id) REFERENCES student_entry(student_id))'''

    table3 = '''CREATE TABLE IF NOT EXISTS buses_taken (
    bus_id Serial PRIMARY KEY,
    week_id INT NOT NULL,
    depart TIME NOT NULL,
    arrival TIME NOT NULL,
    stop VARCHAR NOT NULL,
    bus_route VARCHAR NOT NULL,
    wait_time INT NOT NULL,
    FOREIGN KEY (week_id) REFERENCES day_of_week(week_id))'''

    ### API Key
    # Get API key
    apiKey = get_api_key()
    ### API Key

    # Get echo time
    # eg. echo_time = 1680393600
    echo_time = get_echo_time()

    # Get database credentials
    database_name, username, password = get_postgresql_credentials()

    ##### Filenames and headers.
    # Get input filename
    # eg. input_filename = 'Winter_Data_Output.xlsx'
    input_filename = input("Enter Parsed data file name (Eg. Winter_Data_Output.xlsx): ")
    headers = ["Unique ID", "Postal Code", "Day", "Begin Time", "Section Campus Code"]
    ##### Filenames and headers.

    ##### Postgesql Connection
    # Must change database=,user=,password= depending on how you setup postgres on your local machine.
    conn = psycopg2.connect(database=database_name, 
                            user=username, 
                            password=password, 
                            host='localhost', 
                            port= '5432')
    ##### Postgesql Connection
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(table1)
    cursor.execute(table2)
    cursor.execute(table3)

    # Reads in Data and only uses certain headers and makes it a dataframe.
    user_data = pd.read_excel(input_filename, usecols=headers)
    user_data = pd.DataFrame(user_data)
    
    # Split user_data if the program crashes
    #user_data = user_data.iloc[15229:]

    # Opens the file and reads in the variables

    # Gets unique Student ID to group people by.
    student_id = user_data["Unique ID"].unique()
    splitDataFrames = [[]] * len(student_id)

    for position, id in enumerate(tqdm(student_id, desc="Splitting Frames...")):
        splitDataFrames[position] = user_data[user_data["Unique ID"] == id]
        #time.sleep(0.0005)

    for frame_index, frame in enumerate(tqdm(splitDataFrames, desc="Appending to database...")): # For each student.        
        first = True # COUNTER for a first table insert query
        for index, row in frame.iterrows():
            origin = row["Postal Code"]
            day = row["Day"]
            arrival_time = row["Begin Time"]
            
            # Checks Campus Code Location
            if row["Section Campus Code"] == "UOD":
                destination = "50 King St E, Oshawa, ON L1H 1B3"
            else:
                destination = "2000 Simcoe St N, Oshawa, ON L1G 0C5"
            
            arrival_path = arrival_and_depart(apiKey, echo_time, origin, destination, day, arrival_time) # API Call
            
            if arrival_path["status"] == "OK":
                first = save_to_sql(first, row, index, arrival_path, cursor)
            else:
                print("Error has occured in google maps api")
                break # Skips the whole user since it can bus to school.
                
# Runs program if its the main file.
if __name__ == "__main__":
    main()
