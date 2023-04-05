from IPython.display import display
import pandas as pd
import numpy as np
from tqdm import tqdm
import psycopg2
import colorama
from colorama import Fore
import datetime as dt

from Queries_v2 import get_echo_time, get_postgresql_credentials, get_bus_count_individual_days_echo, get_bus_count_individual_days_datetime

def wait_time_between(cursor, time1, time2):
    '''
        Queries the database and gets
        average commute time per postal code
        and returns a dataframe of results.
    '''
        
    # GETS AVERAGE OF WAIT TIME
    avg_commute_time = '''
                        SELECT student_entry.postal_code, CAST(AVG(buses_taken.wait_time) as float) as average_wait_time 
                        FROM buses_taken 
                        JOIN day_of_week ON day_of_week.week_id = buses_taken.week_id
                        JOIN student_entry  ON student_entry.student_id = day_of_week.student_id
                        GROUP BY student_entry.postal_code 
                        ORDER BY average_wait_time DESC;'''
    
    cursor.execute(avg_commute_time)
    avg_ct_results = cursor.fetchall()
    avg_ct_results = pd.DataFrame(avg_ct_results)
    avg_ct_results = avg_ct_results.rename({0: 'PostalCode', 1: 'Average Wait Time'}, axis=1)
    avg_ct_results = avg_ct_results[avg_ct_results["Average Wait Time"] > time1]
    avg_ct_results = avg_ct_results[avg_ct_results["Average Wait Time"] < time2]
    avg_ct_results["Average Wait Time"] = avg_ct_results["Average Wait Time"].round(0)
    return avg_ct_results

def wait_time_day(cursor, day, time1, time2):
    '''
        Queries the database and gets
        average commute time per postal code
        and returns a dataframe of results.
    '''
        
    # GETS AVERAGE OF WAIT TIME
    avg_commute_time = '''
                        SELECT student_entry.postal_code, student_entry.student_id, buses_taken.week_id, buses_taken.bus_id,        
                        (buses_taken.depart) as depart, (buses_taken.arrival) as arrival,
                        buses_taken.bus_route, buses_taken.wait_time as wait_time, CAST(AVG(day_of_week.distance)/1000 as float) as distance
                        FROM buses_taken 
                        JOIN day_of_week ON day_of_week.week_id = buses_taken.week_id
                        JOIN student_entry  ON student_entry.student_id = day_of_week.student_id
                        WHERE day_of_week.day_of_week = '{}'
                        GROUP BY student_entry.postal_code, buses_taken.wait_time, buses_taken.bus_route, 
                        student_entry.student_id, buses_taken.week_id, buses_taken.bus_id, day_of_week.distance
                        ORDER BY student_id DESC, buses_taken.bus_id ASC;'''.format(day)
    
    cursor.execute(avg_commute_time)
    avg_ct_results = cursor.fetchall()
    avg_ct_results = pd.DataFrame(avg_ct_results)
    avg_ct_results = avg_ct_results.rename({0: 'PostalCode', 1: 'Unique ID', 2: 'Week ID', 
                                            3: 'Bus ID', 4: 'Depart', 5: 'Arrival', 
                                            6: 'Route', 7: 'Wait Time', 8: 'Distance'}, axis=1)
    #avg_ct_results = avg_ct_results[avg_ct_results["Wait Time"] > time1]
    avg_ct_results = avg_ct_results[avg_ct_results["Wait Time"] < time2]
    avg_ct_results["Wait Time"] = avg_ct_results["Wait Time"].round(0)
    avg_ct_results["Distance"] = avg_ct_results["Distance"].round(0)
    
    avg_ct_results['Depart'] = pd.to_datetime(avg_ct_results['Depart'], unit='s').dt.strftime('%H:%M:%S')
    avg_ct_results['Arrival'] = pd.to_datetime(avg_ct_results['Arrival'], unit='s').dt.strftime('%H:%M:%S')

    # Gets unique Student ID to group people by.
    unique_id = avg_ct_results["Week ID"].unique()
    splitDataFrames = [[]] * len(unique_id)
    new_user_frame = pd.DataFrame()

    for position, id in enumerate(tqdm(unique_id, desc="Splitting Frames...")):
        splitDataFrames[position] = avg_ct_results[avg_ct_results["Week ID"] == id]
        #time.sleep(0.0005)

    for frame_index, frame in enumerate(tqdm(splitDataFrames, desc=day+" Getting for interval...")): # For each student.        
        #first = True # COUNTER for a first table insert query
        #print(frame["Wait Time"].max())
        if frame["Distance"].max() <= 70:
            if frame["Wait Time"].max() > time1 and frame["Wait Time"].max() < time2:
                if len(frame[frame['Route'].str.contains('915Harmony Terminal')]) > 0:
                    new_user_frame = pd.concat([new_user_frame, frame])

    return new_user_frame

def wait_time_week(cursor, bus, time1, time2):
    '''
        Queries the database and gets
        average commute time per postal code
        and returns a dataframe of results.
    '''
        
    # GETS AVERAGE OF WAIT TIME
    avg_commute_time = '''
                        SELECT student_entry.postal_code, student_entry.student_id, buses_taken.week_id, buses_taken.bus_id,        
                        (buses_taken.depart) as depart, (buses_taken.arrival) as arrival, buses_taken.bus_route, 
                        buses_taken.wait_time as wait_time, CAST(AVG(day_of_week.distance)/1000 as float) as distance, day_of_week.commute_time
                        FROM buses_taken 
                        JOIN day_of_week ON day_of_week.week_id = buses_taken.week_id
                        JOIN student_entry  ON student_entry.student_id = day_of_week.student_id
                        GROUP BY student_entry.postal_code, buses_taken.wait_time, buses_taken.bus_route, 
                        student_entry.student_id, buses_taken.week_id, buses_taken.bus_id, day_of_week.distance, day_of_week.commute_time
                        ORDER BY student_id DESC, buses_taken.bus_id ASC;'''
    
    cursor.execute(avg_commute_time)
    avg_ct_results = cursor.fetchall()
    avg_ct_results = pd.DataFrame(avg_ct_results)
    avg_ct_results = avg_ct_results.rename({0: 'PostalCode', 1: 'Unique ID', 2: 'Week ID', 
                                            3: 'Bus ID', 4: 'Depart', 5: 'Arrival', 
                                            6: 'Route', 7: 'Wait Time', 8: 'Distance', 9: 'Commute Time'}, axis=1)
    #avg_ct_results = avg_ct_results[avg_ct_results["Wait Time"] > time1]
    avg_ct_results = avg_ct_results[avg_ct_results["Wait Time"] < time2]
    avg_ct_results["Wait Time"] = avg_ct_results["Wait Time"].round(0)
    avg_ct_results["Distance"] = avg_ct_results["Distance"].round(0)
    
    avg_ct_results['Depart'] = pd.to_datetime(avg_ct_results['Depart'], unit='s').dt.strftime('%H:%M:%S')
    avg_ct_results['Arrival'] = pd.to_datetime(avg_ct_results['Arrival'], unit='s').dt.strftime('%H:%M:%S')

    # Gets unique Student ID to group people by.
    unique_id = avg_ct_results["Week ID"].unique()
    splitDataFrames = [[]] * len(unique_id)
    new_user_frame = pd.DataFrame()

    for position, id in enumerate(tqdm(unique_id, desc="Splitting Frames...")):
        splitDataFrames[position] = avg_ct_results[avg_ct_results["Week ID"] == id]
        #time.sleep(0.0005)

    for frame_index, frame in enumerate(tqdm(splitDataFrames, desc=" Getting week interval...")): # For each student.        
        #first = True # COUNTER for a first table insert query
        #print(frame["Wait Time"].max())
        if frame["Distance"].max() <= 70:
            if frame["Wait Time"].max() > time1 and frame["Wait Time"].max() < time2:
                for x in range(len(bus)):
                    if len(frame[frame['Route'].str.contains(bus[x])]) > 0:
                        new_user_frame = pd.concat([new_user_frame, frame])
                        break

    return new_user_frame

def main():
    '''
        Calls the postgresql database
        gets the averages and saves 
        the results to excel file.
    '''

    # Get database credentials
    database_name, username, password = get_postgresql_credentials()
    
    # Get output filename
    # eg. output_filename = 'query_output_winter.xlsx'
    output_filename = input("Enter output filename (eg. query4_output.xlsx): ")
    
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

    print("Enter bus name. If more than one enter like so, seperated by a comma (For example: 915Harmony Terminal, 915Harmony Terminal, ...")
    bus_names = input("Enter bus name(s): ")
    bus_names = bus_names.split(",")

    '''wait_time = wait_time_between(cursor, 20, 40)
    mon_wait_time = wait_time_day(cursor, "Monday", 0, 40)
    tue_wait_time = wait_time_day(cursor, "Tuesday", 0, 40)
    wed_wait_time = wait_time_day(cursor, "Wednesday", 0, 40)
    thu_wait_time = wait_time_day(cursor, "Thursday", 0, 40)
    fri_wait_time = wait_time_day(cursor, "Friday", 0, 40)'''
    total_wait_time = wait_time_week(cursor, ["915Harmony Terminal"], 0, 40)
    #print(wait_time)

    print("Saving...")
    # SAVES ALL QUERY DATA EXCEL FILE TO MAKE GRAPHS
    with pd.ExcelWriter(output_filename) as writer:  
        '''wait_time.to_excel(writer, sheet_name='wait_time', index=False)
        mon_wait_time.to_excel(writer, sheet_name='mon_wait_time', index=False)
        tue_wait_time.to_excel(writer, sheet_name='tue_wait_time', index=False)
        wed_wait_time.to_excel(writer, sheet_name='wed_wait_time', index=False)
        thu_wait_time.to_excel(writer, sheet_name='thu_wait_time', index=False)
        fri_wait_time.to_excel(writer, sheet_name='fri_wait_time', index=False)'''
        total_wait_time.to_excel(writer, sheet_name='total_week_wait_time', index=False)
    print("Complete...")

if __name__ == "__main__":
    main()