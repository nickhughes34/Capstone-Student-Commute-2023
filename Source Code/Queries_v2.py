from IPython.display import display
import pandas as pd
import numpy as np
from tqdm import tqdm
import psycopg2
import colorama
from colorama import Fore

import gmaps

def get_echo_time():
    '''
        Asks the user for UTC 
        echo time at Monday at 6AM
        and returns the input.
    '''
    print("Enter Echo Time in UTC at 6:00AM for an upcoming Monday. See website below to get code:")
    print("https://www.timeanddate.com/date/timezoneduration.html?d1=1&m1=1&y1=1970&d2=03&m2=04&y2=2023&h1=0&i1=0&s1=0&h2=06&i2=0&s2=0&")
    echo_time = int(input("Enter UTC Echo Time: "))
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

def postal_code_count(cursor):
    '''
        Queries the database and gets
        postal code count per student
        and returns a dataframe of results.
    '''
    # GETS COUNT OF POSTAL CODES
    postal_code_count = '''SELECT postal_code, COUNT(postal_code) 
                           FROM student_entry
                           GROUP BY postal_code
                           ORDER BY COUNT(postal_code) DESC;'''
    cursor.execute(postal_code_count)
    results = cursor.fetchall()
    #print(Fore.BLUE + "Postal Code Count:", results)
    results = pd.DataFrame(results)
    results = results.rename({0 : 'Postal Code', 1 : 'Count'}, axis=1)
    #display(results.head(10))
    #display(results["Count"].sum())
    return results

def average_distance_and_commute_time(cursor):
    '''
        Queries the database and gets
        average distance per postal code
        and returns a dataframe of results.
    '''
        
    # GETS AVERAGE OF DISTANCE AND COMMUTE TIME 
    avg_distance_commute_time = '''
                    SELECT postal_code, COUNT(postal_code), 
                    CAST(AVG(distance)/1000 as float) AS avg_distance, 
                    CAST(AVG(commute_time)/60 as float) AS avg_commute_time
                    FROM day_of_week
                    INNER JOIN student_entry
                    ON day_of_week.student_id = student_entry.student_id
                    GROUP BY postal_code
                    ORDER BY COUNT(postal_code) DESC;'''
    cursor.execute(avg_distance_commute_time)
    avg_results = cursor.fetchall()
    avg_results = pd.DataFrame(avg_results)
    avg_results = avg_results.rename({0: 'PostalCode', 1: 'Count', 2: 'Average Distance (km)', 3: 'Average Commute Time (m)'}, axis=1)
    avg_results["Average Distance (km)"] = avg_results["Average Distance (km)"].round(0)
    avg_results["Average Commute Time (m)"] = avg_results["Average Commute Time (m)"].round(0)
    #display(avg_d_results.head(10))
    return avg_results

def average_commute_time(cursor):
    '''
        Queries the database and gets
        average commute time per postal code
        and returns a dataframe of results.
    '''
        
    # GETS AVERAGE OF COMMUTE TIME
    avg_commute_time = '''SELECT postal_code, COUNT(postal_code), CAST(AVG(commute_time)/60 as float) AS avg_commute_time
                            FROM day_of_week
                            INNER JOIN student_entry
	                            ON day_of_week.student_id = student_entry.student_id
                            GROUP BY postal_code
                            ORDER BY COUNT(postal_code) DESC;'''
    cursor.execute(avg_commute_time)
    avg_ct_results = cursor.fetchall()
    avg_ct_results = pd.DataFrame(avg_ct_results)
    avg_ct_results = avg_ct_results.rename({0: 'PostalCode', 1: 'Count', 2: 'Average Commute Time'}, axis=1)
    avg_ct_results["Average Commute Time"] = avg_ct_results["Average Commute Time"].round(0)
    #display(avg_ct_results.head(10))
    return avg_ct_results

def get_coords_heatmap(cursor):
    '''
        Queries the database and gets
        cords for each user and returns 
        a dataframe of results.
    '''
        
    # GETS LAT AND LONG
    lat_df = '''
                SELECT (student_entry.student_id) as studentNumber, CAST(MAX(latitude) as float), 
                CAST(MAX(longitude) as float), MAX(day_of_week.distance/1000) as distance
                FROM student_entry
                INNER JOIN day_of_week
                ON student_entry.student_id = day_of_week.student_id
                WHERE day_of_week.distance/1000 > 1
                GROUP BY studentNumber
                ORDER BY distance ASC;'''

    cursor.execute(lat_df)
    coords = cursor.fetchall()
    coords = pd.DataFrame(coords)
    coords = coords.rename({0: 'id', 1: 'latitude', 2: 'longitude', 3: 'distance'}, axis=1)

    # GENTERATES HEATMAP [USE JUPYTER NOTEBOOK FOR THIS STEP]
    #gmaps.configure(api_key='AI')

    '''figure_layout = {
        'width': '400px',
        'height': '400px',
        'border': '1px solid black',
        'padding': '1px'
    }

    locations = coords[['latitude', 'longitude']]
    fig = gmaps.figure(layout=figure_layout)
    fig.add_layer(gmaps.heatmap_layer(locations))
    display(fig)'''
    # GENTERATES HEATMAP [USE JUPYTER NOTEBOOK FOR THIS STEP]

    return coords

def get_bus_count_total_week_echo(cursor, startime):
    '''
        Queries the database and gets
        total buses used for day
        a dataframe of results.
    '''
    mon6 = startime + 14400
    mon8 = mon6 + 7200
    tue6 = mon6 + 86400
    tue8 = mon8 + 86400
    wed6 = tue6 + 86400
    wed8 = tue8 + 86400
    thu6 = wed6 + 86400
    thu8 = wed8 + 86400
    fir6 = thu6 + 86400
    fir8 = thu8 + 86400

    #GETS TOTAL BUSSES FOR MONDAY
    early_morn_count = ''' 
                        SELECT DISTINCT day_date.bus_route, COUNT(day_date.bus_route) AS num_buses_used
                        FROM day_of_week as dayOfWeek
                        INNER JOIN buses_taken as day_date ON day_date.week_id = dayOfWeek.week_id
                        WHERE day_date.depart BETWEEN {} AND {} 
                        OR day_date.depart BETWEEN {} AND {} 
                        OR day_date.depart BETWEEN {} AND {}
                        OR day_date.depart BETWEEN {} AND {}
                        OR day_date.depart BETWEEN {} AND {}
                        GROUP BY day_date.bus_route
                        ORDER BY num_buses_used DESC;'''.format(mon6, mon8, tue6, tue8, wed6, wed8, thu6, thu8, fir6, fir8)
    
    cursor.execute(early_morn_count)
    early_bus_count = cursor.fetchall()
    early_bus_count = pd.DataFrame(early_bus_count)
    early_bus_count = early_bus_count.rename({0: 'Bus_Route', 1: 'Num_Route'}, axis=1)
    # display(early_bus_count)
    # display(early_week_count)
    return early_bus_count

def get_bus_count_individual_days_echo(cursor, startime):
    '''
        Queries the database and gets
        total bus time for 6-8 for each day
        and returns dataframe of results.
    '''
    day = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
    startime = startime + 14400
    bus_count_individual_days_total_frame = pd.DataFrame()

    for x in range(5):
        #print(startime)

        # GETS TOTAL BUSSES FOR DAY
        bus_count_individual_days = ''' 
                            SELECT DISTINCT day_date.bus_route, COUNT(day_date.bus_route) AS num_buses_used
                            FROM day_of_week as dayOfWeek
                            INNER JOIN buses_taken as day_date ON day_date.week_id = dayOfWeek.week_id
                            WHERE day_date.depart BETWEEN {} AND {} 
                            GROUP BY day_date.bus_route
                            ORDER BY num_buses_used DESC;'''.format(startime, startime+7200)
        
        cursor.execute(bus_count_individual_days)
        bus_count_individual = cursor.fetchall()
        bus_count_individual = pd.DataFrame(bus_count_individual)
        #print(bus_count_individual)
        bus_count_individual = bus_count_individual.rename({0: 'Bus_Route', 1: day[x]}, axis=1)

        if x == 0:
            bus_count_individual_days_total_frame = pd.concat([bus_count_individual_days_total_frame, bus_count_individual], axis=1)
        else:
            bus_count_individual_days_total_frame = pd.merge(bus_count_individual_days_total_frame, bus_count_individual, how='left', on = 'Bus_Route')
        
        startime = startime + 86400

    bus_count_individual_days_total_frame = bus_count_individual_days_total_frame.fillna(0)
    bus_count_individual_days_total_frame['Sum'] = bus_count_individual_days_total_frame['Monday'] + \
                                                   bus_count_individual_days_total_frame['Tuesday'] + \
                                                   bus_count_individual_days_total_frame['Wednesday'] + \
                                                   bus_count_individual_days_total_frame['Thursday'] + \
                                                   bus_count_individual_days_total_frame['Friday']
    
    return bus_count_individual_days_total_frame

def get_bus_count_total_week_datetime(cursor, time1, time2):
    '''
        Queries the database and gets
        total buses used for day
        a dataframe of results.
    '''

    #GETS TOTAL BUSSES FOR MONDAY
    early_morn_count = ''' 
                        SELECT DISTINCT day_date.bus_route, COUNT(day_date.bus_route) AS num_buses_used
                        FROM day_of_week as dayOfWeek
                        INNER JOIN buses_taken as day_date ON day_date.week_id = dayOfWeek.week_id
                        WHERE day_date.depart BETWEEN '{}' AND '{}' 
                        GROUP BY day_date.bus_route
                        ORDER BY num_buses_used DESC;'''.format(time1, time2)
    
    cursor.execute(early_morn_count)
    early_bus_count = cursor.fetchall()
    early_bus_count = pd.DataFrame(early_bus_count)
    early_bus_count = early_bus_count.rename({0: 'Bus_Route', 1: 'Num_Route'}, axis=1)
    # display(early_bus_count)
    # display(early_week_count)
    return early_bus_count

def get_bus_count_individual_days_datetime(cursor, time1, time2):
    '''
        Queries the database and gets
        total bus time for x-x+2 for each day
        and returns dataframe of results.
    '''
    day = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
    bus_count_individual_days_total_frame = pd.DataFrame()

    for x in range(5):
        #print(startime)

        # GETS TOTAL BUSSES FOR DAY
        bus_count_individual_days = ''' 
                                    SELECT day_date.bus_route, COUNT(day_date.bus_route) AS num_buses_used
                                    FROM day_of_week as dayOfWeek
                                    INNER JOIN buses_taken as day_date ON day_date.week_id = dayOfWeek.week_id
                                    WHERE day_date.depart BETWEEN '{}' AND '{}' AND dayOfWeek.day_of_week='{}'
                                    GROUP BY day_date.bus_route
                                    ORDER BY num_buses_used DESC;'''.format(time1, time2, day[x])
        
        cursor.execute(bus_count_individual_days)
        bus_count_individual = cursor.fetchall()
        bus_count_individual = pd.DataFrame(bus_count_individual)
        #print(bus_count_individual)
        bus_count_individual = bus_count_individual.rename({0: 'Bus_Route', 1: day[x]}, axis=1)

        if x == 0:
            bus_count_individual_days_total_frame = pd.concat([bus_count_individual_days_total_frame, bus_count_individual], axis=1)
        else:
            bus_count_individual_days_total_frame = pd.merge(bus_count_individual_days_total_frame, bus_count_individual, how='left', on = 'Bus_Route')

    bus_count_individual_days_total_frame = bus_count_individual_days_total_frame.fillna(0)
    bus_count_individual_days_total_frame['Sum'] = bus_count_individual_days_total_frame['Monday'] + \
                                                   bus_count_individual_days_total_frame['Tuesday'] + \
                                                   bus_count_individual_days_total_frame['Wednesday'] + \
                                                   bus_count_individual_days_total_frame['Thursday'] + \
                                                   bus_count_individual_days_total_frame['Friday']
    
    return bus_count_individual_days_total_frame

def main():
    '''
        Calls the postgresql database
        gets the averages and saves 
        the results to excel file.
    '''
    # Get echo time
    # eg. echo_time = 1680501600
    #echo_time = get_echo_time()

    # Get database credentials
    database_name, username, password = get_postgresql_credentials()
    
    # Get output filename
    # eg. output_filename = 'query_output_winter.xlsx'
    output_filename = input("Enter output filename (eg. query2_output.xlsx): ")
    
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
    
    results = postal_code_count(cursor)
    avg_results = average_distance_and_commute_time(cursor)
    coords = get_coords_heatmap(cursor)

    '''
    six_eightAM = get_bus_count_total_week_echo(cursor, startime=echo_time)
    eight_tenAM = get_bus_count_total_week_echo(cursor, startime=echo_time+7200)
    ten_twelveAM = get_bus_count_total_week_echo(cursor, startime=echo_time+14400)

    individual_6_to_8 = get_bus_count_individual_days_echo(cursor, startime=echo_time)
    individual_8_to_10 = get_bus_count_individual_days_echo(cursor, startime=echo_time+7200)
    individual_10_to_12 = get_bus_count_individual_days_echo(cursor, startime=echo_time+14400)
    '''

    six_eightAM = get_bus_count_total_week_datetime(cursor, time1="06:00:00", time2="08:00:00")
    eight_tenAM = get_bus_count_total_week_datetime(cursor, time1="08:00:00", time2="10:00:00")
    ten_twelveAM = get_bus_count_total_week_datetime(cursor, time1="10:00:00", time2="12:00:00")

    individual_6_to_8 = get_bus_count_individual_days_datetime(cursor, time1="06:00:00", time2="08:00:00")
    individual_8_to_10 = get_bus_count_individual_days_datetime(cursor, time1="08:00:00", time2="10:00:00")
    individual_10_to_12 = get_bus_count_individual_days_datetime(cursor, time1="10:00:00", time2="12:00:00")

    '''
    print("6AM to 8AM:")
    display(individual_6_to_8.head(10))
    print('\n')

    print("8AM to 10AM:")
    display(individual_8_to_10.head(10))
    print('\n')

    print("10AM to 12PM:")
    display(individual_10_to_12.head(10))
    print('\n')
    '''

    # SAVES ALL QUERY DATA EXCEL FILE TO MAKE GRAPHS
    with pd.ExcelWriter(output_filename) as writer:  
        results.to_excel(writer, sheet_name='postal_code_per_student', index=False)
        avg_results.to_excel(writer, sheet_name='average_distance_and_commute_time', index=False)
        coords.to_excel(writer, sheet_name='lat_long_heatmap', index=False)
        six_eightAM.to_excel(writer, sheet_name='six_to_eight_AM', index=False)
        eight_tenAM.to_excel(writer, sheet_name='eight_to_ten_AM', index=False)
        ten_twelveAM.to_excel(writer, sheet_name='ten_to_twelve_PM', index=False)
        individual_6_to_8.to_excel(writer, sheet_name='individual_day_bus_count_6_8', index=False)
        individual_8_to_10.to_excel(writer, sheet_name='individual_day_bus_count_8_10', index=False)
        individual_10_to_12.to_excel(writer, sheet_name='individual_day_bus_count_10_12', index=False)

# Runs program if its the main file.
if __name__ == "__main__":
    main()