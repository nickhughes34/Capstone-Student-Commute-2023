from IPython.display import display
import pandas as pd
import numpy as np
from tqdm import tqdm
import psycopg2
import colorama
from colorama import Fore
import datetime as dt

from Queries_v2 import get_postgresql_credentials

def total_buses(cursor):
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
                        GROUP BY day_date.bus_route
                        ORDER BY num_buses_used DESC;'''
    
    cursor.execute(early_morn_count)
    early_bus_count = cursor.fetchall()
    early_bus_count = pd.DataFrame(early_bus_count)
    early_bus_count = early_bus_count.rename({0: 'Bus_Route', 1: 'Num_Route'}, axis=1)
    # display(early_bus_count)
    # display(early_week_count)
    return early_bus_count

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
    output_filename = input("Enter output filename (eg. query5_output.xlsx): ")
    
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

    wait_time = total_buses(cursor)

    print("Saving...")
    # SAVES ALL QUERY DATA EXCEL FILE TO MAKE GRAPHS
    with pd.ExcelWriter(output_filename) as writer:  
        wait_time.to_excel(writer, sheet_name='total_bus_used', index=False)
    print("Complete...")

if __name__ == "__main__":
    main()