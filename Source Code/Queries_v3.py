from IPython.display import display
import pandas as pd
import numpy as np
from tqdm import tqdm
import psycopg2
import colorama
from colorama import Fore

from Queries_v2 import get_echo_time, get_postgresql_credentials, get_bus_count_individual_days_echo, get_bus_count_individual_days_datetime

def main():
    '''
        Calls the postgresql database
        gets the averages and saves 
        the results to excel file.
    '''

    # Get echo time
    # eg. echo_time = 1680501600
    #echo_time = get_echo_time()
    echo_time = 1680501600
    
    # Get database credentials
    database_name, username, password = get_postgresql_credentials()
    
    # Get output filename
    # eg. output_filename = 'query_output_winter.xlsx'
    output_filename = input("Enter output filename (eg. query3_output.xlsx): ")
    
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

    '''hours = 3600
    individual_6_to_8 = get_bus_count_individual_days_echo(cursor, startime=echo_time)
    individual_8_to_10 = get_bus_count_individual_days_echo(cursor, startime=echo_time+(hours*2))
    individual_10_to_12 = get_bus_count_individual_days_echo(cursor, startime=echo_time+(hours*4))
    individual_12_to_14 = get_bus_count_individual_days_echo(cursor, startime=echo_time+(hours*6))
    individual_14_to_16 = get_bus_count_individual_days_echo(cursor, startime=echo_time+(hours*8))
    individual_16_to_18 = get_bus_count_individual_days_echo(cursor, startime=echo_time+(hours*10))
    individual_18_to_20 = get_bus_count_individual_days_echo(cursor, startime=echo_time+(hours*12))'''

    individual_6_to_8 = get_bus_count_individual_days_datetime(cursor, time1="06:00:00", time2="08:00:00")
    individual_8_to_10 = get_bus_count_individual_days_datetime(cursor, time1="08:00:00", time2="10:00:00")
    individual_10_to_12 = get_bus_count_individual_days_datetime(cursor, time1="10:00:00", time2="12:00:00")
    individual_12_to_14 = get_bus_count_individual_days_datetime(cursor, time1="12:00:00", time2="14:00:00")
    individual_14_to_16 = get_bus_count_individual_days_datetime(cursor, time1="14:00:00", time2="16:00:00")
    individual_16_to_18 = get_bus_count_individual_days_datetime(cursor, time1="16:00:00", time2="18:00:00")
    individual_18_to_20 = get_bus_count_individual_days_datetime(cursor, time1="18:00:00", time2="20:00:00")

    print("Saving...")
    # SAVES ALL QUERY DATA EXCEL FILE TO MAKE GRAPHS
    with pd.ExcelWriter(output_filename) as writer:  
        individual_6_to_8.to_excel(writer, sheet_name='individual_day_bus_count_6_8', index=False)
        individual_8_to_10.to_excel(writer, sheet_name='individual_day_bus_count_8_10', index=False)
        individual_10_to_12.to_excel(writer, sheet_name='individual_day_bus_count_10_12', index=False)
        individual_12_to_14.to_excel(writer, sheet_name='individual_day_bus_count_12_14', index=False)
        individual_14_to_16.to_excel(writer, sheet_name='individual_day_bus_count_14_16', index=False)
        individual_16_to_18.to_excel(writer, sheet_name='individual_day_bus_count_16_18', index=False)
        individual_18_to_20.to_excel(writer, sheet_name='individual_day_bus_count_18_20', index=False)
    print("Complete...")

if __name__ == "__main__":
    main()