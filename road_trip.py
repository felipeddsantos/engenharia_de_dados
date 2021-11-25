'''

Engenharia de Dados - Construção de Fluxo de Dados com AirFlow
Felipe Daniel Dias dos Santos - 11711ECP004
Graduação em Engenharia de Computação - Faculdade de Engenharia Elétrica - Universidade Federal de Uberlândia

'''

import haversine as hs
from datetime import datetime
from airflow import DAG
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

dag = DAG(
    dag_id = "road_trip",
    schedule_interval = "@daily",
    start_date = datetime(2021, 10, 17),
    catchup = False)

def tokenizer_timestamp(timestamp):

    [timestamp_date, timestamp_time] = timestamp.split()
    [timestamp_month, timestamp_day, timestamp_year] = timestamp_date.split('.')
    [timestamp_hour, timestamp_minute] = timestamp_time.split(':')
    
    return datetime(int(timestamp_year), int(timestamp_month), int(timestamp_day), int(timestamp_hour), int(timestamp_minute))

@dag.task(task_id = "get_table_from_database")
def get_table_from_database():

    db = SqliteHook()
    return db.get_records("SELECT * FROM road_trip;")

@dag.task(task_id = "get_coordinates")
def get_coordinates(**kwargs):

    ti = kwargs["ti"]
    table = ti.xcom_pull(task_ids = "get_table_from_database")
    coordinates = []

    for row in table:

        latitude = row[3]
        longitude = row[2]
        coordinates.append((latitude, longitude)) 
        
    return coordinates

@dag.task(task_id = "get_time_data")
def get_time_data(**kwargs):

    ti = kwargs["ti"]
    table = ti.xcom_pull(task_ids = "get_table_from_database")
    time_data = []
    
    for row in table:

        arrived = row[4]
        departed = row[6]
        time_data.append((arrived, departed))

    return time_data

@dag.task(task_id = "get_gallons_of_gas_data")
def get_gallons_of_gas_data(**kwargs):

    ti = kwargs["ti"]
    table = ti.xcom_pull(task_ids = "get_table_from_database")
    gallons_of_gas_data = []

    for row in table:

        gallons_at_arrival = row[5]
        gallons_at_departure = row[7]
        gallons_of_gas_data.append((gallons_at_arrival, gallons_at_departure))
    
    return gallons_of_gas_data

@dag.task(task_id = "compute_distances")
def compute_distances(**kwargs):
    
    ti = kwargs["ti"]
    coordinates = ti.xcom_pull(task_ids = "get_coordinates")
    distances = []

    for row in range(len(coordinates) - 1):

        distances.append(hs.haversine(coordinates[row], coordinates[row + 1], unit = "mi"))
        
    return distances

@dag.task(task_id = "compute_drive_times")
def compute_drive_times(**kwargs):
    
    ti = kwargs["ti"]
    time_data = ti.xcom_pull(task_ids = "get_time_data")
    drive_times = []

    for row in range(len(time_data) - 1):

        departed = tokenizer_timestamp(time_data[row][1])
        arrived = tokenizer_timestamp(time_data[row + 1][0])
        drive_time = arrived - departed
        drive_times.append(drive_time.total_seconds() / 3600)
    
    return drive_times

@dag.task(task_id = "compute_speeds")
def compute_speeds(**kwargs):
    
    ti = kwargs["ti"]
    distances = ti.xcom_pull(task_ids = "compute_distances")
    drive_times = ti.xcom_pull(task_ids = "compute_drive_times")
    speeds = []

    for row in range(len(drive_times) - 1):

        speeds.append(distances[row] / drive_times[row])

    return speeds

@dag.task(task_id = "get_highest_speed_segment")
def get_highest_speed_segment(**kwargs):
    
    ti = kwargs["ti"]
    speeds = ti.xcom_pull(task_ids = "compute_speeds")

    return speeds.index(max(speeds))

@dag.task(task_id = "compute_miles_per_gallon")
def compute_miles_per_gallon(**kwargs):
    
    ti = kwargs["ti"]
    highest_speed_segment = ti.xcom_pull(task_ids = "get_highest_speed_segment")
    distances = ti.xcom_pull(task_ids = "compute_distances")
    gallons_of_gas_data = ti.xcom_pull(task_ids = "get_gallons_of_gas_data")

    gas_at_departure = gallons_of_gas_data[highest_speed_segment][1]
    gas_at_arrival = gallons_of_gas_data[highest_speed_segment + 1][0]
    total_gallons_of_gas = gas_at_departure - gas_at_arrival
    
    return distances[highest_speed_segment] / total_gallons_of_gas

get_table_from_database = get_table_from_database()
compute_distances = compute_distances()
compute_speeds = compute_speeds()
get_highest_speed_segment = get_highest_speed_segment()
compute_miles_per_gallon = compute_miles_per_gallon()

get_table_from_database >> get_coordinates() >> compute_distances >> compute_speeds >> get_highest_speed_segment >> compute_miles_per_gallon
get_table_from_database >> get_time_data() >> compute_drive_times() >> compute_speeds >> get_highest_speed_segment >> compute_miles_per_gallon
compute_distances >> compute_miles_per_gallon
get_table_from_database >> get_gallons_of_gas_data() >> compute_miles_per_gallon