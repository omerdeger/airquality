from datetime import datetime, timedelta
import pandas as pd
import requests
from sqlalchemy import create_engine, inspect

engine = create_engine('sqlite:///airquality/src/airquality3day.db')
url_base = "https://www.havaizleme.gov.tr/"
now = datetime.now()


def station_list():
    if table_exists(engine, 'stations'):
        station_list = engine.execute("SELECT id FROM stations").fetchall()
    else:
        get_station_list()
        update_station_list()
        station_list = engine.execute(
            "SELECT id FROM stations").fetchall()
    return station_list


def get_station_list():
    url = url_base + "Services/GetAirQualityStations?type=0"
    form_data = {
        'Year': now.year,
        'Month': now.month,
        'Day': now.day,
        'Hour': now.hour
    }
    # Gettin data in the url and listing json format.
    station_list = requests.get(url, data=form_data)
    station_list_df = pd.DataFrame(station_list.json()['objects'])
    # Gettin location lat and long.
    station_list_df['Location'] = station_list_df['Location'].apply(
        lambda x: x.strip("POINT ()"))
    station_list_df[['lat', 'long']] = station_list_df['Location'].str.split(
        " ", expand=True,)
    # Making new DataFrame
    station_list_df = station_list_df[[
        'id', 'lat', 'long', 'Name', 'City_Title', 'CityId', 'Town_Title']]

    return station_list_df


def update_station_list():
    get_station_list().to_sql(
        'stations', con=engine,
        if_exists='replace', index_label='id', index=False)


def x_year_ago(x):
    days_per_year = 365.25
    start_time = now - timedelta(days_per_year*x)
    return start_time


def table_exists(engine, name):
    ins = inspect(engine)
    check_table = ins.dialect.has_table(engine.connect(), name)
    print('Table "{}" exists: {}'.format(name, check_table))
    return check_table


def get_last_date_hour(station_id):
    start_time = now
    if table_exists(engine, "station_detail"):
        result = engine.execute(
            """
                SELECT Date
                FROM station_detail
                WHERE stationId = ?
                ORDER BY Date DESC""", (station_id[0],)).fetchone()
        print(station_id[0], result)
        if result:
            last_table_time = datetime.fromisoformat(result[0])
            print(last_table_time)
            start_time = last_table_time + timedelta(hours=1)
            if start_time > now:
                start_time = now
        else:
            start_time = x_year_ago(1)
    else:
        start_time = x_year_ago(1)
    return start_time


def get_station_detail(station_id_list):
    url = url_base + "Services/GetAirQualityStationDetail?type=0"
    for k, station_id in (enumerate(station_id_list)):
        time = get_last_date_hour(station_id)
        while time <= now:
            form_data = {
                'stationId': station_id[0],
                'Year': time.year,
                'Month': time.month,
                'Day': time.day,
                'Hour': time.hour
            }
            station_detail = requests.get(url, data=form_data)
            AQIValues_station = pd.DataFrame(
                station_detail.json()['objects']['AQIValues'])
            print(k+1, "/", len(station_id_list),
                  time.year, time.month, time.day, time.hour)
            AQIValues_station.to_sql(
                'station_detail', con=engine,
                if_exists='append', index=False)
            time += timedelta(hours=72)


if __name__ == "__main__":
    get_station_detail(station_list())
