import pandas as pd
import requests
from datetime import datetime, timedelta
from sqlalchemy import inspect, select
from sqlalchemy.orm import Session
from time import time

from database import engine, Station


url_base = "https://www.havaizleme.gov.tr/"
now = datetime.now()


def station_list():
    if table_exists(engine, "station"):
        station_list = engine.execute(select(Station.station_id)).fetchall()
    else:
        get_station_list()
        station_list = engine.execute(select(Station.station_id)).fetchall()
    return station_list


def get_station_list():
    url = url_base + "Services/GetAirQualityStations?type=0"
    form_data = {"Year": now.year, "Month": now.month, "Day": now.day, "Hour": now.hour}
    # Gettin data in the url and listing json format.
    station_list = requests.get(url, data=form_data)
    station_list = station_list.json()["objects"]
    # Creating a list of station objects.

    def lat_long_from_location(station_id):
        stripped_loc = station_id["Location"].strip("POINT ()")
        lat, long = stripped_loc.split(" ")
        return lat, long

    for station in station_list:
        new_station = Station(
            station_id=station["id"],
            name=station["Name"],
            city=station["City_Title"],
            town=station["Town_Title"],
            lat=lat_long_from_location(station)[0],
            long=lat_long_from_location(station)[1],
        )
        with Session(bind=engine) as session:
            exists_query = session.query(
                session.query(Station)
                .filter(Station.station_id == station["id"])
                .exists()
            ).scalar()

            if not exists_query:
                new_station_data = new_station
                session.add(new_station_data)
                session.commit()
                print(f"Added {station['id']} {station['City_Title']}")


def x_year_ago(x):
    days_per_year = 365.25
    start_time = now - timedelta(days_per_year * x)
    return start_time


def table_exists(engine, name):
    ins = inspect(engine)
    check_table = ins.dialect.has_table(engine.connect(), name)
    print('Table "{}" exists: {}'.format(name, check_table))
    return check_table


def get_last_date_hour(station_id):
    start_time = now
    if table_exists(engine, "station_data"):
        result = engine.execute(
            """
                SELECT Date
                FROM station_data
                WHERE station_id = ?
                ORDER BY Date DESC""",
            (station_id[0],),
        ).fetchone()
        if result:
            last_table_time = datetime.fromisoformat(result[0])
            print("get_last_date_hour(2)", last_table_time)
            start_time = last_table_time + timedelta(hours=1)
            if start_time > now:
                start_time = now
        else:
            start_time = x_year_ago(1)
    else:
        start_time = x_year_ago(1)
    return start_time


def get_station_detail_to_sql(station_id_list):
    url = url_base + "Services/GetAirQualityStationDetail?type=0"
    start_time = time()
    for k, station_id in enumerate(station_id_list):
        last_time = get_last_date_hour(station_id)
        start_time_st = time()
        while last_time <= now:
            start_time_ex = time()
            form_data = {
                "stationId": station_id[0],
                "Year": last_time.year,
                "Month": last_time.month,
                "Day": last_time.day,
                "Hour": last_time.hour,
            }
            station_data = requests.get(url, data=form_data)
            station_data = station_data.json()["objects"]["AQIValues"]
            station_data = sorted(station_data, key=lambda k: k["Date"])
            get_last_date_from_db = engine.execute(
                """
                SELECT Date
                FROM station_data
                WHERE station_id = ?
                ORDER BY Date DESC""",
                (station_id[0],),
            ).fetchone()
            if get_last_date_from_db:
                station_data = [
                    x for x in station_data if x["Date"] > get_last_date_from_db[0]
                ]
            dataf = pd.DataFrame(
                station_data,
                columns=["StationId", "Date", "PM10", "SO2", "NO2", "O3", "CO", "PM25"],
            )
            dataf.rename(
                columns={
                    "StationId": "station_id",
                    "Date": "date",
                    "PM10": "pm10",
                    "SO2": "so2",
                    "NO2": "no2",
                    "O3": "o3",
                    "CO": "co",
                    "PM25": "pm25",
                },
                inplace=True,
            )
            dataf.to_sql("station_data", engine, if_exists="append", index=False)
            print(
                k + 1,
                "/",
                len(station_id_list),
                last_time.year,
                last_time.month,
                last_time.day,
                last_time.hour,
                round(time() - start_time, 2),
                round(time() - start_time_st, 2),
                round(time() - start_time_ex, 2),
            )
            last_time += timedelta(hours=1)
            last_time += timedelta(hours=73)


if __name__ == "__main__":
    get_station_detail_to_sql(station_list())
