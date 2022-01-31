from datetime import datetime, timedelta
import requests
from sqlalchemy import create_engine, inspect, select
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
import time


engine = create_engine("sqlite:///src/data.sqlite3")
Base = declarative_base()
url_base = "https://www.havaizleme.gov.tr/"
now = datetime.now()
session = Session(bind=engine)


class Station(Base):
    __tablename__ = "station"

    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(String)
    name = Column(String)
    city = Column(String)
    town = Column(String)
    lat = Column(String)
    long = Column(String)


class StationData(Base):
    __tablename__ = "station_data"

    id = Column(Integer, primary_key=True)
    station_id = Column(String)
    date = Column(String)
    co = Column(String)
    no2 = Column(String)
    o3 = Column(String)
    pm25 = Column(String)
    pm10 = Column(Integer)
    so2 = Column(Integer)


def station_list():
    if table_exists(engine, "station"):
        station_list = engine.execute(select(Station.station_id)).fetchall()
    else:
        get_station_list()
        station_list = engine.execute(select(Station.station_id)).fetchall()
    return station_list


def get_station_list():
    Base.metadata.create_all(engine)  # create table
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
    start_time = time.time()
    for k, station_id in enumerate(station_id_list):
        last_time = get_last_date_hour(station_id)
        start_time_st = time.time()
        while last_time <= now:
            start_time_ex = time.time()
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
                time.time() - start_time,
                time.time() - start_time_st,
                time.time() - start_time_ex,
            )
            last_time += timedelta(hours=73)


if __name__ == "__main__":
    get_station_detail_to_sql(station_list())
