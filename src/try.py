from sqlalchemy import and_, create_engine, Column, Integer, String
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base
import json
import time
import pandas as pd


Base = declarative_base()
data = json.load(open("src/data.json"))
adddata = json.load(open("src/adddata.json"))
engine1 = create_engine("sqlite:///src/airql.db")
engine2 = create_engine("sqlite:///src/airql2.db")


data = sorted(data, key=lambda k: k["Date"])
adddata = sorted(adddata, key=lambda k: k["Date"])


class Station(Base):
    __tablename__ = "station"

    id = Column(String, primary_key=True)
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
    pm10 = Column(Integer)
    so2 = Column(Integer)
    no2 = Column(Integer)
    co = Column(Integer)
    o3 = Column(Integer)
    pm25 = Column(Integer)


Base.metadata.create_all(engine1)  # create table
Base.metadata.create_all(engine2)  # create table


get_last_date_from_db = engine2.execute(
    "SELECT date FROM station_data ORDER BY date DESC LIMIT 1"
).fetchone()
last_date = get_last_date_from_db[0]
data1 = [x for x in adddata if x["Date"] > last_date]

dataf = pd.DataFrame(
    data1, columns=["StationId", "Date", "PM10", "SO2", "NO2", "O3", "CO", "PM25"]
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
start = time.time()
dataf.to_sql("station_data", engine2, if_exists="append", index=False)
end = time.time()


with Session(bind=engine1) as session:
    start = time.time()
    for data in data:
        exists_query = session.query(
            session.query(StationData)
            .filter(
                and_(
                    StationData.station_id == data["StationId"],
                    StationData.date == data["Date"],
                )
            )
            .exists()
        ).scalar()
        if not exists_query:
            new_station_data = StationData(
                station_id=data["StationId"],
                date=data["Date"],
                pm10=data["PM10"],
                so2=data["SO2"],
                no2=data["NO2"],
                o3=data["O3"],
                co=data["CO"],
                pm25=data["PM25"],
            )
            session.add(new_station_data)
            session.commit()
    end = time.time()
    print(f"Time: {end - start}")
