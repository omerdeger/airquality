from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

Base = declarative_base()
engine = create_engine("sqlite:///src/data.sqlite3")
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


Base.metadata.create_all(engine)  # create table
