from sqlalchemy import and_, create_engine, Column, Integer, ForeignKey, String
from sqlalchemy.orm import relationship, Session
from sqlalchemy.ext.declarative import declarative_base
import json

Base = declarative_base()
data = json.load(open('airquality/src/data.json'))
engine = create_engine('sqlite:///airql.db')


class Station(Base):
    __tablename__ = 'station'

    id = Column(String, primary_key=True)
    name = Column(String)
    city = Column(String)
    town = Column(String)
    lat = Column(String)
    long = Column(String)
    datas = relationship("StationData", backref="station")

    def __repr__(self):
        return f"User(id={self.id!r}, name={self.name!r})"


class StationData(Base):
    __tablename__ = 'station_data'

    id = Column(Integer, primary_key=True)
    station_id = Column(String, ForeignKey('station.id'))
    date = Column(String)
    pm10 = Column(Integer)
    so2 = Column(Integer)

    def __repr__(self):
        return f"Station Data(id={self.station_id!r}, date={self.date!r})"


Base.metadata.create_all(engine)  # create table

with Session(bind=engine) as session:
    for data in data:
        exists_query = session.query(session.query(StationData).filter(and_(
            StationData.station_id == data['StationId'],
            StationData.date == data['Date'])).exists()).scalar()
        print(exists_query, data['StationId'], data['Date'])
        if not exists_query:
            new_station_data = StationData(
                station_id=data['StationId'],
                date=data['Date'],
                pm10=data['PM10'],
                so2=data['SO2']
            )
            session.add(new_station_data)
            session.commit()
            print(f"Added {data['StationId']} {data['Date']}")
        else:
            print(f"{data['StationId']} {data['Date']} already exists")
