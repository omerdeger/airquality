# AirQuality
## _Weather monitoring station for Türkiye_

AirQuality is crawl some air data for türkiye on web, especially [havaizleme](https://www.havaizleme.gov.tr/). Purpose of the tool (for now), get last one year data and store on SQLite db.

## To-do

- Exporting and saving meaningful data
- cli integration
- AI based
- Splitting file and method, Make Class method for scrapper

## Tech

Airquality uses a number of open source projects to work properly:

- [Python] - lets you work quickly and integrate systems more effectively!
- [SQLite] - small, fast, self-contained, high-reliability, full-featured, SQL database engine
- [SQLAlchemy] - Python SQL toolkit and Object Relational Mapper that gives application developers the full power and flexibility of SQL
- [Requests] - Elegant and simple HTTP library for Python
- [Pandas] - Data analysis and manipulation tool


## Installation and Runnig

AirQuality requires [Python] 3.9+ to run.

Install the dependencies and devDependencies and start the server.

```sh
pip install -r requirements.txt
```
```sh
python scraper.py
```
and watch

## Development

Want to contribute? Great!

Airquality uses venv, flake8 and black for fast developing.
Make a change in your file and instantaneously see your updates!

Open your favorite Terminal and run these commands.

```sh
python -m venv venv
```
```sh
pip install -r requirements.txt
```


## License

MIT

**Free Software, Hell Yeah!**

   [python]: <https://www.python.org/>
   [SQLite]: <https://www.sqlite.org/index.html>
   [SQLAlchemy]: <https://www.sqlalchemy.org/>
   [Requests]: <https://docs.python-requests.org/>
   [Pandas]: <https://pandas.pydata.org/>