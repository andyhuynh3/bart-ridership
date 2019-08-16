import logging

import pandas as pd
import requests
from bs4 import BeautifulSoup

from settings import BART_API_TOKEN, engine


class StationInformationParser:

    STATION_URL_BASE = "https://www.bart.gov/stations"
    BART_STATION_API_BASE = f"http://api.bart.gov/api/stn.aspx?cmd=stninfo&key={BART_API_TOKEN}&json=y&orig="

    def __init__(self, station):

        station_url_response = requests.get(
            f"{self.STATION_URL_BASE}/{station.lower()}"
        )
        station_url_content = station_url_response.content
        self.soup = BeautifulSoup(station_url_content, "lxml")
        self.all_links = self.soup.find_all("a")

        station_api_response = requests.get(
            f"{self.BART_STATION_API_BASE}{station.lower()}"
        )
        self.station_api_content = station_api_response.json()["root"]["stations"][
            "station"
        ]

    def get_latitude(self):
        return self.station_api_content["gtfs_latitude"]

    def get_longitude(self):
        return self.station_api_content["gtfs_longitude"]

    def get_address(self):
        return self.station_api_content["address"]

    def get_city(self):
        return self.station_api_content["city"]

    def get_county(self):
        return self.station_api_content["county"]

    def get_state(self):
        return self.station_api_content["state"]

    def get_zipcode(self):
        return self.station_api_content["zipcode"]

    def get_full_address(self):
        full_address = (
            self.get_address()
            + ", "
            + self.get_city()
            + ", "
            + self.get_state()
            + " "
            + self.get_zipcode()
        )
        return full_address

    def get_attraction(self):
        return self.station_api_content["attraction"].get("#cdata-section", "")

    def get_cross_street(self):
        return self.station_api_content["cross_street"].get("#cdata-section", "")

    def get_food(self):
        return self.station_api_content["food"].get("#cdata-section", "")

    def get_intro(self):
        return self.station_api_content["intro"].get("#cdata-section", "")

    def get_link(self):
        return self.station_api_content["link"].get("#cdata-section", "")

    def get_north_platforms(self):
        north_platforms = self.station_api_content["north_platforms"]
        if isinstance(north_platforms, dict):
            return north_platforms.get("platform", "")
        else:
            return north_platforms

    def get_north_routes(self):
        north_routes = self.station_api_content["north_routes"]
        if isinstance(north_routes, dict):
            return north_routes.get("route", "")
        else:
            return north_routes

    def get_platform_info(self):
        return self.station_api_content["platform_info"]

    def get_shopping(self):
        return self.station_api_content["shopping"].get("#cdata-section")

    def get_south_platforms(self):
        south_platforms = self.station_api_content["south_platforms"]
        if isinstance(south_platforms, dict):
            return south_platforms.get("platform", "")
        else:
            return south_platforms

    def get_south_routes(self):
        south_routes = self.station_api_content["south_routes"]
        if isinstance(south_routes, dict):
            return south_routes.get("route", "")
        else:
            return south_routes

    def get_station_map_url(self):
        for link in self.all_links:
            if "Station Map (PDF)" in link.get_text():
                station_map_url = link.get("href")
                break
        return station_map_url


def create_dim_station():
    # Basic station information (name and abbreviations)
    station_abbreviation_url = "https://api.bart.gov/docs/overview/abbrev.aspx"
    station_abbreviation_df = pd.read_html(station_abbreviation_url)[0]
    station_abbreviation_df.index += 1
    station_abbreviation_df.columns = ["abbreviation_lower", "name"]
    station_abbreviation_df["abbreviation"] = station_abbreviation_df[
        "abbreviation_lower"
    ].str.upper()
    station_abbreviation_df = station_abbreviation_df[
        ["abbreviation", "abbreviation_lower", "name"]
    ]
    station_abbreviation_df["station_parser"] = station_abbreviation_df[
        "abbreviation_lower"
    ].apply(StationInformationParser)
    get_methods = [
        method for method in dir(StationInformationParser) if "get_" in method
    ]
    for get_method in get_methods:
        column_name = "_".join(get_method.split("_")[1:])
        station_abbreviation_df[column_name] = station_abbreviation_df[
            "station_parser"
        ].apply(lambda x: getattr(x, get_method)())

    station_abbreviation_df.drop(columns=["station_parser"], inplace=True)
    station_abbreviation_df.to_sql(
        name="dim_station",
        schema="bart",
        if_exists="replace",
        con=engine,
        index_label="id",
    )
    create_index_stmt = (
        "CREATE INDEX idx_abbreviation_ds ON bart.dim_station (abbreviation);"
    )
    logging.info(create_index_stmt)
    engine.execute(create_index_stmt)
    logging.info("Created bart.dim_station")


def create_bart_schema():
    create_schema_sql = "CREATE SCHEMA IF NOT EXISTS bart"
    logging.info(create_schema_sql)
    engine.execute(create_schema_sql)


def create_dim_date():
    sql_stmts = [
        "DROP TABLE IF EXISTS bart.dim_date",
        """
        CREATE TABLE bart.dim_date
        (
        date_id                  INT NOT NULL,
        date                     DATE NOT NULL,
        epoch                    BIGINT NOT NULL,
        day_suffix               VARCHAR(4) NOT NULL,
        day_name                 VARCHAR(9) NOT NULL,
        day_of_week              INT NOT NULL,
        day_of_month             INT NOT NULL,
        day_of_quarter           INT NOT NULL,
        day_of_year              INT NOT NULL,
        week_of_month            INT NOT NULL,
        week_of_year             INT NOT NULL,
        week_of_year_iso         CHAR(10) NOT NULL,
        month_actual             INT NOT NULL,
        month_name               VARCHAR(9) NOT NULL,
        month_name_abbreviated   CHAR(3) NOT NULL,
        quarter_actual           INT NOT NULL,
        quarter_name             VARCHAR(9) NOT NULL,
        year_actual              INT NOT NULL,
        first_day_of_week        DATE NOT NULL,
        last_day_of_week         DATE NOT NULL,
        first_day_of_month       DATE NOT NULL,
        last_day_of_month        DATE NOT NULL,
        first_day_of_quarter     DATE NOT NULL,
        last_day_of_quarter      DATE NOT NULL,
        first_day_of_year        DATE NOT NULL,
        last_day_of_year         DATE NOT NULL,
        mmyyyy                   CHAR(6) NOT NULL,
        mmddyyyy                 CHAR(10) NOT NULL,
        weekend_indr             BOOLEAN NOT NULL
        );
        """,
        "ALTER TABLE bart.dim_date ADD CONSTRAINT idx_date_id PRIMARY KEY (date_id);",
        "CREATE INDEX idx_date ON bart.dim_date(date);",
        """
        INSERT INTO bart.dim_date
        SELECT TO_CHAR(datum,'yyyymmdd')::INT AS date_id,
            datum AS date,
            EXTRACT(epoch FROM datum) AS epoch,
            TO_CHAR(datum,'fmDDth') AS day_suffix,
            TO_CHAR(datum,'Day') AS day_name,
            EXTRACT(isodow FROM datum) AS day_of_week,
            EXTRACT(DAY FROM datum) AS day_of_month,
            datum - DATE_TRUNC('quarter',datum)::DATE +1 AS day_of_quarter,
            EXTRACT(doy FROM datum) AS day_of_year,
            TO_CHAR(datum,'W')::INT AS week_of_month,
            EXTRACT(week FROM datum) AS week_of_year,
            TO_CHAR(datum,'YYYY"-W"IW-') || EXTRACT(isodow FROM datum) AS week_of_year_iso,
            EXTRACT(MONTH FROM datum) AS month_actual,
            TO_CHAR(datum,'Month') AS month_name,
            TO_CHAR(datum,'Mon') AS month_name_abbreviated,
            EXTRACT(quarter FROM datum) AS quarter_actual,
            CASE
                WHEN EXTRACT(quarter FROM datum) = 1 THEN 'First'
                WHEN EXTRACT(quarter FROM datum) = 2 THEN 'Second'
                WHEN EXTRACT(quarter FROM datum) = 3 THEN 'Third'
                WHEN EXTRACT(quarter FROM datum) = 4 THEN 'Fourth'
            END AS quarter_name,
            EXTRACT(isoyear FROM datum) AS year_actual,
            datum +(1 -EXTRACT(isodow FROM datum))::INT AS first_day_of_week,
            datum +(7 -EXTRACT(isodow FROM datum))::INT AS last_day_of_week,
            datum +(1 -EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
            (DATE_TRUNC('MONTH',datum) +INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
            DATE_TRUNC('quarter',datum)::DATE AS first_day_of_quarter,
            (DATE_TRUNC('quarter',datum) +INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
            TO_DATE(EXTRACT(isoyear FROM datum) || '-01-01','YYYY-MM-DD') AS first_day_of_year,
            TO_DATE(EXTRACT(isoyear FROM datum) || '-12-31','YYYY-MM-DD') AS last_day_of_year,
            TO_CHAR(datum,'mmyyyy') AS mmyyyy,
            TO_CHAR(datum,'mmddyyyy') AS mmddyyyy,
            CASE
                WHEN EXTRACT(isodow FROM datum) IN (6,7) THEN TRUE
                ELSE FALSE
            END AS weekend_indr
        FROM (SELECT '2001-01-01'::DATE+ SEQUENCE.DAY AS datum
            FROM GENERATE_SERIES (0,29219) AS SEQUENCE (DAY)
            GROUP BY SEQUENCE.DAY) DQ
        ORDER BY 1;
        """,
    ]
    for sql_stmt in sql_stmts:
        logging.info(sql_stmt)
        engine.execute(sql_stmt)


if __name__ == "__main__":
    create_bart_schema()
    create_dim_date()
    create_dim_station()
