import logging
import re

import pandas as pd
import requests
import usaddress
from bs4 import BeautifulSoup

from settings import engine, BART_API_TOKEN


class StationInformationParser:

    STATION_URL_BASE = 'https://www.bart.gov/stations'
    BART_STATION_API = f'http://api.bart.gov/api/stn.aspx?cmd=stns&key={BART_API_TOKEN}&json=y'

    def __init__(self, station):

        station_response = requests.get(f'{self.STATION_URL_BASE}/{station.lower()}')
        station_content = station_response.content
        self.soup = BeautifulSoup(station_content, 'lxml')
        self.all_links = self.soup.find_all('a')
        self.address_parts = None
        self.bikes = None

        api_response = requests.get(self.BART_STATION_API)
        self.api_content = api_response.json()

    def get_station_base_info():
        for station in self.api_content['stations']['stations']:
            name = station['name']
            abbr = station['abbr']
            gtfs_latitude = station['gtfs_latitude']

    def get_station_map_url(self):
        for link in self.all_links:
            if 'Station Map (PDF)' in link.get_text():
                station_map_url = link.get('href')
                break
        return station_map_url

    # def _populate_address_parts(self):
    #     for link in self.all_links:
    #         if re.search(' CA \d{5}', link.get_text()):
    #             preprocessed_address = link.get_text()
    #             break
    #     parsable_address = self._clean_address(preprocessed_address)
    #     address_parts = usaddress.tag(parsable_address)[0]
    #     address_number = address_parts.get('AddressNumber', '')
    #     street_name_pre = address_parts.get('StreetNamePreType', '')
    #     street_name = address_parts.get('StreetName', '') or address_parts.get('BuildingName', '')
    #     full_street_name = f'{street_name_pre} {street_name}'.strip()
    #     post_type = address_parts.get('StreetNamePostType', '')
    #     street_address = (
    #         f"{address_number} {full_street_name} {post_type}".strip()
    #     )
    #     city = address_parts['PlaceName']
    #     state = address_parts['StateName']
    #     zip_code = address_parts['ZipCode']
    #     self.address_parts = {
    #         'street_address': street_address,
    #         'city': city,
    #         'state': state,
    #         'zip_code': zip_code
    #     }

    # def _clean_address(self, preprocessed_address):
    #     # SFO is an edge case
    #     if 'International Terminal' in preprocessed_address:
    #         return 'San Francisco CA 94128'
    #     else:
    #         address, misc = preprocessed_address.split(',')
    #     state_zip = ' '.join(misc.split(' ')[1:])
    #     cleaned_address = address + ', ' + state_zip
    #     return cleaned_address

    # def get_street_address(self):
    #     if self.address_parts is None:
    #         self._populate_address_parts()
    #     return self.address_parts['street_address']

    # def get_city(self):
    #     if self.address_parts is None:
    #         self._populate_address_parts()
    #     return self.address_parts['city']

    # def get_station_state(self):
    #     if self.address_parts is None:
    #         self._populate_address_parts()
    #     return self.address_parts['state']

    # def get_station_zip_code(self):
    #     if self.address_parts is None:
    #         self._populate_address_parts()
    #     return self.address_parts['zip_code']

    # def get_full_address(self):
    #     if self.address_parts is None:
    #         self._populate_address_parts()
    #     if self.address_parts['street_address']:
    #         self.address_parts['street_address'] += ', '
    #     return (
    #         self.address_parts['street_address']
    #         + self.address_parts['city']
    #         + ', '
    #         + self.address_parts['state']
    #         + ' '
    #         + self.address_parts['zip_code']
    #     )

    # def get_has_bike_racks(self):
    #     pass


def craete_dim_station():
    # Basic station information (name and abbreviations)
    station_abbreviation_url = 'https://api.bart.gov/docs/overview/abbrev.aspx'
    station_abbreviation_df = pd.read_html(station_abbreviation_url)[0]
    station_abbreviation_df.index += 1
    station_abbreviation_df.columns = ['abbreviation_lower', 'name']
    station_abbreviation_df['abbreviation'] = station_abbreviation_df['abbreviation_lower'].str.upper()
    station_abbreviation_df = station_abbreviation_df[['abbreviation', 'abbreviation_lower', 'name']]
    station_abbreviation_df['station_url'] = 'https://www.bart.gov/stations/' + station_abbreviation_df['abbreviation_lower']
    station_abbreviation_df['station_parser'] = station_abbreviation_df['abbreviation_lower'].apply(StationInformationParser)
    station_abbreviation_df['street_address'] = station_abbreviation_df['station_parser'].apply(lambda x: x.get_street_address())
    station_abbreviation_df['city'] = station_abbreviation_df['station_parser'].apply(lambda x: x.get_city())
    station_abbreviation_df['state'] = station_abbreviation_df['station_parser'].apply(lambda x: x.get_station_state())
    station_abbreviation_df['zip_code'] = station_abbreviation_df['station_parser'].apply(lambda x: x.get_station_zip_code())
    station_abbreviation_df['full_address'] = station_abbreviation_df['station_parser'].apply(lambda x: x.get_full_address())
    station_abbreviation_df['station_map_url'] = station_abbreviation_df['station_parser'].apply(lambda x: x.get_station_map_url())
    station_abbreviation_df.drop(columns=['station_parser'], inplace=True)
    station_abbreviation_df.to_sql(name='dim_station', schema='bart', if_exists='replace', con=engine, index_label='id')
    logging.info('Created bart.dim_station')


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
        """
    ]
    for sql_stmt in sql_stmts:
        logging.info(sql_stmt)
        engine.execute(sql_stmt)


if __name__ == '__main__':
    create_bart_schema()
    create_dim_date()
    craete_dim_station()
