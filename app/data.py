from sqlalchemy import engine
import pandas as pd


class BartRidershipData:
    def __init__(self, connection: engine):
        self.connection = connection

    def sql_to_df(self, sql):
        return pd.read_sql(sql, con=self.connection)

    def get_ridership_by_date(self, date):
        date_id = date.replace("-", "")
        sql = f"""
        SELECT
            date,
            hour,
            origin.name AS origin_station,
            dest.name AS destination_station,
            trip_counter
        FROM bart.fact_ridership fr
        JOIN bart.dim_date dd ON 1=1
            AND fr.date_id = dd.date_id
        JOIN bart.dim_station origin ON 1=1
            AND fr.origin_station_id = origin.id
        JOIN bart.dim_station dest  ON 1=1
            AND fr.destination_station_id = dest.id
        WHERE fr.date_id = '{date_id}'
        """
        return self.sql_to_df(sql)

    def get_ridership_data_by_date(self, date="2011-01-01"):
        date_id = date.replace("-", "")
        sql = f"""
        SELECT
            date,
            epoch,
            day_suffix,
            day_name,
            day_of_week,
            day_of_month,
            day_of_quarter,
            week_of_month,
            week_of_year,
            week_of_year_iso,
            month_name,
            month_name_abbreviated,
            quarter_name,
            mmyyyy,
            mmddyyyy,
            weekend_indr,
            hour,
            trip_counter,
            origin.name as origin_station,
            dest.name as destination_station,
            origin.full_address as origin_station_full_address,
            dest.full_address as destination_station_full_address,
            dest.abbreviation as destination_station_abbreviation,
            origin.abbreviation as origin_station_abbreviation,
            dest.abbreviation_lower as destination_station_abbreviation_lower,
            origin.abbreviation_lower as origin_station_abbreviation_lower,
            origin.city as origin_station_city,
            dest.city as destination_station_city,
            origin.cross_street as origin_station_cross_street,
            dest.cross_street as destination_station_cross_street,
            origin.cross_street as origin_station_latitude,
            dest.cross_street as destination_station_latitude,
            origin.cross_street as origin_station_link,
            dest.cross_street as destination_station_link,
            origin.cross_street as origin_station_longitude,
            dest.cross_street as destination_station_longitude,
            origin.cross_street as origin_station_north_routes,
            dest.cross_street as destination_station_north_routes,
            origin.cross_street as origin_station_south_routes,
            dest.cross_street as destination_station_south_routes,
            origin.cross_street as origin_station_south_platforms,
            dest.cross_street as destination_station_south_platforms,
            origin.cross_street as origin_station_north_platforms,
            dest.cross_street as destination_station_north_platforms,
            origin.cross_street as origin_station_state,
            dest.cross_street as destination_station_state,
            origin.cross_street as origin_station_zipcode,
            dest.cross_street as destination_station_zipcode,
            origin.link AS origin_link,
            dest.link AS destination_link,
            origin.station_map_url AS origin_station_map_url,
            dest.station_map_url AS destination_map_url
        FROM bart.fact_ridership fr
        JOIN bart.dim_date dd ON 1=1
            AND fr.date_id = dd.date_id
        JOIN bart.dim_station origin ON 1=1
            AND fr.origin_station_id = origin.id
        JOIN bart.dim_station dest  ON 1=1
            AND fr.destination_station_id = dest.id
        WHERE fr.date_id = {date_id}
        ORDER BY hour, origin_station, destination_station
        """
        return self.sql_to_df(sql)

    def get_date_count_data_by_year(self, year):
        sql = f"""
    SELECT
        date,
        SUM(trip_counter) AS trip_counter
    FROM bart.fact_ridership fr
    JOIN bart.dim_date dd ON 1=1
        AND fr.date_id = dd.date_id
    JOIN bart.dim_station origin ON 1=1
        AND fr.origin_station_id = origin.id
    JOIN bart.dim_station dest  ON 1=1
        AND fr.destination_station_id = dest.id
    WHERE fr.date_id BETWEEN '{year}0101' AND '{year}1231'
    GROUP BY date
    ORDER BY date;
        """
        return self.sql_to_df(sql)

    def get_ridership_by_year(self):
        sql = f"""
    SELECT
        SUBSTRING(date_id::text, 1, 4) AS year,
        SUM(trip_counter) AS trip_counter
    FROM bart.fact_ridership fr
    GROUP BY 1;
        """
        return self.sql_to_df(sql)

    def get_station_lat_lon(self):
        sql = f"""
    SELECT
        latitude,
        longitude,
        abbreviation
    FROM bart.dim_station
        """
        return self.sql_to_df(sql)

    def get_ridership_by_hour_by_station_and_date(
        self, date, station_abb
    ):
        date_id = date.replace("-", "")
        sql = f"""
    SELECT
        hour,
        origin_ridership_total,
        destination_ridership_total
    FROM bart.fact_ridership_by_hour_by_station_by_date
    WHERE 1=1
        AND date_id = '{date_id}'
        AND abbreviation = '{station_abb}'
        """
        return self.sql_to_df(sql)

    def get_ridership_by_hour_by_date(self, date):
        date_id = date.replace("-", "")
        sql = f"""
    SELECT
        hour,
        ridership_total
    FROM bart.fact_ridership_count_by_hour_by_date
    WHERE date_id = '{date_id}'
    ORDER BY hour;
        """
        return self.sql_to_df(sql)

    def get_ridership_by_station_by_date(self, date):
        date_id = date.replace("-", "")
        sql = f"""
        SELECT
            abbreviation,
            latitude,
            longitude,
            origin_count,
            destination_count
        FROM bart.fact_ridership_by_station_by_date
        WHERE date_id = {date_id}
        """
        return self.sql_to_df(sql)

    def get_total_ride_count_by_day(self, date):
        date_id = date.replace("-", "")
        sql = f"""
        SELECT
            cnt
        FROM bart.fact_ridership_count_by_day
        WHERE date_id = '{date_id}'
        """
        return self.sql_to_df(sql)

    def get_station_info(self, station_abb):
        sql = f"""
        SELECT
            name,
            full_address,
            intro,
            link,
            station_map_url
        FROM bart.dim_station
        WHERE abbreviation = '{station_abb}'
        """
        return self.sql_to_df(sql)
