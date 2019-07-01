from settings import engine
import pandas as pd


def sql_to_df(sql, con=engine):
    return pd.read_sql(sql, con=engine)


def get_ridership_by_date(date):
    date_id = date.replace('-', '')
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
    return sql_to_df(sql)


def get_ridership_data(columns, limit=100):
    col_mappings = {
        'origin_station': 'origin.name as origin_station',
        'destination_station': 'dest.name as destination_station',
        'origin_station_full_address': 'origin.full_address as origin_station_full_address',
        'destination_station_full_address': 'dest.full_address as destination_station_full_address',
    }
    columns_str = ','.join([col_mappings.get(col, col) for col in columns])
    sql = f"""
    SELECT
        {columns_str}
    FROM bart.fact_ridership fr
    JOIN bart.dim_date dd ON 1=1
        AND fr.date_id = dd.date_id
    JOIN bart.dim_station origin ON 1=1
        AND fr.origin_station_id = origin.id
    JOIN bart.dim_station dest  ON 1=1
        AND fr.destination_station_id = dest.id
    LIMIT 100
    """
    return sql_to_df(sql)


def get_date_count_data_by_year(year):
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
    return sql_to_df(sql)


def get_ridership_by_year():
    sql = f"""
SELECT
    SUBSTRING(date_id::text, 1, 4) AS year,
    SUM(trip_counter) AS trip_counter
FROM bart.fact_ridership fr
GROUP BY 1;
    """
    return sql_to_df(sql)
