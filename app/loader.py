import argparse
import gzip
from io import BytesIO

import boto3
import requests

from settings import engine, log


class BartRidershipLoader:

    BASE_URL = "http://64.111.127.166/origin-destination"

    def __init__(self, start_year, end_year):
        self.start_year = start_year
        self.end_year = end_year
        self.s3 = boto3.client("s3")

    def get_source_schema_setup_sql(self, year):
        # Extract source data
        # TODO: Get rid of partition and use index
        create_schema_sql = "CREATE SCHEMA IF NOT EXISTS source"
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS source.ridership (
            day DATE,
            hour INT,
            origin VARCHAR(16),
            destination VARCHAR(16),
            trip_counter INT
        ) PARTITION BY RANGE (day);
        """
        create_year_partition = f"""
        CREATE TABLE IF NOT EXISTS source.ridership_{year} PARTITION OF source.ridership
        FOR VALUES FROM ('{year}-01-01') TO ('{int(year) + 1}-01-01');
        """
        truncate_partition = f"""
        TRUNCATE source.ridership_{year}
        """
        return [
            create_schema_sql,
            create_table_sql,
            create_year_partition,
            truncate_partition,
        ]

    def get_bart_schema_setup_sql(self, year):
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS bart.fact_ridership (
            date_id INT,
            hour INT,
            origin_station_id INT,
            destination_station_id INT,
            trip_counter INT
        ) PARTITION BY RANGE (date_id);
        """
        create_year_partition = f"""
        CREATE TABLE IF NOT EXISTS bart.fact_ridership_{year} PARTITION OF bart.fact_ridership
        FOR VALUES FROM ('{year}0101') TO ('{int(year) + 1}0101');
        """
        create_index_sql = f"""
        CREATE INDEX IF NOT EXISTS
        idx_date_id_origin_station_id_destionation_station_id_fr_{year}
        ON bart.fact_ridership_{year} (date_id, origin_station_id, destination_station_id);
        """
        truncate_partition = f"""
        TRUNCATE bart.fact_ridership_{year}
        """
        return [
            create_table_sql,
            create_year_partition,
            create_index_sql,
            truncate_partition,
        ]

    def load_to_source_schema(self, year):
        file = f"date-hour-soo-dest-{year}.csv.gz"
        conn = engine.raw_connection()
        response = requests.get(f"{self.BASE_URL}/{file}")
        content = response.content
        with conn.cursor() as cur:
            for sql in self.get_source_schema_setup_sql(year):
                log.info(sql)
                cur.execute(sql)
            with gzip.GzipFile(fileobj=BytesIO(content), mode="rb") as f:
                copy_cmd = "COPY source.ridership FROM STDIN WITH CSV DELIMITER ','"
                log.info(copy_cmd)
                cur.copy_expert(copy_cmd, f)
            cur.execute("COMMIT")
        conn.close()

    def transform_to_bart_schema(self, year):
        for sql in self.get_bart_schema_setup_sql(year):
            log.info(sql)
            engine.execute(sql)
        transform_sql = f"""
        INSERT INTO bart.fact_ridership
        SELECT
            dim_date.date_id AS date_id,
            hour,
            origin.id AS origin_station_id,
            destination.id AS destination_station_id,
            trip_counter
        FROM source.ridership
        JOIN bart.dim_date ON 1=1
            AND ridership.day = dim_date.date
        JOIN bart.dim_station origin ON 1=1
            AND ridership.origin = origin.abbreviation
        JOIN bart.dim_station destination ON 1=1
            AND ridership.destination = destination.abbreviation
        WHERE day BETWEEN '{year}-01-01' AND '{year}-12-31'
        """
        log.info(transform_sql)
        engine.execute(transform_sql)

    def create_materialized_views(self):
        sql_stmts = [
            """
            CREATE MATERIALIZED VIEW IF NOT EXISTS bart.fact_ridership_by_station_by_date
            AS
            SELECT
                    date_id,
                    abbreviation,
                    latitude,
                    longitude,
                    origin_count,
                    destination_count
                FROM
                (
                    SELECT
                        fr.date_id,
                        ds.abbreviation,
                        ds.latitude,
                        ds.longitude,
                        COUNT(1) AS origin_count
                    FROM bart.fact_ridership fr
                    JOIN bart.dim_station ds ON 1=1
                        AND fr.origin_station_id = ds.id
                    GROUP BY date_id, abbreviation, latitude, longitude
                ) origin
                JOIN
                (
                    SELECT
                        fr.date_id,
                        ds.abbreviation,
                        ds.latitude,
                        ds.longitude,
                        COUNT(1) AS destination_count
                    FROM bart.fact_ridership fr
                    JOIN bart.dim_station ds ON 1=1
                        AND fr.destination_station_id = ds.id
                    GROUP BY date_id, abbreviation, latitude, longitude
                ) destination
            USING (date_id, abbreviation, latitude, longitude)
            WITH NO DATA;
            """,
            """
            CREATE MATERIALIZED VIEW IF NOT EXISTS bart.fact_ridership_count_by_date
            AS
            SELECT
                date_id,
                COUNT(1) AS cnt
            FROM bart.fact_ridership
            GROUP BY date_id
            WITH NO DATA;
            """,
            """
            CREATE MATERIALIZED VIEW IF NOT EXISTS bart.fact_ridership_count_by_hour_by_date
            AS
            SELECT
                date_id,
                hour,
                COUNT(1) AS ridership_total
            FROM bart.fact_ridership fr
            GROUP BY date_id, hour
            WITH NO DATA;
            """,
            """
            CREATE MATERIALIZED VIEW IF NOT EXISTS bart.fact_ridership_by_hour_by_station_by_date
            AS
            SELECT
                date_id,
            abbreviation,
            hour,
            origin_ridership_total,
            destination_ridership_total
            FROM
            (
                SELECT
                    date_id,
                    abbreviation,
                    hour,
                    COUNT(1) AS origin_ridership_total
                FROM bart.fact_ridership fr
                JOIN bart.dim_station ds ON 1=1
                    AND fr.origin_station_id = ds.id
                GROUP BY date_id, abbreviation, hour
            ) origin
            JOIN
            (
                SELECT
                date_id,
                abbreviation,
                hour,
                COUNT(1) AS destination_ridership_total
                FROM bart.fact_ridership fr
                        JOIN bart.dim_station ds ON 1 = 1
                    AND fr.destination_station_id = ds.id
                GROUP BY date_id, abbreviation, hour
            ) dest
            USING (date_id, abbreviation, hour)
            WITH NO DATA;
            """,
            """CREATE INDEX IF NOT EXISTS idx_date_id_abbreviation_frbhbsbd
            ON bart.fact_ridership_by_hour_by_station_by_date (date_id, abbreviation);""",
            """CREATE INDEX IF NOT EXISTS idx_date_id_frcbhbd
            ON bart.fact_ridership_count_by_hour_by_date (date_id);""",
            """CREATE INDEX IF NOT EXISTS idx_date_id_abbreviation_frbsbd
            ON bart.fact_ridership_by_station_by_date (date_id, abbreviation);""",
            "CREATE INDEX IF NOT EXISTS idx_date_frcbd ON bart.fact_ridership_count_by_date (date_id);",
            "REFRESH MATERIALIZED VIEW bart.fact_ridership_by_station_by_date",
            "REFRESH MATERIALIZED VIEW bart.fact_ridership_count_by_date",
            "REFRESH MATERIALIZED VIEW bart.fact_ridership_count_by_hour_by_date",
            "REFRESH MATERIALIZED VIEW bart.fact_ridership_by_hour_by_station_by_date",
        ]
        for sql_stmt in sql_stmts:
            log.info(sql_stmt)
            engine.execute(sql_stmt)

    def run(self):
        for year in range(self.start_year, self.end_year + 1):
            log.info(f"Loading {year} bart data into the data warehouse...")
            self.load_to_source_schema(year)
            self.transform_to_bart_schema(year)
        self.create_materialized_views()

    def drop_all():
        drop_sql_stmts = [
            "DROP TABLE source.ridership",
            "DROP TABLE bart.fact_ridership",
        ]
        for sql_stmt in drop_sql_stmts:
            log.info(sql_stmt)
            engine.execute()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--start_year",
        help="Year to start loading bart data into data warehouse, inclusive",
        required=True,
    )
    parser.add_argument(
        "-e",
        "--end_year",
        help="Year to stop loading bart data into data warehouse, inclusive",
        required=True,
    )
    args = parser.parse_args()
    start_year = int(args.start_year)
    end_year = int(args.end_year)

    loader = BartRidershipLoader(start_year, end_year)
    loader.run()


# def lambda_handler(event, context):

#     for record in event['Records']:
#         bucket_name = record['s3']['bucket']['name']
#         key = record['s3']['object']['key']
#         local_path = '/tmp/' + key.split('/')[-1]
#         # Download file from S3
#         client.download_file(bucket_name, key, local_path)
#         print("Downloaded s3 file, {}, to {}".format(key, local_path))
#         # Transform the file
#         output_path = '/tmp/output.csv'
#         transform_json(local_path, output_path)
#         # Load csv to Postgres
#         pg_load(connection_string, schema+'.'+table, output_path)

#     return {
#         'statusCode': 200,
#         'body': json.dumps('Hello from Lambda!')
#     }

# import csv
# with open('station.csv', 'r') as csvfile:
#     csv_reader = csv.reader(csvfile, delimiter=',')
#     for row in csv_reader:
#         print(f'({row[0].upper()!r}, {row[0]!r}, {row[1]!r}),')
