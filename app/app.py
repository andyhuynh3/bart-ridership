import csv
import os
import tempfile
import time
from datetime import datetime as dt

import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import dash_table
import pandas as pd
from dash.dependencies import Input, Output
from flask import after_this_request, request, send_file
from plotly import graph_objs as go

from data import BartRidershipData
from settings import MAPBOX_ACCESS_TOKEN, engine, log

bart_ridership_data = BartRidershipData(connection=engine)
default_columns = [
    "date",
    "hour",
    "trip_counter",
    "day_name",
    "day_of_week",
    "destination_station_abbreviation",
    "origin_station_abbreviation",
    "destination_station",
    "origin_station",
    "destination_station_full_address",
    "origin_station_full_address",
]

font_awesome_cdn = (
    "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css"
)
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP, font_awesome_cdn],
    meta_tags=[{"name": "viewport", "content": "width=device-width"}],
)
ridership_by_hour_by_date = bart_ridership_data.get_ridership_by_hour_by_date(
    "2015-06-02"
)
ridership_by_date_df = bart_ridership_data.get_ridership_data_by_date()

mapbox_data = bart_ridership_data.get_station_lat_lon()

navbar = dbc.NavbarSimple(
    brand="Bart Ridership Dashboard", brand_href="#", sticky="top"
)

body = dbc.Row(
    [
        dbc.Col(
            [
                html.Div(
                    className="div-user-controls",
                    children=[
                        html.H2("Bart Ridership App"),
                        html.P(
                            "Select different days using the date picker. "
                            "Click on points on the map to update the histogram. "
                            "Explore data with the data table."
                        ),
                        html.Div(
                            className="div-for-dropdown",
                            children=[
                                dcc.DatePickerSingle(
                                    id="date-picker",
                                    min_date_allowed=dt(2011, 1, 1),
                                    max_date_allowed=dt(2018, 12, 31),
                                    initial_visible_month=dt(2011, 1, 1),
                                    date=dt(2011, 1, 1).date(),
                                    display_format="MMMM D, YYYY",
                                    style={"border": "0px solid black"},
                                )
                            ],
                        ),
                        html.P(id="total-rides"),
                        html.P(id="selected-station"),
                        html.P(id="station-address"),
                        html.P(id="station-intro"),
                        dcc.Markdown(id="station-links"),
                        dcc.Markdown(
                            children=[
                                "Data Source: "
                                "[Ridership Reports](https://www.bart.gov/about/reports/ridership) "
                                "and [BART API](http://api.bart.gov/docs/overview/index.aspx)"
                            ]
                        ),
                        dcc.Markdown(
                            children=[
                                "Source Code: [GitHub](https://github.com/andyh1203/bart-ridership)"
                            ]
                        ),
                    ],
                ),
                html.Div(
                    className="div-map",
                    children=[
                        html.Button(
                            id="reset-histogram",
                            type="submit",
                            value="reset",
                            children="Reset Histogram",
                        ),
                        html.Div(id="hover-ts", style={"display": "none"}),
                        dcc.Graph(id="mapbox", animate=True),
                    ],
                ),
            ],
            width="4",
        ),
        dbc.Col(
            [
                dbc.Row(
                    [
                        dbc.Col(
                            html.Div(
                                className="div-field-selection",
                                children=[
                                    dcc.Dropdown(
                                        id="field-selection",
                                        options=[
                                            {"label": "date", "value": "date"},
                                            {"label": "epoch", "value": "epoch"},
                                            {
                                                "label": "day_suffix",
                                                "value": "day_suffix",
                                            },
                                            {"label": "day_name", "value": "day_name"},
                                            {
                                                "label": "day_of_week",
                                                "value": "day_of_week",
                                            },
                                            {
                                                "label": "day_of_month",
                                                "value": "day_of_month",
                                            },
                                            {
                                                "label": "day_of_quarter",
                                                "value": "day_of_quarter",
                                            },
                                            {
                                                "label": "week_of_month",
                                                "value": "week_of_month",
                                            },
                                            {
                                                "label": "week_of_year",
                                                "value": "week_of_year",
                                            },
                                            {
                                                "label": "week_of_year_iso",
                                                "value": "week_of_year_iso",
                                            },
                                            {
                                                "label": "month_name",
                                                "value": "month_name",
                                            },
                                            {
                                                "label": "month_name_abbreviated",
                                                "value": "month_name_abbreviated",
                                            },
                                            {
                                                "label": "quarter_name",
                                                "value": "quarter_name",
                                            },
                                            {"label": "mmyyyy", "value": "mmyyyy"},
                                            {"label": "mmddyyyy", "value": "mmddyyyy"},
                                            {
                                                "label": "weekend_indr",
                                                "value": "weekend_indr",
                                            },
                                            {"label": "hour", "value": "hour"},
                                            {
                                                "label": "trip_counter",
                                                "value": "trip_counter",
                                            },
                                            {
                                                "label": "origin_station",
                                                "value": "origin_station",
                                            },
                                            {
                                                "label": "destination_station",
                                                "value": "destination_station",
                                            },
                                            {
                                                "label": "origin_station_full_address",
                                                "value": "origin_station_full_address",
                                            },
                                            {
                                                "label": "destination_station_full_address",
                                                "value": "destination_station_full_address",
                                            },
                                            {
                                                "label": "destination_station_abbreviation",
                                                "value": "destination_station_abbreviation",
                                            },
                                            {
                                                "label": "origin_station_abbreviation",
                                                "value": "origin_station_abbreviation",
                                            },
                                            {
                                                "label": "origin_station_abbreviation",
                                                "value": "origin_station_abbreviation_lower",
                                            },
                                            {
                                                "label": "destination_station_abbreviation_lower",
                                                "value": "destination_station_abbreviation_lower",
                                            },
                                            {
                                                "label": "origin_station_city",
                                                "value": "origin_station_city",
                                            },
                                            {
                                                "label": "destination_station_city",
                                                "value": "destination_station_city",
                                            },
                                            {
                                                "label": "origin_station_cross_street",
                                                "value": "origin_station_cross_street",
                                            },
                                            {
                                                "label": "destination_station_cross_street",
                                                "value": "destination_station_cross_street",
                                            },
                                            {
                                                "label": "origin_station_latitude",
                                                "value": "origin_station_latitude",
                                            },
                                            {
                                                "label": "destination_station_latitude",
                                                "value": "destination_station_latitude",
                                            },
                                            {
                                                "label": "origin_station_link",
                                                "value": "origin_station_link",
                                            },
                                            {
                                                "label": "destination_station_link",
                                                "value": "destination_station_link",
                                            },
                                            {
                                                "label": "origin_station_longitude",
                                                "value": "origin_station_longitude",
                                            },
                                            {
                                                "label": "destination_station_longitude",
                                                "value": "destination_station_longitude",
                                            },
                                            {
                                                "label": "origin_station_north_routes",
                                                "value": "origin_station_north_routes",
                                            },
                                            {
                                                "label": "destination_station_north_routes",
                                                "value": "destination_station_north_routes",
                                            },
                                            {
                                                "label": "origin_station_south_routes",
                                                "value": "origin_station_south_routes",
                                            },
                                            {
                                                "label": "destination_station_south_routes",
                                                "value": "destination_station_south_routes",
                                            },
                                            {
                                                "label": "origin_station_south_platforms",
                                                "value": "origin_station_south_platforms",
                                            },
                                            {
                                                "label": "destination_station_south_platforms",
                                                "value": "destination_station_south_platforms",
                                            },
                                            {
                                                "label": "origin_station_north_platforms",
                                                "value": "origin_station_north_platforms",
                                            },
                                            {
                                                "label": "destination_station_north_platforms",
                                                "value": "destination_station_north_platforms",
                                            },
                                            {
                                                "label": "origin_station_state",
                                                "value": "origin_station_state",
                                            },
                                            {
                                                "label": "destination_station_state",
                                                "value": "destination_station_state",
                                            },
                                            {
                                                "label": "origin_station_zipcode",
                                                "value": "origin_station_zipcode",
                                            },
                                            {
                                                "label": "destination_station_zipcode",
                                                "value": "destination_station_zipcode",
                                            },
                                            {
                                                "label": "origin_link",
                                                "value": "origin_link",
                                            },
                                            {
                                                "label": "destination_link",
                                                "value": "destination_link",
                                            },
                                            {
                                                "label": "origin_station_map_url",
                                                "value": "origin_station_map_url",
                                            },
                                            {
                                                "label": "destination_map_url",
                                                "value": "destination_map_url",
                                            },
                                        ],
                                        multi=True,
                                        value=default_columns,
                                    )
                                ],
                            ),
                            width=11,
                        ),
                        dbc.Col(
                            html.Div(
                                html.A(
                                    dbc.Button(
                                        "Download",
                                        outline=True,
                                        color="primary",
                                        className="mr-1",
                                    ),
                                    id="download-csv",
                                )
                            ),
                            width=1,
                        ),
                    ]
                ),
                html.Div(
                    className="div-data-table",
                    children=[
                        dcc.Loading(
                            id="loading",
                            children=[
                                dash_table.DataTable(
                                    id="data-table",
                                    page_action="native",
                                    filter_action="native",
                                    style_table={
                                        "overflowX": "auto",
                                        "overflowY": "auto",
                                        "height": "420px",
                                    },
                                    sort_action="native",
                                    sort_mode="multi",
                                    style_filter={"background-color": "#ffffff"},
                                    style_header={
                                        "backgroundColor": "rgb(30, 30, 30)",
                                        "color": "white",
                                    },
                                    style_cell={
                                        "backgroundColor": "rgb(50, 50, 50)",
                                        "color": "white",
                                    },
                                    css=[
                                        {
                                            "selector": ".previous-page, .next-page",
                                            "rule": "background-color: transparent; "
                                            "width: 50%; border-color: #007bff; color: #007bff;",
                                        }
                                    ],
                                    sort_by=[],
                                )
                            ],
                            fullscreen=True,
                            style={"background-color": "#1e1e1e"},
                        )
                    ],
                ),
                dcc.Graph(id="histogram-ridership-by-hour-by-date"),
            ],
            width=8,
        ),
    ]
)

app.layout = html.Div([dcc.Store(id="memory"), body])

color_val = [
    "#F4EC15",
    "#DAF017",
    "#BBEC19",
    "#9DE81B",
    "#80E41D",
    "#66E01F",
    "#4CDC20",
    "#34D822",
    "#24D249",
    "#25D042",
    "#26CC58",
    "#28C86D",
    "#29C481",
    "#2AC093",
    "#2BBCA4",
    "#2BB5B8",
    "#2C99B4",
    "#2D7EB0",
    "#2D65AC",
    "#2E4EA4",
    "#2E38A4",
    "#3B2FA0",
    "#4E2F9C",
    "#603099",
]


def get_histogram_figure(date, click_data=None):

    layout = go.Layout(
        bargap=0.01,
        bargroupgap=0,
        barmode="group",
        margin=go.layout.Margin(l=10, r=10, t=0, b=50),  # noqa: E741
        showlegend=False,
        plot_bgcolor="#323130",
        paper_bgcolor="#323130",
        dragmode="select",
        font=dict(color="white"),
        xaxis=dict(
            range=[-0.5, 23.5],
            showgrid=False,
            nticks=25,
            fixedrange=True,
            ticksuffix=":00",
        ),
    )

    if click_data is not None:
        station_abb = click_data["points"][0]["text"].split(" - ")[0]
        df = bart_ridership_data.get_ridership_by_hour_by_station_and_date(
            date, station_abb
        )
        max_y_origin = df["origin_ridership_total"].max()
        max_y_destination = df["destination_ridership_total"].max()
        max_y = max_y_origin if max_y_origin > max_y_destination else max_y_destination
        data = [
            go.Bar(
                x=df["hour"],
                y=df["origin_ridership_total"],
                hoverinfo="y",
                name="Origin",
            ),
            go.Bar(
                x=df["hour"],
                y=df["destination_ridership_total"],
                hoverinfo="y",
                opacity=0.5,
                name="Destination",
            ),
        ]
        # layout["annotations"] = [
        #     dict(
        #         x=xi + 0.25 if i else xi - 0.25,
        #         y=yi,
        #         text=str(yi),
        #         xanchor="center",
        #         yanchor="bottom",
        #         showarrow=False,
        #         font=dict(color="white"),
        #     )
        #     for i, df in enumerate([])
        #     for xi, yi in zip(df["hour"], df["ridership_total"])
        # ]
        layout["showlegend"] = True
        layout["legend"] = go.layout.Legend(x=0, y=1)
        layout["title"] = go.layout.Title(text=f"Station: {station_abb}", x=0.5, y=0.95)
    else:
        df = bart_ridership_data.get_ridership_by_hour_by_date(date)
        max_y = df["ridership_total"].max()
        data = [
            go.Bar(
                x=df["hour"],
                y=df["ridership_total"],
                marker={"color": color_val},
                hoverinfo="y",
            )
        ]
        layout["annotations"] = [
            dict(
                x=xi,
                y=yi,
                text=str(yi),
                xanchor="center",
                yanchor="bottom",
                showarrow=False,
                font=dict(color="white"),
            )
            for xi, yi in zip(df["hour"], df["ridership_total"])
        ]
        layout["title"] = go.layout.Title(text="Station: All", x=0.5, y=0.95)
    layout["yaxis"] = dict(
        range=[0, 1.25 * max_y],
        showticklabels=False,
        showgrid=False,
        fixedrange=True,
        rangemode="nonnegative",
        zeroline=False,
    )

    figure = go.Figure(data=data, layout=layout)
    return figure


@app.callback(Output("hover-ts", "children"), [Input("mapbox", "hoverData")])
def store_hover_ts(click_data):
    return round(1000 * time.time())


@app.callback(
    output=[
        Output("histogram-ridership-by-hour-by-date", "figure"),
        Output("selected-station", "children"),
        Output("station-address", "children"),
        Output("station-links", "children"),
    ],
    inputs=[
        Input("date-picker", "date"),
        Input("mapbox", "clickData"),
        Input("reset-histogram", "n_clicks_timestamp"),
        Input("hover-ts", "children"),
    ],
)
def update_hourly_histogram_by_date(date, click_data, n_clicks_timestamp, hover_ts):
    if n_clicks_timestamp and n_clicks_timestamp > hover_ts:
        clicked_station_str = f"Currently clicked station: none"
        station_full_address_str = None
        station_links_str = None
        figure = get_histogram_figure(date, None)
    else:
        if click_data is not None:
            station_abb = click_data["points"][0]["text"].split(" - ")[0]
            station_info = bart_ridership_data.get_station_info(station_abb)
            station_map_url = station_info["station_map_url"][0]
            station_link = station_info["link"][0]
            station_full_address = station_info["full_address"][0]
            station_name = station_info["name"][0]
            station_display = f"{station_abb} ({station_name})"
            station_full_address_str = f"Station address: {station_full_address}"
            station_links_str = (
                f"More information: [Station link]({station_link}) and [Map]({station_map_url})"
            )
        else:
            station_display = "none"
            station_full_address_str = None
            station_links_str = None
        clicked_station_str = f"Currently clicked station: {station_display}"
        figure = get_histogram_figure(date, click_data)
    return figure, clicked_station_str, station_full_address_str, station_links_str


@app.callback(
    output=[Output("mapbox", "figure")], inputs=[Input("date-picker", "date")]
)
def update_map(date):
    df = bart_ridership_data.get_ridership_by_station_by_date(date)
    return [
        go.Figure(
            data=[
                go.Scattermapbox(
                    lat=df["latitude"],
                    lon=df["longitude"],
                    mode="markers",
                    marker=go.scattermapbox.Marker(size=6),
                    text=[
                        f"{abbreviation} - Origin: {origin_cnt}, Destination: {destination_cnt}"
                        for abbreviation, origin_cnt, destination_cnt in zip(
                            df["abbreviation"],
                            df["origin_count"],
                            df["destination_count"],
                        )
                    ],
                    hoverinfo="lat+lon+text",
                )
            ],
            layout=go.Layout(
                autosize=True,
                hovermode="closest",
                margin=go.layout.Margin(l=0, r=0, t=0, b=0),  # noqa: E741
                mapbox=go.layout.Mapbox(
                    accesstoken=MAPBOX_ACCESS_TOKEN,
                    bearing=0,
                    style="dark",
                    center=dict(lat=37.806022, lon=-122.1118),
                    pitch=0,
                    zoom=8.5,
                ),
            ),
        )
    ]


@app.callback(Output("total-rides", "children"), [Input("date-picker", "date")])
def update_total_rides_selection(date, click_data=None):
    total_rides = bart_ridership_data.get_total_ride_count_by_day(date)["cnt"][0]
    return f"Total rides on {date}: {total_rides}"


@app.callback(
    output=[Output("data-table", "data"), Output("data-table", "columns")],
    inputs=[Input("field-selection", "value"), Input("date-picker", "date")],
)
def update_data_table(columns, date):
    global ridership_by_date_df
    if str(ridership_by_date_df["date"][0]) != date:
        ridership_by_date_df = bart_ridership_data.get_ridership_data_by_date(date)
    selected_cols_df = ridership_by_date_df[columns]
    new_cols = [{"name": i, "id": i} for i in selected_cols_df.columns]
    return selected_cols_df.to_dict("records"), new_cols


@app.callback(
    output=Output("download-csv", "href"), inputs=[Input("data-table", "data")]
)
def update_link(value):
    df = pd.DataFrame(value)
    with tempfile.NamedTemporaryFile(delete=False) as f:
        tmp_fn = f.name
        df.to_csv(tmp_fn, quoting=csv.QUOTE_ALL, encoding="utf-8")
    return f"/dash/downloadcsv?tmp_fn={tmp_fn}"


@app.server.route("/dash/downloadcsv")
def download_csv():
    tmp_fn = request.args.get("tmp_fn")

    @after_this_request
    def remove_file(response):
        try:
            os.remove(tmp_fn)
        except Exception as error:
            log.warn(f"Ran into an issue removing the tmp file: {error}")
        return response

    current_time = dt.now().strftime("%Y-%m-%d-%H:%m:%S")

    return send_file(
        tmp_fn,
        mimetype="text/csv",
        attachment_filename=f"ridership_{current_time}.csv",
        as_attachment=True,
    )


if __name__ == "__main__":
    app.run_server(debug=True)
