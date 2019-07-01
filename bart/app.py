import csv
import os
import tempfile
from ast import literal_eval

import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
import dash_table
import pandas as pd
from dash.dependencies import Input, Output, State
from flask import after_this_request, request, send_file


from data import get_ridership_data, get_date_count_data_by_year, get_ridership_by_year
from settings import log
from datetime import datetime
from plotly import graph_objs as go


default_columns = ["date", "hour", "destination_station", "origin_station", "trip_counter"]
df = get_ridership_data(default_columns)
ridership_by_year_df = get_ridership_by_year()

font_awesome_cdn = "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css"
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, font_awesome_cdn])

mapbox_access_token = 'pk.eyJ1IjoiYW5keWh1eW5oIiwiYSI6ImNqeGZoOGw4MjAwZDgzb3A4NmptYml6YXMifQ.EpFfyMkhfaRcnA7okCjiVw'

navbar = dbc.NavbarSimple(
    brand="Bart Ridership Dashboard",
    brand_href="#",
    sticky="top",
)

body = dbc.Container(
    [
        dbc.Row(
            [
                dbc.Col(html.H3(id='field-selection-header', children='Field selection'), width=9),
                dbc.Col(html.Button('Run', id='run-button', n_clicks=0), width=1),
                dbc.Col(html.A(html.Button('Download CSV'), id='download-csv'), width=2),
            ]
        ),
        dbc.Row(
            dbc.Col(
                dcc.Dropdown(
                    id='field-selection',
                    options=[
                        {'label': 'date', 'value': 'date'},
                        {'label': 'epoch', 'value': 'epoch'},
                        {'label': 'day_suffix', 'value': 'day_suffix'},
                        {'label': 'day_name', 'value': 'day_name'},
                        {'label': 'day_of_week', 'value': 'day_of_week'},
                        {'label': 'day_of_month', 'value': 'day_of_month'},
                        {'label': 'day_of_quarter', 'value': 'day_of_quarter'},
                        {'label': 'day_of_year', 'value': 'day_of_year'},
                        {'label': 'week_of_month', 'value': 'week_of_month'},
                        {'label': 'week_of_year', 'value': 'week_of_year'},
                        {'label': 'week_of_year_iso', 'value': 'week_of_year_iso'},
                        {'label': 'month_actual', 'value': 'month_actual'},
                        {'label': 'month_name', 'value': 'month_name'},
                        {'label': 'month_name_abbreviated', 'value': 'month_name_abbreviated'},
                        {'label': 'hour', 'value': 'hour'},
                        {'label': 'destination_station', 'value': 'destination_station'},
                        {'label': 'origin_station', 'value': 'origin_station'},
                        {'label': 'trip_counter', 'value': 'trip_counter'},
                        {'label': 'origin_station_full_address', 'value': 'origin_station_full_address'},
                        {'label': 'destination_station_full_address', 'value': 'destination_station_full_address'},
                    ],
                    multi=True,
                    value=default_columns
                ),
                width="12"
            )
        ),
        dash_table.DataTable(
            id='data-table',
            columns=[
                {'name': i, 'id': i, 'deletable': True} for i in df.columns
            ],
            page_action='native',
            filter_action='native',
            style_table={'overflowX': 'auto', 'overflowY': 'auto', 'height': '300px'},
            sort_action='native',
            sort_mode='multi',
            sort_by=[]
        ),
        # dbc.Row(
        #     [
        #         dbc.Col(
        #             dcc.Dropdown(
        #                 options=[
        #                     {'label': '2011', 'value': '2011'},
        #                     {'label': '2012', 'value': '2012'},
        #                     {'label': '2013', 'value': '2013'},
        #                     {'label': '2014', 'value': '2014'},
        #                     {'label': '2015', 'value': '2015'},
        #                     {'label': '2016', 'value': '2016'},
        #                     {'label': '2017', 'value': '2017'},
        #                     {'label': '2018', 'value': '2018'},
        #                 ],
        #                 value='2011',
        #                 id='histogram-date-count-year-selection'
        #             ),
        #             width="2"
        #         ),
        #         dbc.Col(
        #             dcc.Graph(id='histogram-date-count-by-year'),
        #             width="10"
        #         )
        #     ]
        # ),
        # dbc.Row(
        #     dbc.Col(
        #         dcc.Graph(
        #             figure=go.Figure(
        #                 data=[
        #                     go.Bar(
        #                         x=ridership_by_year_df['year'],
        #                         y=ridership_by_year_df['trip_counter'],
        #                         hoverinfo="y"
        #                     )
        #                 ]
        #             )
        #         )
        #     )
        # ),
        dbc.Row(
            dbc.Col(
                dcc.Graph(
                    figure=go.Figure(
                        data=[
                            go.Scattermapbox(
                                lat=['45.5017'],
                                lon=['-73.5673'],
                                mode='markers',
                                marker=go.scattermapbox.Marker(
                                    size=14
                                ),
                                text=['Montreal'],
                            )
                        ],
                        layout=go.Layout(
                            autosize=True,
                            hovermode='closest',
                            margin=go.layout.Margin(l=0, r=0, t=0, b=0),
                            mapbox=go.layout.Mapbox(
                                accesstoken=mapbox_access_token,
                                bearing=0,
                                style="dark",
                                center=dict(
                                    lat=37.806022,
                                    lon=-122.3118
                                ),
                                pitch=0,
                                zoom=9
                            ),
                        )
                    )
                )
            )
        )
    ]
)


app.layout = html.Div([navbar, body])


# @app.callback(
#     output=[
#         Output('histogram-date-count-by-year', 'figure')
#     ],
#     inputs=[
#         Input('histogram-date-count-year-selection', 'value')
#     ]
# )
# def update_yearly_histogram(year):
#     df = get_date_count_data_by_year(year)
#     return [
#             go.Figure(
#                 data=[
#                     go.Bar(x=df['date'], y=df['trip_counter'],  hoverinfo="x"),
#                 ],
#             )
#     ]


# @app.callback(
#     output=[
#         Output('data-table', 'data'),
#         Output('data-table', 'columns')
#     ],
#     inputs=[Input('run-button', 'n_clicks')],
#     state=[State('field-selection', 'value')]
# )
# def update_data_table(click, state):
#     df = get_ridership_data(state)
#     new_cols = [
#         {'name': i, 'id': i} for i in df.columns
#     ]
#     return df.to_dict('records'), new_cols


@app.callback(
    output=Output('download-csv', 'href'),
    inputs=[Input('data-table', 'data')]
)
def update_link(value):
    return '/dash/downloadcsv?value={}'.format(value)


@app.server.route('/dash/downloadcsv')
def download_csv():

    value = literal_eval(request.args.get('value'))
    df = pd.DataFrame(value)
    with tempfile.NamedTemporaryFile(delete=False) as f:
        tmp_fn = f.name
        df.to_csv(tmp_fn, quoting=csv.QUOTE_ALL, encoding='utf-8')

    @after_this_request
    def remove_file(response):
        try:
            os.remove(tmp_fn)
        except Exception as error:
            log.warn(f'Ran into an issue removing the tmp file: {error}')
        return response

    current_time = datetime.now().strftime('%Y-%m-%d-%H:%m:%S')

    return send_file(tmp_fn, mimetype='text/csv',
                     attachment_filename=f'ridership_{current_time}.csv',
                     as_attachment=True)


if __name__ == '__main__':
    app.run_server(debug=True)
