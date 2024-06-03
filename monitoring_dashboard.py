import dash
from dash import dcc, html
import sqlite3
import pandas as pd
import time

app = dash.Dash(__name__)

app.layout = html.Div(
    [
        html.H1("Live Monitoring Dashboard"),
        html.Div(id="live-update-text"),
        dcc.Interval(
            id="interval-component",
            interval=10 * 1000,  # in milliseconds (updates every 10 seconds)
            n_intervals=0,
        ),
    ]
)


@app.callback(
    dash.dependencies.Output("live-update-text", "children"),
    [dash.dependencies.Input("interval-component", "n_intervals")],
)
def update_metrics(n):
    conn = sqlite3.connect("star_schema.db")
    new_rows_count = pd.read_sql("SELECT COUNT(*) FROM ETLLog", conn).iloc[0, 0]
    conn.close()

    return html.Div([html.H3(f"New Rows Inserted: {new_rows_count}")])


if __name__ == "__main__":
    app.run_server(debug=True)
