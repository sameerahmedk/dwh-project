import dash
import pandas as pd
import plotly.express as px
from dash import dcc, html

# Load the fact table snapshot
df_snapshot = pd.read_csv("./fact_table_snapshot.csv")

# Get the top 10 industries by the number of job postings
top_industries = df_snapshot["industry"].value_counts().nlargest(10).index
df_top_industries = df_snapshot[df_snapshot["industry"].isin(top_industries)]

# Ensure the Date column is parsed correctly
df_top_industries["Date"] = pd.to_datetime(df_top_industries["Date"], errors="coerce")
df_top_industries = df_top_industries.dropna(subset=["Date"])
df_top_industries["Month"] = df_top_industries["Date"].dt.to_period("M").astype(str)

# Normalize the apply_rate values
df_top_industries["normalized_apply_rate"] = (
    df_top_industries["apply_rate"] / df_top_industries["apply_rate"].max()
)

# Create the figures for the charts
fig_views_vs_applies = px.scatter(
    df_top_industries,
    x="views",
    y="applies",
    color="industry",
    title="Views vs Applies (Top 10 Industries)",
)
fig_salary_distribution = px.box(
    df_top_industries,
    x="industry",
    y="med_salary",
    title="Salary Distribution by Industry (Top 10 Industries)",
)
fig_company_size_distribution = px.pie(
    df_snapshot, names="company_size", title="Company Size Distribution"
)
fig_application_type = px.pie(
    df_top_industries, names="application_type", title="Application Type Distribution"
)

# Create a Dash app
app = dash.Dash(__name__)

# Define the layout of the app
app.layout = html.Div(
    [
        html.H1("Job Postings Dashboard"),
        dcc.Graph(id="views-vs-applies", figure=fig_views_vs_applies),
        dcc.Graph(id="salary-distribution", figure=fig_salary_distribution),
        dcc.Graph(id="application-type", figure=fig_application_type),
        dcc.Interval(
            id="interval-component",
            interval=1 * 60000,  # in milliseconds (update every minute)
            n_intervals=0,
        ),
    ]
)

# Run the app
if __name__ == "__main__":
    app.run_server(debug=True)
