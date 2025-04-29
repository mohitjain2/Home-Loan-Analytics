# 📁 File: pages/page1_overview.py
import sqlite3
import pandas as pd
import plotly.express as px
from dash import html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc

def load_chunked_data_from_db(db_path,
                             table_name,
                             columns,
                             chunksize=100_000,
                             max_rows=2_350_000):
    """
    Streams specified columns from a SQLite table in chunks,
    stopping once max_rows have been read.
    """
    # Quote each column for SQLite (handles hyphens, etc.)
    quoted_cols = ', '.join(f'"{col}"' for col in columns)

    conn = sqlite3.connect(db_path)
    chunks = []
    total_rows = 0

    # Stream in chunks of only the selected columns
    sql = f'SELECT {quoted_cols} FROM "{table_name}"'
    for chunk in pd.read_sql_query(sql, conn, chunksize=chunksize):
        chunks.append(chunk)
        total_rows += len(chunk)
        if max_rows is not None and total_rows >= max_rows:
            break

    conn.close()
    return pd.concat(chunks, ignore_index=True)

# Load Data from SQLite instead of CSV
columns = [
    'activity_year',
    'derived_msa-md',
    'state_code',
    'conforming_loan_limit',
    'derived_loan_product_type',
    'derived_dwelling_category',
    'derived_ethnicity',
    'derived_race',
    'derived_sex',
    'action_taken',
    'loan_type',
    'loan_purpose',
    'lien_status',
    'loan_amount',
    'loan_to_value_ratio',
    'interest_rate',
    'rate_spread',
    'hoepa_status',
    'total_loan_costs',
    'origination_charges',
    'loan_term',
    'property_value',
    'construction_method',
    'occupancy_type',
    'manufactured_home_secured_property_type',
    'manufactured_home_land_property_interest',
    'total_units',
    'income',
    'debt_to_income_ratio',
    'applicant_credit_score_type',
    'applicant_sex',
    'applicant_age',
    'applicant_age_above_62',
    'denial_reason-1',
    'source_year'
]

# you can tweak max_rows (e.g. to None) once you’re ready to load full table
df = load_chunked_data_from_db(
    db_path='my_database.db',
    table_name='downsample_random',
    columns=columns,
    chunksize=100000,
    max_rows=2500000
)

# Map action codes to labels
action_labels = {
    1: 'Loan Approved',
    2: 'Approved but Not Accepted',
    3: 'Denied',
    8: 'Preapproval Approved but Not Accepted'
}
df['action_taken'] = pd.to_numeric(df['action_taken'], errors='coerce')
df['action_label'] = df['action_taken'].map(action_labels)
df['activity_year'] = pd.to_numeric(df['activity_year'], errors='coerce')
df['state_code'] = df['state_code'].astype(str).str.strip()

# --- Dropdown Filters ---
years = sorted(df['activity_year'].dropna().unique())
states = sorted(df['state_code'].dropna().unique())

# --- Initial KPI Values ---
total_apps = len(df)
approved = len(df[pd.to_numeric(df['action_taken'], errors='coerce') == 1])
denied = len(df[df['action_taken'] == 3])
avg_loan_amount = df['loan_amount'].mean()
avg_interest_rate = (df.loc[df['action_taken'].isin([1, 2, 8]), 'interest_rate'].mean())
approval_rate = round((approved / total_apps) * 100, 2)
# denial_rate = round((denied / total_apps) * 100, 2)

# --- Layout ---
layout = html.Div([
    html.Div("📊 Loan Outcome Overview", className="page-title"),

    # Filters
    dbc.Row([
        dbc.Col(dcc.RangeSlider(
            id='year-range-slider',
            min=min(years), max=max(years), step=1,
            marks={int(year): str(year) for year in years},
            value=[min(years), max(years)]
        ), width=6),

        dbc.Col(dcc.Dropdown(id='state-filter', options=[{"label": s, "value": s} for s in states],
                              placeholder="Select State", clearable=True), width=3),

        dbc.Col(dcc.Dropdown(
            id='map-metric',
            options=[
                {"label": "Loan Count", "value": "count"},
                {"label": "Approval Rate", "value": "approval_rate"},
                {"label": "Total Loan Amount", "value": "loan_amount"}
            ],
            value="count",
            clearable=False
        ), width=3)
    ], className="mb-4"),

    # KPI Cards
    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H5("📦 Total Applications", className="kpi-title"),
                html.H2(f"{total_apps:,}", id="kpi-total", className="kpi-value")
            ])
        ], className="kpi-card bg-secondary text-white shadow")),

        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H5("✅ Approval Rate", className="kpi-title"),
                html.H2(f"{approval_rate}%", id="kpi-approval", className="kpi-value")
            ])
        ], className="kpi-card bg-success text-white shadow")),

        # dbc.Col(dbc.Card([
        #     dbc.CardBody([
        #         html.H5("❌ Denial Rate", className="kpi-title"),
        #         html.H2(f"{denial_rate}%", id="kpi-denial", className="kpi-value")
        #     ])
        # ], className="kpi-card bg-danger text-white shadow")),

        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H5("💰 Avg. Loan Amount", className="kpi-title"),
                html.H2(f"${avg_loan_amount:,.0f}", id="kpi-loan", className="kpi-value")
            ])
        ], className="kpi-card bg-info text-white shadow")),

        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H5("📈 Avg. Interest Rate", className="kpi-title"),
                html.H2(f"{avg_interest_rate:.2f}%", id="kpi-interest", className="kpi-value")
            ])
        ], className="kpi-card bg-primary text-white shadow"))
    ], className="mb-5"),

    # Graphs
    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardBody([
                dcc.Graph(id="outcome-pie", style={"height": "400px", "margin-bottom": "0"})
            ])
        ], className="graph-card"), width=6),

        dbc.Col(dbc.Card([
            dbc.CardBody([
                dcc.Graph(id="bar-outcome-year", style={"height": "400px", "margin-bottom": "0"})
            ])
        ], className="graph-card"), width=6)
    ], className="mb-4"),

    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardBody([
            dcc.Graph(id="line-total-apps", style={"height": "400px"})
            ])
        ], className="graph-card")),

        dbc.Col(dbc.Card([
            dbc.CardBody([
                dcc.Graph(id="state-map", style={"height": "450px", "margin-bottom": "0"})
            ])
        ], className="graph-card"))
    ])
])

@callback(
    Output("outcome-pie", "figure"),
    Output("bar-outcome-year", "figure"),
    Output("state-map", "figure"),
    Output("line-total-apps", "figure"),   # NEW
    Output("kpi-total", "children"),       # NEW
    Output("kpi-approval", "children"),
    # Output("kpi-denial", "children"),
    Output("kpi-loan", "children"),
    Output("kpi-interest", "children"),
    Input("year-range-slider", "value"),
    Input("state-filter", "value"),
    Input("map-metric", "value")
)
def update_page(year_range, selected_state, map_metric):
    filtered_df = df[(df['activity_year'] >= year_range[0]) & (df['activity_year'] <= year_range[1])].copy()

    if selected_state:
        filtered_df = filtered_df[filtered_df['state_code'] == selected_state]

    if filtered_df.empty:
        return {}, {}, {}, "0%", "0%", "$0", "0%"

    # KPIs
    total_apps = len(filtered_df)
    approved = len(filtered_df[filtered_df['action_taken'] == 1])
    # denied = len(filtered_df[filtered_df['action_taken'] == 3])
    avg_loan = filtered_df['loan_amount'].mean()
    avg_rate = filtered_df['interest_rate'].mean()

    approval_rate = f"{(approved / total_apps * 100):.2f}%" if total_apps else "0%"
    # denial_rate = f"{(denied / total_apps * 100):.2f}%" if total_apps else "0%"
    avg_loan_fmt = f"${avg_loan:,.0f}" if avg_loan else "$0"
    avg_rate_fmt = f"{avg_rate:.2f}%" if avg_rate else "0%"

    # Pie chart
    pie_fig = px.pie(
        filtered_df, names='action_label', hole=0.5,
        title='Loan Outcome Distribution'
    )
    pie_fig.update_layout(height=400, width=550)

    # Bar by year
    yearly = filtered_df.groupby(['activity_year', 'action_label']).size().reset_index(name='count')
    bar_fig = px.bar(yearly, x='activity_year', y='count', color='action_label', barmode='stack')
    bar_fig.update_layout(title='Loan Outcomes by Year', height=400, width=650)

    #Line Chart
    line_data = filtered_df.groupby('activity_year').size().reset_index(name='count')
    line_fig = px.line(line_data, x='activity_year', y='count', title="Total Applications Over Time")
    line_fig.update_layout(height=400)


    # Choropleth Map by state
    if map_metric == "count":
        state_group = filtered_df.groupby('state_code').size().reset_index(name='value')
    elif map_metric == "approval_rate":
        total_by_state = filtered_df.groupby('state_code').size()
        approved_by_state = filtered_df[filtered_df['action_taken'] == 1].groupby('state_code').size()
        approval_rate_by_state = (approved_by_state / total_by_state * 100).reset_index(name='value')
        state_group = approval_rate_by_state
    elif map_metric == "loan_amount":
        state_group = filtered_df.groupby('state_code')['loan_amount'].sum().reset_index(name='value')


    map_fig = px.choropleth(
        state_group,
        locations='state_code',
        locationmode="USA-states",
        color='value',
        scope="usa",
        color_continuous_scale="Viridis_r",
        title="State-wise {}".format(
            "Loan Count" if map_metric == "count"
            else "Approval Rate (%)" if map_metric == "approval_rate"
            else "Total Loan Amount ($)")
    )
    map_fig.update_layout(height=450)

    return pie_fig, bar_fig, map_fig, line_fig, f"{total_apps:,}", approval_rate, avg_loan_fmt, avg_rate_fmt

