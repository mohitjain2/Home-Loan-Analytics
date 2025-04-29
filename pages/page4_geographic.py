# 📁 pages/page4_geographic.py
import pandas as pd
import plotly.express as px
from dash import html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc
import sqlite3

# 🔄 Load chunked data
def load_chunked_data_from_db(db_path,table_name,columns,chunksize=100_000,max_rows=2_350_000):
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
for col in ['activity_year','loan_amount','property_value',
            'loan_to_value_ratio','interest_rate','action_taken']:
    df[col] = pd.to_numeric(df[col], errors='coerce')
df['state_code'] = df['state_code'].astype(str).str.strip()
df['loan_purpose'] = df['loan_purpose'].astype(str).str.strip()
df['action_label'] = df['action_taken'].map(action_labels)

# Dropdown filter options
years        = sorted(df['activity_year'].dropna().unique())
states       = sorted(df['state_code'].dropna().unique())
loan_purposes = sorted(df['loan_purpose'].dropna().unique())
map_metrics  = [
    {"label": "Average Loan Amount",           "value": "loan_amount"},
    {"label": "Average Property Value",        "value": "property_value"},
    {"label": "Average Interest Rate",         "value": "interest_rate"},
    {"label": "Loan Volume (Application Count)","value": "application_count"}
]

# ─── Layout ─────────────────────────────────────────────────────────────────
layout = html.Div([
    html.H3("🗺️ Geographic & Regional Trends", className="page-title"),

    # Filters
    dbc.Row([
        dbc.Col(dcc.Dropdown(
            id='geo-year',
            options=[{"label": str(y), "value": y} for y in years],
            placeholder="Select Year",
            clearable=True
        ), width=3),
        dbc.Col(dcc.Dropdown(
            id='geo-state',
            options=[{"label": s, "value": s} for s in states],
            placeholder="Select State",
            clearable=True
        ), width=3),
        dbc.Col(dcc.Dropdown(
            id='geo-purpose',
            options=[{"label": p, "value": p} for p in loan_purposes],
            placeholder="Select Loan Purpose",
            clearable=True
        ), width=3),
        dbc.Col(dcc.Dropdown(
            id='geo-metric',
            options=map_metrics,
            value='loan_amount',
            clearable=False
        ), width=3),
    ], className="mb-4"),

    # KPIs
    dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H5("✅ Top 5 Approval States", className="kpi-title"),
            html.Div(id="kpi-top-states", className="kpi-value")
        ]), className="kpi-card bg-success text-white shadow"), width=4),

        dbc.Col(dbc.Card(dbc.CardBody([
            html.H5("💰 Highest Avg Loan State", className="kpi-title"),
            html.Div(id="kpi-highest-loan", className="kpi-value")
        ]), className="kpi-card bg-info text-white shadow"), width=4),

        dbc.Col(dbc.Card(dbc.CardBody([
            html.H5("📦 Total Loan Volume", className="kpi-title"),
            html.Div(id="kpi-total-volume", className="kpi-value")
        ]), className="kpi-card bg-primary text-white shadow"), width=4),
    ], className="mb-4"),

    # Map
    dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([
            dcc.Graph(id="geo-map", style={"height": "550px"})
        ]), className="graph-card"), width=12)
    ], className="mb-4"),

    # Bar Chart: Loan Purpose vs Metric
    dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([
            dcc.Graph(id="geo-loanpurpose-bar", style={"height": "500px"})
        ]), className="graph-card"), width=12)
    ]),
])

# ─── Callback ───────────────────────────────────────────────────────────────
@callback(
    Output("geo-map", "figure"),
    Output("geo-loanpurpose-bar", "figure"),
    Output("kpi-top-states", "children"),
    Output("kpi-highest-loan", "children"),
    Output("kpi-total-volume", "children"),
    Input("geo-year",    "value"),
    Input("geo-state",   "value"),
    Input("geo-purpose", "value"),
    Input("geo-metric",  "value"),
)
def update_geographic_insights(year, state, purpose, selected_metric):
    dff = df.copy()
    if year:
        dff = dff[dff['activity_year'] == year]
    if state:
        dff = dff[dff['state_code'] == state]
    if purpose:
        dff = dff[dff['loan_purpose'] == purpose]

    # Map loan_purpose codes to labels
    purpose_map = {
        "1": "Home purchase",
        "2": "Home improvement",
        "31": "Refinancing",
        "32": "Cash-out refinancing",
        "4": "Other purpose"
    }
    dff['loan_purpose'] = (
        dff['loan_purpose']
        .astype(str)
        .map(purpose_map)
        .fillna(dff['loan_purpose'])
    )
    # Remove "Not applicable"
    dff = dff[dff['loan_purpose'] != "5"]
    dff = dff[dff['loan_purpose'] != "Not applicable"]

    # State-level stats for map
    state_stats = (
        dff.groupby('state_code')
           .agg(loan_amount=('loan_amount','mean'),
                property_value=('property_value','mean'),
                interest_rate=('interest_rate','mean'),
                application_count=('state_code','count'))
           .reset_index()
    )

    # Purpose-level stats for bar chart
    purpose_stats = (
        dff.groupby('loan_purpose')
           .agg(loan_amount=('loan_amount','mean'),
                property_value=('property_value','mean'),
                interest_rate=('interest_rate','mean'),
                application_count=('state_code','count'))
           .reset_index()
    )

    # Titles
    metric_label = {m['value']: m['label'] for m in map_metrics}[selected_metric]
    map_title = f"{metric_label} by State"
    bar_title = f"{metric_label} by Loan Purpose"

    map_fig = px.choropleth(
        state_stats,
        locations='state_code',
        locationmode='USA-states',
        color=selected_metric,
        scope="usa",
        title=map_title,
        color_continuous_scale="Viridis"
    )

    # Bar Chart
    bar_fig = px.bar(
        purpose_stats.sort_values(selected_metric, ascending=False),
        x='loan_purpose',
        y=selected_metric,
        title=bar_title,
        color=selected_metric,
        color_continuous_scale="Plasma"
    )

    # KPIs
    top_states = state_stats.nlargest(5, 'application_count')['state_code']
    top_states_str = ", ".join(top_states) if not top_states.empty else "N/A"

    highest_loan_state = state_stats.nlargest(1, 'loan_amount')
    highest_state_str = highest_loan_state.iloc[0]['state_code'] if not highest_loan_state.empty else "N/A"

    total_volume = f"{len(dff):,}"

    return map_fig, bar_fig, top_states_str, highest_state_str, total_volume
