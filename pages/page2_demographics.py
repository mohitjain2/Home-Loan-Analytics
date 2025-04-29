# 📁 File: pages/page2_demographics.py
import pandas as pd
import plotly.express as px
from dash import html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc
import sqlite3

# 🔄 Chunk loader for development
# Development mode limit
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

# Type conversions & cleaning
for col in ['loan_amount', 'debt_to_income_ratio', 'interest_rate']:
    df[col] = pd.to_numeric(df[col], errors='coerce')

df['applicant_age'] = df['applicant_age'].astype(str).str.strip()
df = df[df['applicant_age'].str.lower() != 'nan']
df['derived_sex'] = df['derived_sex'].astype(str).str.strip()
df['derived_race'] = df['derived_race'].astype(str).str.strip()
df['applicant_credit_score_type'] = df['applicant_credit_score_type'].astype(str).str.strip()

df['action_taken'] = pd.to_numeric(df['action_taken'], errors='coerce')
df['action_label'] = df['action_taken'].map({
    1: 'Loan Originated',
    2: 'Approved but Not Accepted',
    3: 'Denied',
    8: 'Preapproval Approved but Not Accepted'
})

# Dropdown options (cleaned)
races = sorted(df['derived_race'][~df['derived_race'].isin([
    "Free Form Text Only", "Joint", "Race Not Available"
])].dropna().unique())

sexes = sorted(df['derived_sex'][~df['derived_sex'].isin([
    "Sex Not Available"
])].dropna().unique())

ages = sorted(df['applicant_age'].dropna().unique())

# --- Layout ---
layout = html.Div([
    html.Div("📋 Applicant Demographics & Equity", className="page-title"),

    # Filters
    dbc.Row([
        dbc.Col(dcc.Dropdown(
            id='race-filter',
            options=[{"label": r, "value": r} for r in races],
            placeholder="Select Race",
            multi=True
        ), width=4),
        dbc.Col(dcc.Dropdown(
            id='age-filter',
            options=[{"label": a, "value": a} for a in ages],
            placeholder="Select Age Group",
            multi=True
        ), width=4),
        dbc.Col(dcc.Dropdown(
            id='sex-filter',
            options=[{"label": s, "value": s} for s in sexes],
            placeholder="Select Sex",
            multi=True
        ), width=4),
    ], className="mb-4"),

    # NEW KPIs
    dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H5("⚥ Highest Avg. Interest by Sex", className="kpi-title"),
            html.H2(id="kpi-top-sex-interest", className="kpi-value")
        ]), className="kpi-card bg-primary text-white shadow"), width=4),

        dbc.Col(dbc.Card(dbc.CardBody([
            html.H5("📈 Highest Avg. by Race", className="kpi-title"),
            html.H2(id="kpi-top-race-interest", className="kpi-value")
        ]), className="kpi-card bg-info text-white shadow"), width=4),

        dbc.Col(dbc.Card(dbc.CardBody([
            html.H5("👴 Highest Avg. by Age Group", className="kpi-title"),
            html.H2(id="kpi-top-age-interest", className="kpi-value")
        ]), className="kpi-card bg-success text-white shadow"), width=4),
    ], className="mb-5"),

    # Bubble Chart
    dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([
            html.H5(
                "Interest Rate Trends by Race, Age, and Sex",
                className="card-title text-center"
            ),
            dcc.Graph(
                id="bubble-interest-trends",
                style={"height": "1500px"}
            )
        ]), className="graph-card"), width=12)
    ])
])

# --- Callback ---
@callback(
    Output("kpi-top-sex-interest",      "children"),
    Output("kpi-top-race-interest",     "children"),
    Output("kpi-top-age-interest",      "children"),
    Output("bubble-interest-trends",    "figure"),
    Input("race-filter",                "value"),
    Input("age-filter",                 "value"),
    Input("sex-filter",                 "value")
)
def update_demographics(race_val, age_val, sex_val):
    # Base cleaning
    base = df[
        (~df['derived_race'].isin(["Free Form Text Only", "Joint", "Race Not Available"])) &
        (~df['derived_sex'].isin(["Sex Not Available"])) &
        (df['applicant_age'].str.lower() != "nan")
    ].copy()

    # Apply filters
    dff = base
    if race_val:
        dff = dff[dff['derived_race'].isin(race_val)]
    if age_val:
        dff = dff[dff['applicant_age'].isin(age_val)]
    if sex_val:
        dff = dff[dff['derived_sex'].isin(sex_val)]

    if dff.empty:
        return "N/A", "N/A", "N/A", {}

    # KPI 1: highest avg interest by sex
    sex_grp = dff.groupby('derived_sex')['interest_rate'].mean()
    top_sex = sex_grp.idxmax()
    top_sex_val = f"{sex_grp.max():.2f}%"
    top_sex_str = f"{top_sex}: {top_sex_val}"

    # KPI 2: race with highest avg interest
    race_grp = dff.groupby('derived_race')['interest_rate'].mean()
    top_race = race_grp.idxmax()
    top_race_val = f"{race_grp.max():.2f}%"
    top_race_str = f"{top_race}: {top_race_val}"

    # KPI 3: age group with highest avg interest
    age_grp = dff.groupby('applicant_age')['interest_rate'].mean()
    top_age = age_grp.idxmax()
    top_age_val = f"{age_grp.max():.2f}%"
    top_age_str = f"{top_age}: {top_age_val}"

    # Bubble Chart: avg interest by race/age/sex
    bubble_df = (
        dff
        .groupby(['derived_race', 'applicant_age', 'derived_sex'])['interest_rate']
        .mean()
        .reset_index(name='avg_interest_rate')
    )

    fig = px.scatter(
        bubble_df,
        x='derived_race',
        y='avg_interest_rate',
        size='avg_interest_rate',
        color='derived_sex',
        symbol='applicant_age',
        title="Avg Interest Rate by Race, Age & Sex",
        labels={'avg_interest_rate': 'Avg. Interest Rate (%)'},
        size_max=40,
        height=1500,
        hover_data={
            'derived_race': True,
            'applicant_age': True,
            'derived_sex': True,
            'avg_interest_rate': ':.2f'
        }
    )

    # scale bubbles appropriately
    fig.update_traces(marker=dict(
        sizemode='area',
        sizeref=2. * bubble_df['avg_interest_rate'].max() / (50 ** 2),
        sizemin=4,
        line=dict(width=1, color='DarkSlateGrey')
    ))
    fig.update_layout(
        margin={"t": 60, "l": 20, "r": 20, "b": 40},
        yaxis=dict(
            range=[4.2, 5.5],
            tick0=4,
            dtick=0.025,
            title="Avg Interest Rate"
        )
    )

    return top_sex_str, top_race_str, top_age_str, fig
