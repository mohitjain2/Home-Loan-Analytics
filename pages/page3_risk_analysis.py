# 📁 File: pages/page3_risk_analysis.py
import pandas as pd
import plotly.express as px
from dash import html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc
import sqlite3

# ✅ Load Data Function (assumes preprocessed)
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

# Map action codes to labels
action_labels = {
    1: 'Loan Originated',
    2: 'Approved but Not Accepted',
    3: 'Denied',
    8: 'Preapproval Approved but Not Accepted'
}
df['action_taken'] = pd.to_numeric(df['action_taken'], errors='coerce')
df['action_label'] = df['action_taken'].map(action_labels)

# Dropdown values
limits = sorted(df['conforming_loan_limit'].dropna().unique())
types = sorted(df['loan_type'].dropna().unique())
liens = sorted(df['lien_status'].dropna().unique())
hoepas = sorted(df['hoepa_status'].dropna().unique())

# Layout
layout = html.Div([
    html.Div("📊 Loan Risk & Characteristics", className="page-title"),

    # Filters
    dbc.Row([
        dbc.Col(dcc.Dropdown(id='limit-filter', options=[{"label": l, "value": l} for l in limits],
                              placeholder="Conforming Loan Limit", multi=True), width=3),
        dbc.Col(dcc.Dropdown(id='type-filter', options=[{"label": t, "value": t} for t in types],
                              placeholder="Loan Type", multi=True), width=3),
        dbc.Col(dcc.Dropdown(id='lien-filter', options=[{"label": l, "value": l} for l in liens],
                              placeholder="Lien Status", multi=True), width=3),
        dbc.Col(dcc.Dropdown(id='hoepa-filter', options=[{"label": h, "value": h} for h in hoepas],
                              placeholder="HOEPA Status", multi=True), width=3)
    ], className="mb-4"),

    # KPIs
    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H5("📈 Avg. Loan-to-Value Ratio", className="kpi-title"),
                html.H2(id="kpi-ltv", className="kpi-value")
            ])
        ], className="kpi-card bg-primary text-white shadow")),

        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H5("🏠 Avg. Property Value", className="kpi-title"),
                html.H2(id="kpi-property", className="kpi-value")
            ])
        ], className="kpi-card bg-success text-white shadow")),

        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H5("🚫 % Non-Conforming Loans", className="kpi-title"),
                html.H2(id="kpi-nonconf", className="kpi-value")
            ])
        ], className="kpi-card bg-danger text-white shadow"))
    ], className="mb-5"),

    # Graphs
    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H5("DTI vs Loan Amount by Action Taken", className="card-title text-center"),
                dcc.Graph(id="scatter-dti-loan")
            ])
        ], className="graph-card"), width=6),

        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H5("Interest Rate Distribution", className="card-title text-center"),
                dcc.Graph(id="hist-interest")
            ])
        ], className="graph-card"), width=6)
    ]),

    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardBody([
                html.H5("Loan Amount by Lien Status", className="card-title text-center"),
                dcc.Graph(id="box-loan-lien")
            ])
        ], className="graph-card"))
    ])
])

# Callback
@callback(
    Output("kpi-ltv", "children"),
    Output("kpi-property", "children"),
    Output("kpi-nonconf", "children"),
    Output("scatter-dti-loan", "figure"),
    Output("hist-interest", "figure"),
    Output("box-loan-lien", "figure"),
    Input("limit-filter", "value"),
    Input("type-filter", "value"),
    Input("lien-filter", "value"),
    Input("hoepa-filter", "value")
)
def update_risk(limit_val, type_val, lien_val, hoepa_val):
    filtered_df = df.copy()
    if limit_val:
        filtered_df = filtered_df[filtered_df['conforming_loan_limit'].isin(limit_val)]
    if type_val:
        filtered_df = filtered_df[filtered_df['loan_type'].isin(type_val)]
    if lien_val:
        filtered_df = filtered_df[filtered_df['lien_status'].isin(lien_val)]
    if hoepa_val:
        filtered_df = filtered_df[filtered_df['hoepa_status'].isin(hoepa_val)]

    if filtered_df.empty:
        return "0%", "$0", "0%", {}, {}, {}

    # KPIs
    avg_ltv = f"{filtered_df['loan_to_value_ratio'].mean():.2f}%"
    avg_property = f"${filtered_df['property_value'].mean():,.0f}"
    non_conf_ratio = len(filtered_df[filtered_df['conforming_loan_limit'] == 'NC']) / len(filtered_df) * 100
    non_conf_fmt = f"{non_conf_ratio:.2f}%"

    # Scatter
    scatter_fig = px.scatter(filtered_df, x='debt_to_income_ratio', y='loan_amount', color='action_label',
                             title='Debt-to-Income vs Loan Amount', labels={
                                 'debt_to_income_ratio': 'DTI Ratio',
                                 'loan_amount': 'Loan Amount'
                             })

    # Histogram
    hist_fig = px.histogram(filtered_df, x='interest_rate', nbins=40, color_discrete_sequence=['#636EFA'])

    # Box plot
    box_fig = px.box(filtered_df, x='lien_status', y='loan_amount', color='lien_status')

    return avg_ltv, avg_property, non_conf_fmt, scatter_fig, hist_fig, box_fig