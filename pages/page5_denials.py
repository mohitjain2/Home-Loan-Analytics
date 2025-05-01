# 📁 pages/page6_interest.py
import pandas as pd
import plotly.express as px
from dash import html, dcc, Input, Output, callback
import dash_bootstrap_components as dbc

# ─── Dev loader (first ~200k rows) ─────────────────────────────────────────
def load_chunked_data(path, chunksize=20000, max_rows=2000000):
    pieces, total = [], 0
    for chunk in pd.read_csv(path, chunksize=chunksize, low_memory=False):
        pieces.append(chunk)
        total += len(chunk)
        if total >= max_rows:
            break
    return pd.concat(pieces, ignore_index=True)

# ─── Data Load & Prep ──────────────────────────────────────────────────────
df = load_chunked_data("downsampled_2M.csv")
df["activity_year"] = pd.to_numeric(df["activity_year"], errors="coerce")
df["interest_rate"]  = pd.to_numeric(df["interest_rate"], errors="coerce")
df["derived_loan_product_type"] = df["derived_loan_product_type"].astype(str).str.strip()

# Map loan_type codes to labels
loan_type_map = {
    "1": "Conventional (not insured or guaranteed)",
    "2": "FHA insured",
    "3": "VA guaranteed",
    "4": "RHS/FSA guaranteed"
}
# Ensure loan_type is string, then map
df["loan_type"] = (
    df["loan_type"]
    .astype(str)
    .map(loan_type_map)
    .fillna(df["loan_type"])
)

# ─── Filter options ─────────────────────────────────────────────────────────
years      = sorted(df.activity_year.dropna().unique())
products   = sorted(df.derived_loan_product_type.dropna().unique())
loan_types = sorted(df.loan_type.dropna().unique())

# ─── Layout ────────────────────────────────────────────────────────────────
layout = html.Div([
    html.H3("📈 Interest Rate Distribution", className="page-title"),

    dbc.Row([
        dbc.Col(dcc.RangeSlider(
            id="year-slider",
            min=int(min(years)), max=int(max(years)), step=1,
            value=[int(min(years)), int(max(years))],
            marks={int(y): str(y) for y in years}
        ), width=6),
        dbc.Col(dcc.Dropdown(
            id="product-filter",
            options=[{"label": p, "value": p} for p in products],
            placeholder="Loan Product",
            clearable=True
        ), width=3),
        dbc.Col(dcc.Dropdown(
            id="type-filter",
            options=[{"label": lt, "value": lt} for lt in loan_types],
            placeholder="Loan Type",
            clearable=True
        ), width=3),
    ], className="mb-4"),

    dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([
            dcc.Graph(id="hist-by-product", style={"height": "600px", "width" : "580px"})
        ]), className="graph-card"), width=6),
        dbc.Col(dbc.Card(dbc.CardBody([
            dcc.Graph(id="hist-by-loantype", style={"height": "600px", "width" : "580px"})
        ]), className="graph-card"), width=6),
    ])
])

# ─── Callbacks ───────────────────────────────────────────────────────────────
@callback(
    Output("hist-by-product",   "figure"),
    Output("hist-by-loantype",  "figure"),
    Input("year-slider",        "value"),
    Input("product-filter",     "value"),
    Input("type-filter",        "value"),
)
def update_interest_histograms(year_range, prod, loantype):
    dff = df[
        (df.activity_year >= year_range[0]) &
        (df.activity_year <= year_range[1])
    ]
    if prod:
        dff = dff[dff.derived_loan_product_type == prod]
    if loantype:
        dff = dff[dff.loan_type == loantype]

    # Histogram by derived loan product type
    fig_prod = px.histogram(
        dff,
        x="interest_rate",
        color="derived_loan_product_type",
        nbins=50,
        title="Interest Rate by Loan Product",
        labels={"interest_rate": "Interest Rate (%)"}
    )
    fig_prod.update_layout(barmode="overlay", bargap=0.1)

    # Histogram by loan_type
    fig_type = px.histogram(
        dff,
        x="interest_rate",
        color="loan_type",
        nbins=50,
        title="Interest Rate by Loan Type",
        labels={"interest_rate": "Interest Rate (%)"}
    )
    fig_type.update_layout(barmode="overlay", bargap=0.1)

    return fig_prod, fig_type
