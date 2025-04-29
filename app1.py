# 📁 app.py
import dash
from dash import dcc, html, Input, Output
import dash_bootstrap_components as dbc

# Import pages
from pages import page1_overview, page2_demographics, page4_geographic, page5_denials

# Initialize Dash app
app1 = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app1.title = "Home Loan Analytics Dashboard"
server = app1.server

# Layout with Tabs
app1.layout = dbc.Container([
    html.H1("🏠 Home Loan Analytics", className="text-center my-4"),

    dbc.Tabs([
        dbc.Tab(label="📊 Loan Overview", tab_id="overview"),
        dbc.Tab(label="👥 Demographics & Equity", tab_id="demographics"),
        dbc.Tab(label="🌍 Geographic Trends", tab_id="geographic"),
        dbc.Tab(label="📈 Interest Rate Distribution ", tab_id="rate")

    ], id="tabs", active_tab="overview"),

    html.Div(id="page-content", className="mt-4")
], fluid=True)

# Callback to switch between pages
@app1.callback(
    Output("page-content", "children"),
    Input("tabs", "active_tab")
)
def render_tab_content(active_tab):
    try:
        if active_tab == "overview":
            return page1_overview.layout
        elif active_tab == "demographics":
            return page2_demographics.layout
        elif active_tab == "geographic":
            return page4_geographic.layout
        elif active_tab == "rate":
            return page5_denials.layout
        return html.Div("Page Not Found")
    except Exception as e:
        import traceback; traceback.print_exc()
        # return a friendly error in the UI so the front-end at least gets a response:
        return html.Div(f"⚠️ Error in server callback: {e}")


if __name__ == "__main__":
    print("Running app...")
    app1.run(debug=True)
