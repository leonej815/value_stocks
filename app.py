from dotenv import load_dotenv
load_dotenv()
import dash
from dash import dcc, html, Input, Output, State, ctx, ALL, MATCH
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
from data_manager import get_chart_data, get_db_conn, get_watchlist_info


db_conn = get_db_conn()

# (dict[str, dict[str, Dataframe]]): nested dictionary where first level keys are stock symbols and second level keys are the candle intervals and the value is the associated candle dataframe
CANDLE_DATA = get_chart_data(db_conn)

WATCHLIST_DATA = get_watchlist_info(db_conn)
TIMEFRAME_MAP = {
    "5D": "30m",
    "1M": "1d",
    "6M": "1wk",
    "2Y": "1mo", 
    "5Y": "1mo"
}

app = dash.Dash(__name__, title="Value Stocks", external_stylesheets=[dbc.themes.CYBORG, dbc.icons.BOOTSTRAP])
server = app.server

app.layout = dbc.Container([
    dcc.Location(id="url", refresh=False),

    # to preload plotly engine
    html.Div(dcc.Graph(figure=go.Figure(), style={'display': 'none'})),

    # watchlist header row
    dbc.Row([
        dbc.Col(html.Small("TICKER", className="text-muted"), width=3),
        dbc.Col(html.Small("PRICE", className="text-muted"), width=3),
        dbc.Col(html.Small("DIST FROM 2YR LOW", className="text-muted"), width=6),
    ], className="px-3 mb-2 mt-4"),

    # container to be filled by callback function "render_list"
    html.Div(id="list-container")

], fluid=True, style={"maxWidth": "800px"})


@app.callback(Output("list-container", "children"), Input("url", "pathname"))
def render_list(_):
    """Renders the interactive stock watchlist UI components. Iterates over the global "watchlist_data" list and constructs dynamic Bootstrap list items containing ticker metadata, financial metrics, and collapsible chart containers with timeframe selector buttons.

    Args:
        _: Unused pathname trigger from the "url" Location component.

    Returns:
        list[html.Div]: A list of Dash HTML Div elements, each containing a 
        "dbc.ListGroupItem" header and a corresponding "dbc.Collapse" section.
    """

    rows = []
    for stock_info in WATCHLIST_DATA:
        ticker = stock_info["ticker"]
        quick_ratio = stock_info.get("quick_ratio", 0)
        quick_ratio_color = "#00ff88" if quick_ratio >= 1.0 else "#ffcc00"
        
        rows.append(html.Div([
            # watchlist row showing ticker, price, distance from 2 yr low and the expandable arrow
            dbc.ListGroupItem([
                dbc.Row([
                    dbc.Col(
                        html.A(
                            html.Strong(ticker, className="ticker-symbol", style={"fontSize": "1.1rem", "color": "#00d4ff"}),
                            href=f"https://www.google.com/finance/quote/{ticker}:{stock_info.get('exchange').upper()}",
                            target="_blank",
                            id={"type": "ticker-link", "index": ticker},
                            n_clicks=0,
                            className="ticker-link-anchor"
                        ),
                        width=3,
                    ),
                    dbc.Col(f"${stock_info['price']:.2f}", width=3, className="text-info"),
                    dbc.Col(f"{stock_info['dist_from_low']:.1f}%", width=4, className="fw-bold"),
                    dbc.Col(html.I(className="bi bi-chevron-down", id={"type": "arrow", "index": ticker}), width=2, className="text-end text-muted"),
                ], align="center"),
            ], 
            id={"type": "row", "index": ticker},
            n_clicks=0,
            action=True, 
            className="stock-row-container",
            style={"cursor": "pointer", "border": "none", "borderBottom": "1px solid #333", "padding": "12px 15px"}
            ),

            # element that shows when the user clicks to expand a row
            dbc.Collapse(
                html.Div([
                    dbc.Row([
                        dbc.Col([html.Small(stock_info.get("name", "N/A"), className="me-3")], width=10),
                    ], className="ms-1 mt-1"),
                    dbc.Row([
                        dbc.Col([
                            html.Small("DIV: ", className="text-muted"),
                            html.Small(f"{stock_info.get('dividend_yield', 0):.1f}%", className="me-3"),
                            html.Small("FCF: ", className="text-muted"),
                            html.Small(f"{stock_info.get('fcf_yield', 0):.1f}%", className="me-3"),
                            html.Small("EV/E: ", className="text-muted"),
                            html.Small(f"{stock_info.get('ev_ebitda', 0):.1f}", className="me-3"),
                            html.Small("QR: ", className="text-muted"),
                            html.Small(f"{quick_ratio:.1f}", style={"color": quick_ratio_color}),
                        ], width=10),
                    ], className="ms-1"),
                    dbc.Row([
                        dbc.Col(
                            html.Div(
                                id={"type": "chart-content", "index": ticker},
                                style={"minHeight": "230px", "backgroundColor": "#111"}
                            ), 
                            width=10,
                        ),
                        dbc.Col([
                            dbc.ButtonGroup([
                                dbc.Button("5D", id={"type": "tf-btn", "index": ticker, "tf": "5D"}, size="sm", color="secondary", outline=True),
                                dbc.Button("1M", id={"type": "tf-btn", "index": ticker, "tf": "1M"}, size="sm", color="secondary", outline=True),
                                dbc.Button("6M", id={"type": "tf-btn", "index": ticker, "tf": "6M"}, size="sm", color="secondary", outline=True),
                                dbc.Button("2Y", id={"type": "tf-btn", "index": ticker, "tf": "2Y"}, size="sm", color="dark", outline=False),
                                dbc.Button("5Y", id={"type": "tf-btn", "index": ticker, "tf": "5Y"}, size="sm", color="secondary", outline=True),
                            ], vertical=True, className="w-100 mt-2")
                        ], width=2, className="d-flex align-items-center")
                    ], className="g-0 p-2 mt-1", style={"backgroundColor": "#111"}),
                ]),
                id={"type": "collapse", "index": ticker}, is_open=False
            )
        ]))
    return rows


@app.callback(
    [Output({"type": "collapse", "index": ALL}, "is_open"),
     Output({"type": "arrow", "index": ALL}, "className"),
     Output({"type": "chart-content", "index": ALL}, "children"),
     Output({"type": "tf-btn", "index": ALL, "tf": ALL}, "color"),
     Output({"type": "tf-btn", "index": ALL, "tf": ALL}, "outline")],
    [Input({"type": "row", "index": ALL}, "n_clicks"),
     Input({"type": "tf-btn", "index": ALL, "tf": ALL}, "n_clicks"),
     Input({"type": "ticker-link", "index": ALL}, "n_clicks")],
    [State({"type": "collapse", "index": ALL}, "is_open"),
     State({"type": "chart-content", "index": ALL}, "children")],
    prevent_initial_call=True
)
def handle_accordion(row_clicks, btn_clicks, link_clicks, current_states, current_charts):
    """Handles accordion expansion, timeframe selection, and active button styling. Processes pattern-matching callback triggers across the watchlist table. Toggling a row expands/collapses its chart and rotates the chevron arrow. Clicking a timeframe button keeps the container open, fetches a new chart figure via "create_chart", and updates button active states. Clicking direct ticker links bypasses state updates.

    Args:
        row_clicks (list[int]): Click counts for each stock row component.
        btn_clicks (list[int]): Click counts for all timeframe selector buttons.
        link_clicks (list[int]): Click counts for external anchor links.
        current_states (list[bool]): Current "is_open" boolean states of collapse components.
        current_charts (list[dash.development.base_component.Component]): Current child components 
            of the chart containers.

    Returns:
        tuple[list[bool], list[str], list[Component], list[str], list[bool]]: A 5-element 
        tuple matching the callback outputs:
            - new_states: Updated "is_open" flags for accordion sections.
            - new_arrows: Bootstrap icon class names ("bi-chevron-up" / "bi-chevron-down").
            - chart_updates: Rendered chart components or empty "html.Div" elements.
            - button_colors: Bootstrap color names ('dark' for active, 'secondary' for inactive).
            - button_outlines: Outline flags ("False" for active, "True" for inactive).
    """

    # checks if user didn't trigger the callback and returns nothing if so
    if not ctx.triggered:
        return [dash.no_update] * 5   
    trigger = ctx.triggered_id
    
    # stop toggle if ticker link clicked
    if trigger.get("type") == "ticker-link":
        return [dash.no_update] * 5

    # store info from clicked html element
    clicked_ticker = trigger["index"]
    timeframe = trigger.get("tf", "2Y")
    
    new_states = []
    new_arrows = [] 
    chart_updates = []
    button_colors = []
    button_outlines = []
    
    # looping through #collapse elements
    for i, output in enumerate(ctx.outputs_list[0]): 
        ticker = output["id"]["index"]
        is_currently_open = current_states[i] 
        
        # handle the clicked row
        if ticker == clicked_ticker:
            if trigger.get("type") == "row":

                # change from closed to open or from open to closed and change chevron direction and show or hide chart
                to_open = not is_currently_open
                new_states.append(to_open)
                if to_open:
                    new_arrows.append("bi bi-chevron-up")
                    chart_updates.append(create_chart(ticker, timeframe))
                else:
                    new_arrows.append("bi bi-chevron-down")
                    chart_updates.append(html.Div())
            
            # handle if timeframe button clicked
            else:
                new_states.append(True)
                new_arrows.append("bi bi-chevron-up")
                chart_updates.append(create_chart(ticker, timeframe))
        
        # handle the rows that weren't clicked
        else:
            new_states.append(False)
            new_arrows.append("bi bi-chevron-down")
            chart_updates.append(html.Div())

    # handle color of timeframe buttons when timeframe button clicked
    for btn_output in ctx.outputs_list[3]:
        btn_ticker = btn_output["id"]["index"]
        btn_tf = btn_output["id"]["tf"]
        if btn_ticker == clicked_ticker and btn_tf == timeframe:
            button_colors.append("dark")
            button_outlines.append(False)
        else:
            button_colors.append("secondary")
            button_outlines.append(True)
            
    return new_states, new_arrows, chart_updates, button_colors, button_outlines


def create_chart(ticker, timeframe):
    """Creates a chart and returns it as a dash component
    Inputs:
        ticker (str): stock symbol
        timeframe (str): string that represents the timeframe from starting candle to ending candle
    Outputs:
        dcc.Graph: chart element to be inserted into the page
    """

    # getting candle data for given ticker and timeframe
    interval = TIMEFRAME_MAP.get(timeframe, "1mo")
    candle_df = CANDLE_DATA.get(ticker, {}).get(interval, pd.DataFrame()).copy()
    candle_df = candle_df.reset_index(drop=True)
    if candle_df.empty: 
        return html.Div(f"No {interval} data", className="p-3 text-muted small")

    # 5 year and 2 year both use the same 1 month candle interval so the dataframe needs to be reduced for the 2 year timeframe
    if timeframe == "2Y":
        candle_df = candle_df.tail(24).reset_index(drop=True)
    
    x_col = "timestamp"
    candle_df[x_col] = pd.to_datetime(candle_df[x_col])

    # get the lowest low and highest high  
    min_p = candle_df["low"].min()
    max_p = candle_df["high"].max()

    # calculate padding based on the high and low points
    pad = (max_p - min_p) * 0.3

    # calculate the vertical range of the chart using the min and max and padding
    y_range = [min_p - pad, max_p + pad]
    
    # format hovertext for each interval
    if timeframe == "5D":
        tick_format = "%a %H:%M"
        hover_text_list = candle_df[x_col].dt.strftime("%b %d, %H:%M").tolist()
    elif timeframe == "1M":
        tick_format = "%b %d"
        hover_text_list = candle_df[x_col].dt.strftime("%b %d, %Y").tolist()
    elif timeframe == "6M":
        tick_format = "%b %y"
        hover_text_list = candle_df[x_col].dt.strftime("Week of %b %d, %Y").tolist()
    elif timeframe in ["2Y", "5Y"]:
        tick_format = "%b %Y"
        hover_text_list = candle_df[x_col].dt.strftime("%b %Y").tolist()

    # format x indices and labels
    total_points = len(candle_df)
    tick_indices = [0, total_points // 3, (2 * total_points) // 3, total_points - 1]
    tick_indices = sorted(list(set(tick_indices)))
    tick_labels = [candle_df.iloc[i][x_col].strftime(tick_format) for i in tick_indices]

    # create candlestick figure
    fig = go.Figure(data=[go.Candlestick(
        x=candle_df.index,
        open=candle_df["open"], 
        high=candle_df["high"], 
        low=candle_df["low"], 
        close=candle_df["close"],
        increasing_line_color="#00ff88", 
        decreasing_line_color="#ff3333",
        customdata=hover_text_list,
        hovertemplate="<b>%{customdata}</b><br>O: %{open:.2f} H: %{high:.2f}<br>L: %{low:.2f} C: %{close:.2f}<extra></extra>"
    )])

    # add annotations
    hi_pos = candle_df['high'].idxmax()
    lo_pos = candle_df['low'].idxmin()
    fig.add_annotation(
        x=hi_pos, 
        y=max_p, 
        text=f"HI: ${max_p:.2f}", 
        showarrow=False, 
        yanchor="bottom", 
        font=dict(size=10, color="#00ff88"), 
        yshift=5
    )
    fig.add_annotation(
        x=lo_pos, 
        y=min_p, 
        text=f"LO: ${min_p:.2f}", 
        showarrow=False, 
        yanchor="top", 
        font=dict(size=10, color="#ff3333"), yshift=-5
    )

    # update chart display
    fig.update_layout(
        template="plotly_dark", 
        height=230, 
        margin=dict(l=5, r=45, t=20, b=25),
        xaxis_rangeslider_visible=False, showlegend=False, uirevision=timeframe, dragmode=False,
        xaxis={
            "visible": True, 
            "type": "category", 
            "tickfont": {"size": 9, "color": "gray"}, 
            "showgrid": False, 
            "fixedrange": True, 
            "tickmode": "array", 
            "tickvals": tick_indices, 
            "ticktext": tick_labels,
            "ticks": "outside", # draws tiny tick marks pointing toward labels
            "ticklen": 6, # length of the tick mark in pixels
            "tickwidth": 1.5, # thickness of the tick line
            "tickcolor": "gray", # Color of the tick line
            "showline": True, # ensures the horizontal baseline is visible for ticks to anchor to
            "linecolor": "gray" # color of the horizontal baseline
        },
        yaxis={
            "visible": True, 
            "side": "right", 
            "tickformat": "$.2f", 
            "tickfont": {"size": 9, "color": "gray"}, 
            "showgrid": True, 
            "gridcolor": "rgba(255,255,255,0.05)", 
            "range": y_range, 
            "fixedrange": True, 
            "autorange": False
        },
        paper_bgcolor="rgba(0,0,0,0)", 
        plot_bgcolor="rgba(0,0,0,0)", 
        hovermode="closest"
    )

    return dcc.Graph(
        id={"type": "stock-graph", "index": ticker, "tf": timeframe}, 
        figure=fig, 
        config={"displayModeBar": False}
    )


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8050)