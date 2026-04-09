import dash
from dash import dcc, html, Input, Output, State, ctx, ALL, MATCH
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
from data_manager import get_chart_data, get_db_conn, get_watchlist_info

db_conn = get_db_conn()
candle_data = get_chart_data(db_conn)
watchlist_data = get_watchlist_info(db_conn)
chart_to_interval = {
    "5D": "30m",
    "1M": "1d",
    "6M": "1wk",
    "2Y": "1mo", 
    "5Y": "3mo"
}

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG, dbc.icons.BOOTSTRAP])
server = app.server

app.layout = dbc.Container([
    dcc.Location(id="url", refresh=False),

    # to preload plotly engine
    html.Div(dcc.Graph(figure=go.Figure(), style={'display': 'none'})),

    dbc.Row([
        dbc.Col(html.Small("TICKER", className="text-muted"), width=3),
        dbc.Col(html.Small("PRICE", className="text-muted"), width=3),
        dbc.Col(html.Small("DIST FROM 2YR LOW", className="text-muted"), width=6),
    ], className="px-3 mb-2 mt-4"),
    html.Div(id="list-container")
], fluid=True, style={"maxWidth": "800px"})


@app.callback(Output("list-container", "children"), Input("url", "pathname"))
def render_list(_):
    rows = []
    for stock_info in watchlist_data:
        ticker = stock_info["ticker"]
        quick_ratio = stock_info.get("quick_ratio", 0)
        quick_ratio_color = "#00ff88" if quick_ratio >= 1.0 else "#ffcc00"
        
        rows.append(html.Div([
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
                    chart_updates.append(create_chart(ticker, timeframe, chart_to_interval, candle_data))
                else:
                    new_arrows.append("bi bi-chevron-down")
                    chart_updates.append(html.Div())
            
            # handle if timeframe button clicked
            else:
                new_states.append(True)
                new_arrows.append("bi bi-chevron-up")
                chart_updates.append(create_chart(ticker, timeframe, chart_to_interval, candle_data))
        
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


def create_chart(ticker, timeframe, timeframe_map, candle_data):

    # getting candle data for given ticker and timeframe
    interval = timeframe_map.get(timeframe, "1mo")
    candle_df = candle_data.get(ticker, {}).get(interval, pd.DataFrame()).copy()
    candle_df = candle_df.reset_index(drop=True)
    if candle_df.empty: 
        return html.Div(f"No {interval} data", className="p-3 text-muted small")
    
    x_col = "timestamp"
    candle_df[x_col] = pd.to_datetime(candle_df[x_col])   
    min_p = candle_df["low"].min()
    max_p = candle_df["high"].max()
    pad = (max_p - min_p) * 0.3 
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
    elif timeframe == "2Y":
        tick_format = "%b %Y"
        hover_text_list = candle_df[x_col].dt.strftime("%b %Y").tolist()
    else:
        tick_format = "%Y"
        hover_text_list = []
        end_dates = candle_df[x_col] + pd.DateOffset(months = 2)
        for start_date, end_date in zip(candle_df[x_col], end_dates):
            hover_text_list.append(f"{start_date.strftime('%b %Y')} - {end_date.strftime('%b %Y')}")

    # format x indices and labels
    total_points = len(candle_df)
    tick_indices = [0, total_points // 3, (2 * total_points) // 3, total_points - 1]
    tick_indices = sorted(list(set(tick_indices)))
    tick_labels = [candle_df.iloc[i][x_col].strftime(tick_format) for i in tick_indices]

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
            "ticktext": tick_labels
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