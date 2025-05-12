import dash
from dash import dcc, html, Input, Output
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta, timezone
import plotly.express as px
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from plotly.colors import qualitative
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

# Load env vars
load_dotenv("config/.env")

# Mongo & Qdrant setup
mongo = MongoClient(os.getenv("MONGODB_STRING", "mongodb://localhost:27017/"))["crisiscast"]["unified_posts"]
qdrant = QdrantClient(host="127.0.0.1", port=6333)
embed_model = SentenceTransformer("all-MiniLM-L6-v2")
QCOL = "post_vectors"

# Icon and color mappings
CRISIS_ICONS = {
    "war": "🚨", "pandemic": "🦠", "cyberattack": "💻", "terrorist_attack": "🔫",
    "natural_disaster": "🌪️", "civil_unrest": "🔥", "crime": "🚓",
    "financial_crisis": "📉", "infrastructure_failure": "🏗️", "environmental_crisis": "🌿"
}

CRISIS_COLORS = {
    "war": "danger", "pandemic": "info", "cyberattack": "primary", "terrorist_attack": "danger",
    "natural_disaster": "warning", "civil_unrest": "dark", "crime": "secondary",
    "financial_crisis": "info", "infrastructure_failure": "light", "environmental_crisis": "success"
}

# Helper functions for MongoDB aggregation (to replace MongoStorage methods)
def get_count_by_type_over_time(from_date=None, to_date=None, unit="minute"):
    """Replacement for mongo.get_count_by_type_over_time method"""
    match_stage = {}
    if from_date or to_date:
        match_stage["timestamp"] = {}
        if from_date:
            # Convert datetime to ISO string format for matching
            if isinstance(from_date, datetime):
                from_date = from_date.isoformat()
            match_stage["timestamp"]["$gte"] = from_date
        if to_date:
            # Convert datetime to ISO string format for matching
            if isinstance(to_date, datetime):
                to_date = to_date.isoformat()
            match_stage["timestamp"]["$lte"] = to_date

    # Format for grouping based on time unit
    date_format = {
        "minute": "%Y-%m-%d %H:%M:00",
        "hour": "%Y-%m-%d %H:00:00",
        "day": "%Y-%m-%d 00:00:00"
    }

    pipeline = [
        {"$match": match_stage} if match_stage else {"$match": {}},
        # First convert the timestamp string to a date object
        {"$addFields": {
            "timestamp_date": {
                "$dateFromString": {
                    "dateString": "$timestamp",
                    "timezone": "UTC"
                }
            }
        }},
        # Then format the date to the requested unit granularity
        {"$project": {
            "crisis_type": 1,
            "date_str": {"$dateToString": {
                "format": date_format.get(unit, "%Y-%m-%d %H:%M:00"), 
                "date": "$timestamp_date"
            }}
        }},
        {"$group": {
            "_id": {"crisis_type": "$crisis_type", "date": "$date_str"},
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id.date": 1}},
        {"$project": {
            "_id": 0,
            "crisis_type": "$_id.crisis_type",
            "date": "$_id.date",
            "count": "$count"
        }}
    ]
    
    try:
        result = list(mongo.aggregate(pipeline))
        df = pd.DataFrame(result)
        
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            
            # Filter out 'none' crisis types and empty values
            df = df[df['crisis_type'].notna()]
            df = df[df['crisis_type'] != 'none']
            df = df[df['crisis_type'] != '']
            
        return df
    except Exception as e:
        print(f"Error in get_count_by_type_over_time: {e}")
        return pd.DataFrame()  # Return empty dataframe on error

# Time series chart for total crisis count with smoother visualization
def draw_initial_time_series():
    end_time = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start_time = end_time - timedelta(hours=12)  # Get data for the last 12 hours
    
    # Convert to ISO format for MongoDB query
    end_time_iso = end_time.isoformat()
    start_time_iso = start_time.isoformat()
    
    df = get_count_by_type_over_time(from_date=start_time_iso, to_date=end_time_iso, unit="minute")
    fig = go.Figure()
    
    if not df.empty:
        # Group by date to get total count across all crisis types
        total_by_time = df.groupby('date')['count'].sum().reset_index()
        
        # Create a more aggregated view by resampling to 30-minute intervals
        if len(total_by_time) > 10:  # Only resample if we have enough data
            # Convert to pandas datetime index for resampling
            total_by_time.set_index('date', inplace=True)
            resampled = total_by_time.resample('30min').sum().reset_index()
            
            # Add trace for total count - area chart for better trend visualization
            fig.add_trace(go.Scatter(
                x=resampled['date'],
                y=resampled['count'],
                mode='lines',
                name='Total Crisis Reports',
                line=dict(color='#FF4136', width=3, shape='spline', smoothing=1.3),  # Smooth curved line
                fill='tozeroy',  
                fillcolor='rgba(255, 65, 54, 0.3)'  # Transparent red
            ))
        else:
            
            total_by_time = total_by_time.sort_values('date')
            fig.add_trace(go.Scatter(
                x=total_by_time['date'],
                y=total_by_time['count'],
                mode='lines',
                name='Total Crisis Reports',
                line=dict(color='#FF4136', width=3, shape='spline', smoothing=1.3),
                fill='tozeroy',
                fillcolor='rgba(255, 65, 54, 0.3)'
            ))
                
    # Add layout settings
    fig.update_layout(
        title="Total Crisis Reports Over Time",
        xaxis_title="Time",
        yaxis_title="Number of Reports",
        hovermode="x unified",
        showlegend=False, 
        height=400,  # Reduced height
        margin=dict(l=50, r=50, t=50, b=50),
        plot_bgcolor='white',  # White background
        paper_bgcolor='white',  # White background for entire plot
        font=dict(color='black')  # Black text
    )
    
    # Improve the grid and axes
    fig.update_xaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor='rgba(211, 211, 211, 0.5)',
        zeroline=False,
        tickangle=0,
        tickformat='%H:%M\n%b %d',  # More readable time format
        color='black'  # Black axis labels
    )
    
    fig.update_yaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor='rgba(211, 211, 211, 0.5)',
        zeroline=False,
        color='black'  # Black axis labels
    )
    
    return fig

# Bar chart
def draw_crisis_distribution_bar():
    from_date = datetime.now(timezone.utc) - timedelta(hours=6)
    to_date = datetime.now(timezone.utc)
    
    # Converting to ISO format for MongoDB query
    from_date_iso = from_date.isoformat()
    to_date_iso = to_date.isoformat()
    
    df = get_count_by_type_over_time(
        from_date=from_date_iso,
        to_date=to_date_iso,
        unit="hour"
    )
    if df.empty:
        return px.bar(title="No data available")
    
    total_df = df.groupby("crisis_type")["count"].sum().reset_index()
    total_df = total_df.sort_values(by="count", ascending=False)

    fig = px.bar(
        total_df,
        x="count",
        y="crisis_type",
        orientation="h",
        color="crisis_type",
        color_discrete_sequence=qualitative.Set3,
        text="count",
        title="🔥 Total Crisis Mentions (Last 6 Hours)",
        labels={"count": "Mentions", "crisis_type": "Crisis Type"}
    )

    fig.update_traces(
        textposition="outside",
        marker_line_width=1.5,
        marker_line_color="black"
    )
    fig.update_layout(
        yaxis=dict(title=None),
        xaxis=dict(title="Mentions"),
        showlegend=False,
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(color="black", size=14),
    )

    return fig

# Heatmap
def draw_crisis_heatmap():
    from_date = datetime.now(timezone.utc) - timedelta(days=1)
    to_date = datetime.now(timezone.utc)
    
    # Convert to ISO format for MongoDB query
    from_date_iso = from_date.isoformat()
    to_date_iso = to_date.isoformat()
    
    df = get_count_by_type_over_time(
        from_date=from_date_iso,
        to_date=to_date_iso,
        unit="hour"
    )
    
    if df.empty:
        return px.imshow([[0]], labels=dict(x="Hour", y="Crisis Type", color="Count"))
    
    # Extract hour from datetime for better display
    df["hour"] = df["date"].dt.strftime("%H:%M")
    
    # Handle potential duplicates by summing counts for same crisis_type and hour
    df = df.groupby(['crisis_type', 'hour'])['count'].sum().reset_index()
    
    # Now create the pivot table with the deduplicated data
    pivot = df.pivot(index="crisis_type", columns="hour", values="count").fillna(0)
    
    fig = px.imshow(
        pivot,
        labels=dict(x="Hour", y="Crisis Type", color="Mentions"),
        aspect="auto",
        title="Crisis Frequency Heatmap (Last 24h)",
        color_continuous_scale="Viridis"
    )
    
    # Improving readability
    fig.update_layout(
        xaxis=dict(tickangle=45, color='black'),
        yaxis=dict(autorange="reversed", color='black'),  # To show most important crises at top
        coloraxis_colorbar=dict(title="Mentions", tickfont=dict(color='black'), title_font=dict(color='black')),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(color="black"),
        height=400,  # Reduced height
        margin=dict(l=50, r=50, t=50, b=50)
    )
    
    return fig

# Emergency contacts section
def emergency_contacts():
    return dbc.Card([
        dbc.CardHeader("🚨 Emergency Services"),
        dbc.CardBody([
            html.Ul([
                html.Li("🚑 Medical: 911"),
                html.Li("🚒 Fire: 101"),
                html.Li("🚓 Police: 100"),
                html.Li("🧠 Mental Health: 988"),
                html.Li("🌪️ FEMA: 1-800-621-3362")
            ])
        ])
    ], className="mt-3")

# DASH APP
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])
app.title = "CrisisCast Dashboard"

app.layout = dbc.Container([
    dcc.Tabs([
        dcc.Tab(label="📅 Live Feed + Insights", children=[
            dbc.Row([
                dbc.Col([
                    html.H2("📅 Real-Time Crisis Feed"),
                    html.Div(id="feed-container"),
                    dcc.Interval(id="feed-interval", interval=10*1000, n_intervals=0)
                ], width=6),

                dbc.Col([
                    html.H2("📊 Crisis Analytics"),
                    dcc.Graph(id="crisis-bar-chart", className="mb-4"),  # Added margin-bottom
                    dcc.Interval(id="analytics-interval", interval=20*1000, n_intervals=0),
                    dcc.Graph(id="time-series", figure=draw_initial_time_series(), className="mb-4"),  # Added margin-bottom
                    dcc.Interval(id="time-series-interval", interval=20*1000, n_intervals=0),
                    dcc.Graph(id="crisis-heatmap", className="mb-4"),  # Added margin-bottom
                    dcc.Interval(id="heatmap-interval", interval=20*1000, n_intervals=0),
                    emergency_contacts()  # Moved emergency contacts here
                ], width=6),
            ], className="mt-4"),
        ]),

        dcc.Tab(label="🔎 Semantic Search", children=[
            dbc.Row([
                dbc.Col([
                    html.H2("🔍 Search Crisis Info"),
                    dcc.Input(id="search-input", placeholder="Type a query...", type="text", style={"width": "100%"}),
                    html.Div(id="search-results", className="mt-3")
                ], width=12)
            ], className="mt-4")
        ])
    ])
], fluid=True, className="p-4")

# CALLBACKS
@app.callback(
    Output("feed-container", "children"),
    Input("feed-interval", "n_intervals")
)
def update_feed(n):
    raw = list(
        mongo.find({"crisis_type": {"$ne": "none"}})
             .sort("timestamp", -1)
             .limit(50)
    )
    seen_titles = set()
    unique = []
    for doc in raw:
        title = doc.get("title", "").strip().lower()
        if title and title not in seen_titles:
            seen_titles.add(title)
            unique.append(doc)
        if len(unique) >= 10:
            break

    cards = []
    for d in unique:
        crisis = d.get("crisis_type", "none")
        cards.append(
            dbc.Alert([
                html.H5(f"{CRISIS_ICONS.get(crisis, '⚠️')} {d.get('title')}", className="alert-heading"),
                html.P(f"🆔 Type: {crisis}", className="mb-1"),
                html.P(f"🕒 {d.get('timestamp')} | 📡 Source: {d.get('source')}", className="mb-2"),
                html.A("🔗 View Source", href=d.get("url", "#"), target="_blank")
            ], color=CRISIS_COLORS.get(crisis, "secondary"), className="mb-3")
        )

    return cards if cards else [html.P("No crisis posts available yet.", className="text-muted")]

@app.callback(
    Output("search-results", "children"),
    Input("search-input", "value")
)
def run_search(q):
    if not q:
        return ""
    vec = embed_model.encode(q).tolist()
    raw_hits = qdrant.search(collection_name=QCOL, query_vector=vec, limit=20)
    seen_titles = set()
    unique_hits = []
    for hit in raw_hits:
        title = hit.payload.get("title", "").strip().lower()
        if title not in seen_titles and title:
            seen_titles.add(title)
            unique_hits.append(hit)
            if len(unique_hits) >= 5:
                break
    results = []
    for i, h in enumerate(unique_hits, 1):
        p = h.payload
        results.append(
            dbc.Card([
                dbc.CardBody([
                    html.H6(f"Result {i} – Score {h.score:.2f}"),
                    html.H5(p.get("title", "(no title)"), className="card-title"),
                    html.P(f"Type: {p.get('crisis_type', 'none')}", className="card-text"),
                    html.A("🔗 Source", href=p.get("url", "#"), target="_blank")
                ])
            ], className="mb-3 bg-secondary text-white")
        )
    return results

@app.callback(
    Output("time-series", "figure"),  # Changed from extendData to figure for full refresh
    Input("time-series-interval", "n_intervals")
)
def update_time_series(n):
    # Completely redraw the chart instead of extending it
    return draw_initial_time_series()

@app.callback(
    Output("crisis-bar-chart", "figure"),
    Input("analytics-interval", "n_intervals")
)
def update_crisis_bar_chart(n):
    return draw_crisis_distribution_bar()

@app.callback(
    Output("crisis-heatmap", "figure"),
    Input("heatmap-interval", "n_intervals")
)
def update_crisis_heatmap(n):
    return draw_crisis_heatmap()

if __name__ == "__main__":
    app.run(debug=True, port=4567)