import dash
from dash import dcc, html, Input, Output
import dash_bootstrap_components as dbc

from pymongo import MongoClient
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

# ─── BACKEND SETUP ───────────────────────────────────────────────────────────────
mongo = MongoClient("mongodb://localhost:27017/")["crisiscast"]["unified_posts"]
qdrant = QdrantClient(host="127.0.0.1", port=6333)
embed_model = SentenceTransformer("all-MiniLM-L6-v2")
QCOL = "post_vectors"

# ─── DASH APP LAYOUT ────────────────────────────────────────────────────────────
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])
app.layout = dbc.Container([
    dbc.Row([
        # Live Feed column
        dbc.Col([
            html.H2("Live Feed"),
            # This div will get updated every 10 s
            html.Div(id="feed-container"),
            dcc.Interval(id="feed-interval", interval=10*1000, n_intervals=0)
        ], width=6),

        # Semantic Search column
        dbc.Col([
            html.H2("Semantic Search"),
            dcc.Input(id="search-input", placeholder="type query…", type="text", style={"width":"100%"}),
            html.Div(id="search-results", className="mt-3")
        ], width=6),
    ], className="mt-4")
], fluid=True, className="p-4")

# ─── CALLBACK: Live Feed ─────────────────────────────────────────────────────────
@app.callback(
    Output("feed-container", "children"),
    Input("feed-interval", "n_intervals")
)
def update_feed(n):
    # 1) Fetch newest posts sorted by your unified timestamp
    print(f"↻ update_feed called (n_intervals={n})")  # debug line
    raw = list(
        mongo.find()
             .sort("timestamp", -1)
             .limit(20)   # grab a few extra so dedupe can trim to 10
    )
    if raw:
        print("↻  fetched", len(raw), "docs; newest timestamp:", raw[0].get("timestamp"))
    else:
        print("↻  fetched 0 docs")
    # 2) Deduplicate by *title* (normalized), keep first 10 unique titles
    seen_titles = set()
    unique = []
    for doc in raw:
        title = doc.get("title", "").strip().lower()
        if title and title not in seen_titles:
            seen_titles.add(title)
            unique.append(doc)
        if len(unique) >= 10:
            break

    # 3) Build cards
    cards = []
    for d in unique:
        cards.append(
            dbc.Card([
                dbc.CardBody([
                    html.H5(d.get("title","(no title)"), className="card-title"),
                    html.P(
                        f"Time: {d.get('timestamp','')}  |  "
                        f"Type: {d.get('crisis_type','none')}  |  "
                        f"Source: {d.get('source','')}",
                        className="card-text"
                    ),
                    html.A("🔗 Source Link", href=d.get("url","#"), target="_blank")
                ])
            ], className="mb-3 bg-secondary text-white")
        )
    # If no posts, show a placeholder
    if not cards:
        cards = [html.P("No posts available yet.", className="text-muted")]

    return cards

# ─── CALLBACK: Semantic Search ──────────────────────────────────────────────────
@app.callback(
    Output("search-results", "children"),
    Input("search-input", "value")
)
def run_search(q):
    if not q:
        return ""

    # 1) Encode the query to a vector
    vec = embed_model.encode(q).tolist()

    # 2) Fetch top N raw hits from Qdrant
    raw_hits = qdrant.search(collection_name=QCOL, query_vector=vec, limit=20)

    # 3) Deduplicate by *title* (normalized), keep first 5 unique titles
    seen_titles = set()
    unique_hits = []
    for hit in raw_hits:
        title = hit.payload.get("title", "").strip().lower()
        if not title or title in seen_titles:
            continue
        seen_titles.add(title)
        unique_hits.append(hit)
        if len(unique_hits) >= 5:
            break

    # 4) Build Dash cards from unique_hits
    results = []
    for idx, h in enumerate(unique_hits, start=1):
        p = h.payload
        title = p.get("title", "(no title)")
        url   = p.get("url", "#")
        ctype = p.get("crisis_type", "none")
        results.append(
            dbc.Card([
                dbc.CardBody([
                    html.H6(f"Result {idx} – Score {h.score:.3f}", className="card-subtitle"),
                    html.H5(title, className="card-title"),
                    html.P(f"Type: {ctype}", className="card-text"),
                    html.A("🔗 Link", href=url, target="_blank")
                ])
            ], className="mb-3 bg-secondary text-white")
        )
    return results 


# ─── RUN SERVER ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(debug=True, port=4567)