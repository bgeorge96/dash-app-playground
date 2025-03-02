from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go
import json, urllib
import duckdb, numpy


app = Dash(__name__)

df = duckdb.read_parquet("data.parquet")
labels_df = (
    df.select("source as node")
    .distinct()
    .union(df.select("target as node").distinct())
    .distinct()
    .query("labels", "select *, row_number() over (order by node) as index from labels")
)
labels = labels_df.select("node").fetchnumpy()["node"].tolist()
labels_df.create("v_label")


app.layout = html.Div(
    [
        html.H4("Supply chain of the energy production"),
        dcc.Graph(id="graph"),
        html.P("Depth"),
        dcc.Slider(id="slider", min=5, max=10, value=5, step=1),
        html.P("Starting Node"),
        dcc.Dropdown(labels, "node_16", id="sankey-starter-dropdown", multi=False),
        html.P("Included Items"),
        dcc.Dropdown(labels, labels, id="sankey-included-dropdown", multi=True),
        html.Div(id="dd-output-container"),
    ]
)


@app.callback(
    Output("dd-output-container", "children"),
    Input("sankey-included-dropdown", "value"),
)
def update_output(value):
    return f"You have selected {value}"


@app.callback(
    Output("graph", "figure"),
    Input("slider", "value"),
    Input("sankey-starter-dropdown", "value"),
    Input("sankey-included-dropdown", "value"),
)
def display_sankey(depth, start_node, included_nodes):
    print(start_node, included_nodes)
    included_nodes_np = numpy.array(included_nodes)
    items_df = df.query(
        "base",
        """
         select 
            source_label.index as source,
            source_label.node as source_node,
            target_label.index as target,
            target_label.node as target_node,
            b.value
         from base b
         left join v_label source_label on b.source = source_label.node
         left join v_label target_label on b.target = target_label.node
         where 
            b.source in (select * from included_nodes_np)
                and
            b.target in (select * from included_nodes_np) 
         """,
    )
    items_df.filter("source_node = 'node_17' or target_node = 'node_17' ").show()
    items = items_df.select("source", "target", "value").fetchnumpy()
    node = {"label": labels}
    link = items

    fig = go.Figure(go.Sankey(link=link, node=node))
    fig.update_layout(font_size=10)
    return fig


app.run_server(debug=True)
