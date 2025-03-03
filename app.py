from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go
import duckdb, numpy


app = Dash(__name__)

df = duckdb.read_parquet("data.parquet")
con = duckdb.connect(":default:")
labels_df = (
    df.select("source as node")
    .distinct()
    .union(df.select("target as node").distinct())
    .distinct()
    .query(
        "labels",
        """
        with 
            start as (
                select 
                    *, 
                    row_number() over (order by node) as index 
                from labels
            ) 
        select node, index - 1 as index from start""",
    )
)
labels_df.create_view("v_label", replace=True)
items_df = df.query(
    "base",
    """
        select  
            source_label.index as source,
            source_label.node as source_node,
            target_label.index as target,
            target_label.node as target_node,
            sum(b.value) as value
        from base b
        left join v_label source_label on b.source = source_label.node
        left join v_label target_label on b.target = target_label.node
        group by 1,2,3,4
    """,
)
items_df.to_table("base_items")

labels = labels_df.select("node").fetchnumpy()["node"].tolist()

app.layout = html.Div(
    [
        html.H4("Interactive Sankey Design"),
        dcc.Graph(id="graph"),
        html.P("Depth"),
        dcc.Slider(id="slider", min=2, max=20, value=2, step=1),
        html.P("Starting Node"),
        dcc.Dropdown(labels, "node_16", id="sankey-starter-dropdown", multi=False),
        html.P("Included Items"),
        dcc.Dropdown(labels, labels, id="sankey-included-dropdown", multi=True),
        html.Div(id="dd-output-container"),
    ]
)


@app.callback(
    Output("graph", "figure"),
    Input("slider", "value"),
    Input("sankey-starter-dropdown", "value"),
    Input("sankey-included-dropdown", "value"),
)
def display_sankey(depth, start_node, included_nodes):
    # creating Numpy arrays for easy reuse in duckdb
    included_nodes_np = numpy.array(included_nodes)

    # base query for getting the gaphical view
    items_df = con.query(
        """
        with base_join as (
            select *
            from base_items b
            where 
                (b.source_node in (select column0 from included_nodes_np)
                    and
                b.target_node in (select column0 from included_nodes_np))
        )
         select * from base_join
         """,
    )
    items_df.create_view("v_graph", replace=True)
    depth_graph = con.query(
        """
            WITH RECURSIVE paths(start_node, next_node, depth, path) AS (
                    SELECT -- Define the path as the first edge of the traversal
                        source_node AS start_node,
                        target_node AS next_node,
                        1 as depth,
                        [source_node, target_node] AS path
                    FROM v_graph
                    WHERE source_node = $start
                    UNION ALL
                    SELECT -- Concatenate new edge to the path
                        source_node AS start_node,
                        target_node AS next_node,
                        paths.depth + 1 as depth,
                        array_append(path, target_node) AS path
                    FROM paths
                    JOIN v_graph ON paths.next_node = source_node
                    -- Prevent adding a repeated node to the path.
                    -- This ensures that no cycles occur.
                    WHERE list_position(paths.path, target_node) IS NULL
                    and (depth + 1) <= $depth
                )
            SELECT start_node as source_node, next_node as target_node, depth, path
            FROM paths
            ;
        """,
        params={"start": start_node, "depth": depth},
    ).set_alias('r1')
    items = items_df.set_alias('r0').join(depth_graph,condition='r1.source_node = r0.source_node and r1.target_node = r0.target_node', how='inner').select("source", "target", "value").fetchnumpy()
    node = {"label": labels}
    link = items

    fig = go.Figure(go.Sankey(link=link, node=node))
    fig.update_layout(font_size=10)
    return fig


app.run_server(debug=True)
