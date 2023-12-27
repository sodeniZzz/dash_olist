import json
from dash import Dash, dcc, html, Input, Output
import pandas as pd
import plotly.express as px
from urllib.request import urlopen


app = Dash(__name__)


customers = pd.read_csv("dataset_olist/olist_customers_dataset.csv")
location = pd.read_csv("dataset_olist/olist_geolocation_dataset.csv")
items = pd.read_csv("dataset_olist/olist_order_items_dataset.csv")
payments = pd.read_csv("dataset_olist/olist_order_payments_dataset.csv")
reviews = pd.read_csv("dataset_olist/olist_order_reviews_dataset.csv")
orders = pd.read_csv("dataset_olist/olist_orders_dataset.csv")
products = pd.read_csv("dataset_olist/olist_products_dataset.csv")
translation = pd.read_csv("dataset_olist/product_category_name_translation.csv")
sellers = pd.read_csv("dataset_olist/olist_sellers_dataset.csv")

with urlopen(
    "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
) as response:
    brazil = json.load(response)

for feature in brazil["features"]:
    feature["id"] = feature["properties"]["sigla"]


# добавим перевод
translation.loc[len(translation.index)] = [
    "portateis_cozinha_e_preparadores_de_alimentos",
    "portable kitchen and food preparers",
]
translation.loc[len(translation.index)] = ["pc_gamer", "PC Gamer"]

# в product категории будут сразу на английском
products = pd.merge(products, translation, on=["product_category_name"])
products = products.drop("product_category_name", axis=1)
products.rename(
    columns={"product_category_name_english": "product_category_name"}, inplace=True
)
products.insert(1, "product_category_name", products.pop("product_category_name"))

customers_ = customers[["customer_id", "customer_state"]]
sellers_ = sellers[["seller_id", "seller_state"]]
# делаем общую табличку заказы-товар-продавцы-покупатели
items_products = pd.merge(items, products, on="product_id", how="inner")
items_products_orders = pd.merge(items_products, orders, on="order_id", how="inner")
items_products_orders_customers = pd.merge(
    items_products_orders, customers_, on="customer_id", how="inner"
)
orders_products_sellers_customers = pd.merge(
    items_products_orders_customers, sellers_, on="seller_id", how="inner"
)
orders_products_sellers_customers[
    "months"
] = orders_products_sellers_customers.order_purchase_timestamp.apply(lambda x: x[0:7])

########### task4
products_sellers_customers = orders_products_sellers_customers[
    [
        "order_id",
        "product_id",
        "order_status",
        "product_category_name",
        "customer_state",
        "seller_state",
        "months",
    ]
]
products_sellers_customers = products_sellers_customers.drop_duplicates()

seller_states_status_categories = (
    products_sellers_customers.groupby(
        ["seller_state", "product_category_name", "order_status", "months"]
    )
    .agg({"product_id": "count"})
    .reset_index()
    .rename(columns={"product_id": "orders_amount"})
)

customer_states_status_categories = (
    products_sellers_customers.groupby(
        ["customer_state", "product_category_name", "order_status", "months"]
    )
    .agg({"product_id": "count"})
    .reset_index()
    .rename(columns={"product_id": "orders_amount"})
)


############ task5
states_sellers_customers = orders_products_sellers_customers[
    [
        "order_id",
        "seller_id",
        "customer_id",
        "order_status",
        "product_category_name",
        "customer_state",
        "seller_state",
        "months",
    ]
]
states_sellers_customers = states_sellers_customers.drop_duplicates()

states_sellers = (
    states_sellers_customers.groupby(["seller_state", "order_status", "months"])
    .agg({"seller_id": "count"})
    .reset_index()
    .rename(columns={"seller_id": "sellers_amount"})
)
states_customers = (
    states_sellers_customers.groupby(["customer_state", "order_status", "months"])
    .agg({"customer_id": "count"})
    .reset_index()
    .rename(columns={"customer_id": "customers_amount"})
)

all_states = pd.DataFrame({"state": [state["id"] for state in brazil["features"]]})


def get_dict_dates_to_str(df):
    date_dict = {}
    min_date = df["months"].min()
    for date_str in df["months"]:
        date_ = pd.to_datetime(date_str)
        minutes_epoch = int((date_ - pd.to_datetime(min_date)).total_seconds() / 60)
        date_dict[minutes_epoch] = date_str
    return date_dict


date_to_str = get_dict_dates_to_str(products_sellers_customers)


app.layout = html.Div(
    children=[
        html.Div(
            children=[
                html.H1(
                    "Sales category distribution by seller state",
                    style={"text-align": "center"},
                ),
                dcc.Markdown("Select state:"),
                dcc.Checklist(
                    options=[{"label": "All Brazil", "value": "All Brazil"}],
                    value=[],
                    id="all_states_checklist1",
                ),
                dcc.Dropdown(
                    options=seller_states_status_categories["seller_state"].unique(),
                    value=seller_states_status_categories["seller_state"].unique()[3],
                    id="state_1_dropdown",
                ),
                dcc.Markdown("Select order status:"),
                dcc.Checklist(
                    options=seller_states_status_categories["order_status"].unique(),
                    value=seller_states_status_categories["order_status"].unique(),
                    id="status_1_checklist",
                    labelStyle={"display": "block"},
                ),
                dcc.Markdown("Select date:"),
                dcc.RangeSlider(
                    id="date_slider1",
                    min=min(date_to_str.keys()),
                    max=max(date_to_str.keys()),
                    step=None,
                    value=[min(date_to_str.keys()), max(date_to_str.keys())],
                    marks=date_to_str,
                ),
                dcc.Graph(id="sellers_sales_pie_chart", style={"height": "700px"}),
            ]
        ),
        html.Div(
            children=[
                html.H1(
                    "Purchases category distribution by customer state",
                    style={"text-align": "center"},
                ),
                dcc.Markdown("Select state:"),
                dcc.Checklist(
                    options=[{"label": "All Brazil", "value": "All Brazil"}],
                    value=[],
                    id="all_states_checklist2",
                ),
                dcc.Dropdown(
                    options=customer_states_status_categories[
                        "customer_state"
                    ].unique(),
                    value=customer_states_status_categories["customer_state"].unique()[
                        0
                    ],
                    id="state_2_dropdown",
                ),
                dcc.Markdown("Select order status:"),
                dcc.Checklist(
                    options=customer_states_status_categories["order_status"].unique(),
                    value=customer_states_status_categories["order_status"].unique(),
                    id="status_2_checklist",
                    labelStyle={"display": "block"},
                ),
                dcc.Markdown("Select date:"),
                dcc.RangeSlider(
                    id="date_slider2",
                    min=min(date_to_str.keys()),
                    max=max(date_to_str.keys()),
                    step=None,
                    value=[min(date_to_str.keys()), max(date_to_str.keys())],
                    marks=date_to_str,
                ),
                dcc.Graph(id="customers_sales_pie_chart", style={"height": "700px"}),
            ]
        ),
        html.Div(
            [
                html.H1(
                    "Sellers and Customers amount by state",
                    style={"text-align": "center"},
                ),
                dcc.Dropdown(
                    id="data_selector",
                    options=[
                        {"label": "Sellers", "value": "sellers"},
                        {"label": "Customers", "value": "customers"},
                    ],
                    value="customers",
                ),
                dcc.Markdown("Select order status:"),
                dcc.Checklist(
                    options=states_sellers_customers["order_status"].unique(),
                    value=states_sellers_customers["order_status"].unique(),
                    id="status_3_checklist",
                    labelStyle={"display": "block"},
                ),
                dcc.Markdown("Select date:"),
                dcc.RangeSlider(
                    id="date_slider3",
                    min=min(date_to_str.keys()),
                    max=max(date_to_str.keys()),
                    step=None,
                    value=[min(date_to_str.keys()), max(date_to_str.keys())],
                    marks=date_to_str,
                ),
                html.Button("Back to all Brazil", id="reset_map_button"),
                dcc.Graph(id="brazil_map", style={"height": "700px"}),
                html.Div(
                    [
                        dcc.Graph(
                            id="sellers_sales_pie_chart_map",
                            style={"height": "700px", "display": "none"},
                        ),
                        dcc.Graph(
                            id="customers_sales_pie_chart_map",
                            style={"height": "700px", "display": "none"},
                        ),
                    ]
                ),
            ]
        ),
    ]
)


@app.callback(
    Output("sellers_sales_pie_chart", "figure"),
    [
        Input("all_states_checklist1", "value"),
        Input("state_1_dropdown", "value"),
        Input("status_1_checklist", "value"),
        Input("date_slider1", "value"),
    ],
)
def update_seller_pie_chart(all_states, selected_state, selected_statuses, date_range):
    start_date, end_date = date_range
    start_date = date_to_str[start_date]
    end_date = date_to_str[end_date]

    date_filtered_df = seller_states_status_categories[
        (seller_states_status_categories["months"] >= start_date)
        & (seller_states_status_categories["months"] <= end_date)
    ]

    if all_states:
        res_df = date_filtered_df[
            date_filtered_df["order_status"].isin(selected_statuses)
        ]
    else:
        res_df = date_filtered_df[
            (date_filtered_df["seller_state"] == selected_state)
            & (date_filtered_df["order_status"].isin(selected_statuses))
        ]

    fig = px.pie(
        data_frame=res_df,
        names="product_category_name",
        values="orders_amount",
    )

    return fig


@app.callback(
    Output("customers_sales_pie_chart", "figure"),
    [
        Input("all_states_checklist2", "value"),
        Input("state_2_dropdown", "value"),
        Input("status_2_checklist", "value"),
        Input("date_slider2", "value"),
    ],
)
def update_customer_pie_chart(
    all_states, selected_state, selected_statuses, date_range
):
    start_date, end_date = date_range
    start_date = date_to_str[start_date]
    end_date = date_to_str[end_date]

    date_filtered_df = customer_states_status_categories[
        (customer_states_status_categories["months"] >= start_date)
        & (customer_states_status_categories["months"] <= end_date)
    ]

    if all_states:
        res_df = date_filtered_df[
            date_filtered_df["order_status"].isin(selected_statuses)
        ]
    else:
        res_df = date_filtered_df[
            (date_filtered_df["customer_state"] == selected_state)
            & (date_filtered_df["order_status"].isin(selected_statuses))
        ]

    fig = px.pie(
        data_frame=res_df,
        names="product_category_name",
        values="orders_amount",
    )

    return fig


@app.callback(
    Output("brazil_map", "figure"),
    [
        Input("data_selector", "value"),
        Input("status_3_checklist", "value"),
        Input("date_slider3", "value"),
        Input("brazil_map", "clickData"),
        Input("reset_map_button", "n_clicks"),
    ],
)
def update_map(
    selected_data, selected_statuses, date_range, clickData, reset_map_clicks
):
    start_date, end_date = date_range
    start_date = date_to_str[start_date]
    end_date = date_to_str[end_date]

    if reset_map_clicks:
        clickData = None

    if selected_data == "sellers":
        date_filtered_df = states_sellers[
            (states_sellers["months"] >= start_date)
            & (states_sellers["months"] <= end_date)
        ]
        state_date_filtered_df = date_filtered_df[
            date_filtered_df["order_status"].isin(selected_statuses)
        ]
        state_date_filtered_df = (
            state_date_filtered_df.groupby("seller_state")
            .agg({"sellers_amount": "sum"})
            .reset_index()
        )

        res_df = pd.merge(
            all_states,
            state_date_filtered_df,
            how="outer",
            left_on="state",
            right_on="seller_state",
        )
        res_df["sellers_amount"].fillna(0, inplace=True)
    else:
        date_filtered_df = states_customers[
            (states_customers["months"] >= start_date)
            & (states_customers["months"] <= end_date)
        ]

        state_date_filtered_df = date_filtered_df[
            date_filtered_df["order_status"].isin(selected_statuses)
        ]
        state_date_filtered_df = (
            state_date_filtered_df.groupby("customer_state")
            .agg({"customers_amount": "sum"})
            .reset_index()
        )

        res_df = pd.merge(
            all_states,
            state_date_filtered_df,
            how="outer",
            left_on="state",
            right_on="customer_state",
        )
        res_df["customers_amount"].fillna(0, inplace=True)

    if clickData:
        selected_state = clickData["points"][0]["location"]
        res_df = res_df[res_df["state"] == selected_state]

    fig = px.choropleth(
        res_df,
        geojson=brazil,
        locations="state",
        color="sellers_amount" if selected_data == "sellers" else "customers_amount",
        color_continuous_scale="Viridis",
        range_color=(0, 10000),
        hover_data=[
            "state",
            "sellers_amount" if selected_data == "sellers" else "customers_amount",
        ],
    )

    fig.update_geos(fitbounds="locations", visible=False)

    return fig


@app.callback(
    Output("reset_map_button", "n_clicks"),
    Input("brazil_map", "clickData"),
    prevent_initial_call=True,
)
def reset_map_click(clickData):
    return None


@app.callback(
    Output("sellers_sales_pie_chart_map", "figure"),
    [
        Input("status_3_checklist", "value"),
        Input("date_slider3", "value"),
        Input("brazil_map", "clickData"),
        Input("reset_map_button", "n_clicks"),
    ],
)
def display_click_data_sellers(
    selected_statuses, date_range, clickData, reset_map_clicks
):
    start_date, end_date = date_range
    start_date = date_to_str[start_date]
    end_date = date_to_str[end_date]

    date_filtered_df = seller_states_status_categories[
        (seller_states_status_categories["months"] >= start_date)
        & (seller_states_status_categories["months"] <= end_date)
    ]

    if reset_map_clicks:
        clickData = None

    if clickData is not None:
        last_clicked_state = clickData["points"][0]["location"]
    else:
        last_clicked_state = None

    if last_clicked_state is None:
        res_df = date_filtered_df[
            date_filtered_df["order_status"].isin(selected_statuses)
        ]
    else:
        res_df = date_filtered_df[
            (date_filtered_df["seller_state"] == last_clicked_state)
            & (date_filtered_df["order_status"].isin(selected_statuses))
        ]

    fig = px.pie(
        data_frame=res_df,
        names="product_category_name",
        values="orders_amount",
        title=f'Sales category distribution in {last_clicked_state if last_clicked_state is not None else "Brazil"}',
    )

    return fig


@app.callback(
    Output("customers_sales_pie_chart_map", "figure"),
    [
        Input("status_3_checklist", "value"),
        Input("date_slider3", "value"),
        Input("brazil_map", "clickData"),
        Input("reset_map_button", "n_clicks"),
    ],
)
def display_click_data_customers(
    selected_statuses, date_range, clickData, reset_map_clicks
):
    start_date, end_date = date_range
    start_date = date_to_str[start_date]
    end_date = date_to_str[end_date]

    date_filtered_df = customer_states_status_categories[
        (customer_states_status_categories["months"] >= start_date)
        & (customer_states_status_categories["months"] <= end_date)
    ]

    if reset_map_clicks:
        clickData = None

    if clickData is not None:
        last_clicked_state = clickData["points"][0]["location"]
    else:
        last_clicked_state = None

    if last_clicked_state is None:
        res_df = date_filtered_df[
            date_filtered_df["order_status"].isin(selected_statuses)
        ]
    else:
        res_df = date_filtered_df[
            (date_filtered_df["customer_state"] == last_clicked_state)
            & (date_filtered_df["order_status"].isin(selected_statuses))
        ]

    fig = px.pie(
        data_frame=res_df,
        names="product_category_name",
        values="orders_amount",
        title=f'Purchases category distribution in {last_clicked_state if last_clicked_state is not None else "Brazil"}',
    )

    return fig


@app.callback(
    [
        Output("sellers_sales_pie_chart_map", "style"),
        Output("customers_sales_pie_chart_map", "style"),
    ],
    Input("data_selector", "value"),
)
def update_pie_chart_visibility(data_selected):
    if data_selected == "sellers":
        return {"display": "block"}, {"display": "none"}
    else:
        return {"display": "none"}, {"display": "block"}


if __name__ == "__main__":
    app.run_server(debug=True)
