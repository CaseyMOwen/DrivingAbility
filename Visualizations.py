import pandas as pd
import folium
import json
import csv

def create_Choropleth(data_df):
    cp_df = create_choropleth_data(data_df)
    # print(id_list)
    state_geo = get_state_geo()
    m = folium.Map(location=[40, -96.5], zoom_start=4, min_zoom=3)
    cp = folium.Choropleth(
    geo_data=state_geo,
    name="choropleth",
    data=cp_df,
    columns=["State", "Average Score"],
    key_on="feature.properties.name",
    fill_color="YlGn",
    fill_opacity=0.7,
    line_opacity=0.2,
    legend_name="Average Score",
    ).add_to(m)

    # m.save("state_choropleth.html")
    add_choropleth_tooltip(cp_df, cp, m)
    return m

def create_choropleth_data(data_df):
    states_dict = create_states_dict()
    inv_states_dict = {v: k for k, v in states_dict.items()}
    # df = pd.read_parquet('scored_post_data.parquet')
    data_df['State'] = data_df['subreddit'].map(inv_states_dict)
    grouped = data_df.groupby("State")
    cp_df = grouped['tot_score'].mean().reset_index(name="Average Score")
    # print(df.groupby("State").count().reset_index())
    # print(df)
    # print(df.value_counts("State"))
    num_posts = data_df.value_counts("State").reset_index(name='Number of Posts')
    cp_df = cp_df.merge(num_posts, on='State')
    return cp_df

def add_choropleth_tooltip(cp_df, cp, m):
    cp_df_state_indexed = cp_df.set_index('State')
    for state in cp.geojson.data['features']:
        state_name = state['properties']['name']
        if state_name in cp_df_state_indexed.index:
            state['properties']['average score'] = float(cp_df_state_indexed.loc[state_name, 'Average Score'])
            state['properties']['number of posts'] = float(cp_df_state_indexed.loc[state_name, 'Number of Posts'])
        else:
            state['properties']['average score'] = 0
            state['properties']['number of posts'] = 0
    folium.GeoJsonTooltip(['name', 'number of posts', 'average score']).add_to(cp.geojson)
    folium.LayerControl().add_to(m)

def get_state_geo():
    state_geo_f = open('us_states_geo.json')
    state_geo = json.load(state_geo_f)
    return state_geo

def create_states_dict():
    with open('StateSubreddits.csv') as f:
        next(f)  # Skip the header
        reader = csv.reader(f, skipinitialspace=True)
        states_dict = dict(reader)
    return states_dict

# m = create_Choropleth()
# m.save("state_choropleth.html")
