import pandas as pd
import folium
import json
import Querier

def create_Choropleth():
    cp_df = create_choropleth_data()
    # print(id_list)
    state_geo = get_state_geo()
    m = folium.Map(location=[40, -96.5], zoom_start=4, min_zoom=3)
    cp = folium.Choropleth(
    geo_data=state_geo,
    name="choropleth",
    data=cp_df,
    columns=["State", "Number of Posts"],
    key_on="feature.properties.name",
    fill_color="YlGn",
    fill_opacity=0.7,
    line_opacity=0.2,
    legend_name="Number of Posts Retrieved",
    ).add_to(m)

    # m.save("state_choropleth.html")
    add_choropleth_tooltip(cp_df, cp, m)
    return m

def create_choropleth_data():
    q = Querier.Querier()
    states_dict = q.create_states_dict()
    inv_states_dict = {v: k for k, v in states_dict.items()}
    df = pd.read_parquet('post_data.parquet')
    df['State'] = df['subreddit'].map(inv_states_dict)
    cp_df = df.groupby("State").count()['id'].reset_index(name="Number of Posts")
    return cp_df

def add_choropleth_tooltip(cp_df, cp, m):
    cp_df_state_indexed = cp_df.set_index('State')
    for state in cp.geojson.data['features']:
        state_name = state['properties']['name']
        state['properties']['number of posts'] = float(cp_df_state_indexed.loc[state_name, 'Number of Posts'])
    folium.GeoJsonTooltip(['name', 'number of posts']).add_to(cp.geojson)
    folium.LayerControl().add_to(m)

def get_state_geo():
    state_geo_f = open('us_states_geo.json')
    state_geo = json.load(state_geo_f)
    return state_geo

create_Choropleth()

