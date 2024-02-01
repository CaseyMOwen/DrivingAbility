import streamlit as st
import pandas as pd
import numpy as np
from st_files_connection import FilesConnection
import Visualizations
import streamlit_folium as sf
import requests
import io
import datetime

# Create connection object and retrieve file contents.
# Specify input format is a csv and to cache the result for 600 seconds.
# conn = st.connection('s3', type=FilesConnection)
# df = conn.read("caseyowendrivingdata/post_data.parquet", input_format="parquet", ttl=600)
# conn

@st.cache_data(ttl=datetime.timedelta(hours=1))
def read_csv(filename):
    url = st.secrets["webdav_server"] + '/remote.php/dav/files/' + st.secrets["webdav_username"] + '/Repos/DrivingAbility/data/' + filename
    auth = (st.secrets["webdav_username"], st.secrets["webdav_password"])
    try:
        r = requests.get(url=url, auth=auth)
        r.raise_for_status()
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)
    data = r.content.decode('utf-8')
    df = pd.read_csv(io.StringIO(data))
    return df

data_post_df = read_csv('post_data.csv')
data_comment_df = read_csv('comment_data.csv')
scored_post_df = read_csv('scored_post_data.csv')
scored_comment_df = read_csv('scored_comment_data.csv')

# data_post_df = pd.read_parquet('post_data.parquet')
# data_comment_df = pd.read_parquet('comments_data.parquet')
# scored_post_df = pd.read_parquet('scored_post_data.parquet')
# scored_comment_df = pd.read_parquet('scored_comment_data.parquet')

post_df = pd.merge(data_post_df, scored_post_df, on='id', how='inner')
comment_df = pd.merge(data_comment_df, scored_comment_df, on='id', how='inner')
# Print results.
# print(df.iloc[:1])
# st.write(df.iloc[:1])
thresh = .675
post_df["Classification"] = post_df['tot_score'] > thresh
comment_df["Classification"] = comment_df['tot_score'] > thresh
st.write(post_df)
st.write(comment_df)

# id_list = df.groupby("subreddit").count()['id']
# print(id_list)


st.title('Reddit Driving Confidence vs. Ability')

st.title("Post data")
m1 = Visualizations.create_Choropleth(post_df)
st_data1 = sf.folium_static(m1)

st.title("Comment data")
m2 = Visualizations.create_Choropleth(comment_df)
st_data2 = sf.folium_static(m2)



# DATE_COLUMN = 'date/time'
# DATA_URL = ('https://s3-us-west-2.amazonaws.com/'
#          'streamlit-demo-data/uber-raw-data-sep14.csv.gz')

# @st.cache_data
# def load_data(nrows):
#     data = pd.read_csv(DATA_URL, nrows=nrows)
#     lowercase = lambda x: str(x).lower()
#     data.rename(lowercase, axis='columns', inplace=True)
#     data[DATE_COLUMN] = pd.to_datetime(data[DATE_COLUMN])
#     return data

# # Create a text element and let the reader know the data is loading.
# data_load_state = st.text('Loading data...')
# # Load 10,000 rows of data into the dataframe.
# data = load_data(10000)
# # Notify the reader that the data was successfully loaded.
# data_load_state.text("Done! (using st.cache_data)")

# if st.checkbox('Show raw data'):
#     st.subheader('Raw data')
#     st.write(data)
# st.subheader('Number of pickups by hour')
# hist_values = np.histogram(
#     data[DATE_COLUMN].dt.hour, bins=24, range=(0,24))[0]
# st.bar_chart(hist_values)

# hour_to_filter = st.slider('hour', 0, 23, 17)  # min: 0h, max: 23h, default: 17h
# filtered_data = data[data[DATE_COLUMN].dt.hour == hour_to_filter]
# st.subheader(f'Map of all pickups at {hour_to_filter}:00')
# st.map(filtered_data)

