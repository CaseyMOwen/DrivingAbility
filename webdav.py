from requests.auth import HTTPBasicAuth
import requests
import config
import io
import pandas as pd


# path_to_file = 'davs://caseyowen@nx48208.your-storageshare.de/remote.php/webdav/Repos/DrivingAbility/scored_post_data.parquet'
# auth = (config.webdav_username, config.webdav_password)
# # r = requests.request(
# #     method='get',
# #     url='https://mycloudinstance/index.php/apps/files/?dir=/Test&fileid=431',
# #     auth=('username', 'pass')
# # )
# r = requests.get(url=base_url, auth=auth)



def read_csv(filename):
    url = config.webdav_server + '/remote.php/dav/files/' + config.webdav_username + '/Repos/DrivingAbility/data/' + filename
    auth = (config.webdav_username, config.webdav_password)
    try:
        r = requests.get(url=url, auth=auth)
        r.raise_for_status()
    except requests.exceptions.HTTPError as err:
        raise SystemExit(err)
    data = r.content.decode('utf-8')
    df = pd.read_csv(io.StringIO(data))
    return df


# print(read_csv('scored_post_data.csv'))

# print(r.status_code)
# # print(r.text)
# print(df)

# pd.read_parquet('comments_data.parquet').to_csv('comment_data.csv')
# pd.read_parquet('post_data.parquet').to_csv('post_data.csv')
# pd.read_parquet('scored_comment_data.parquet').to_csv('scored_comment_data.csv')
# pd.read_parquet('scored_post_data.parquet').to_csv('scored_post_data.csv')