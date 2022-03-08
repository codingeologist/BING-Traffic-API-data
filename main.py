import time
import requests
import pandas as pd
import geopandas as gpd

query = 'http://dev.virtualearth.net/REST/v1/Traffic/Incidents/LATITUDE-LOWER-LEFT,LONGITUDE-LOWER-LEFT,LATITUDE-UPPER-RIGHT,LONGITUDE-UPPER-RIGHT?key=API_KEY'

API_URL = query

start_time = time.time()

r = requests.get(url=API_URL)

results = r.json()
data = results['resourceSets']
#print(json.dumps(data, indent=4, sort_keys=True))

end_call = time.time()
print('API data fetch in: {}s'.format(round(end_call-start_time, 2)))

normalised_results = pd.json_normalize(data=data, record_path=['resources'])
data = pd.DataFrame()
data['incident_id'] = normalised_results['incidentId']
data['title'] = normalised_results['title']
data['type'] = normalised_results['type']
data['road_closed'] = normalised_results['roadClosed']
data['severity'] = normalised_results['severity']
data['start_time'] = normalised_results['start']
data['description'] = normalised_results['description']
data['verified'] = normalised_results['verified']

data[['latitude', 'longitude']] = pd.DataFrame(
    normalised_results['point.coordinates'].to_list(), index=data.index)

data[['to_latitude', 'to_longitude']] = pd.DataFrame(
    normalised_results['toPoint.coordinates'].to_list(), index=data.index)

data['start_time'] = data['start_time'].str.extract('(\d+)')
data['start_time'] = pd.to_datetime(data['start_time'], unit='ms')

data.loc[data['severity'] == 1, 'severity'] = 'low_impact'
data.loc[data['severity'] == 2, 'severity'] = 'minor'
data.loc[data['severity'] == 3, 'severity'] = 'moderate'
data.loc[data['severity'] == 4, 'severity'] = 'serious'

data.loc[data['type'] == 1, 'type'] = 'accident'
data.loc[data['type'] == 2, 'type'] = 'congestion'
data.loc[data['type'] == 3, 'type'] = 'disabled_vehicle'
data.loc[data['type'] == 4, 'type'] = 'mass_transit'
data.loc[data['type'] == 5, 'type'] = 'miscellaneous'
data.loc[data['type'] == 6, 'type'] = 'other_news'
data.loc[data['type'] == 7, 'type'] = 'planned_event'
data.loc[data['type'] == 8, 'type'] = 'road_hazard'
data.loc[data['type'] == 9, 'type'] = 'construction'
data.loc[data['type'] == 10, 'type'] = 'alert'
data.loc[data['type'] == 11, 'type'] = 'weather'

end_prcs = time.time()
print('Data processed in: {}s'.format(round(end_prcs-start_time, 2)))

geo_data = gpd.GeoDataFrame(
    data, geometry=gpd.points_from_xy(data.longitude, data.latitude))
geo_data = geo_data.set_crs(4326)
print(geo_data.crs)

rule_britannia = gpd.read_file('british_isles.geojson', driver='GeoJSON')

geo_data = geo_data.clip(rule_britannia)

end_geo_prcs = time.time()
print('Data processed in: {}s'.format(round(end_geo_prcs-start_time, 2)))

geo_data.to_file('traffic_incidents_2.gpkg',
                 layer='british_isles', driver='GPKG')

geo_data.drop('geometry', axis=1).to_csv(
    'traffic_incidents_2.csv', index=False)

end_sav = time.time()
print('Data saved in: {}s'.format(round(end_sav-start_time, 2)))
print('Process Completed!')
