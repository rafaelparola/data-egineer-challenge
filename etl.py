import pandas as pd
import sqlalchemy
import os
import logging
from datetime import datetime
from geopy.distance import geodesic as GD

# Get the execution datetime for log and for not duplicating rows in case of executing the ETL more then once
etl_start_datetime = datetime.now()

# Initializes the log information
logging.basicConfig(filename="log.txt", level=logging.DEBUG)
logging.info('Initializing execution at: '+ str(etl_start_datetime))


# Prepare the db engine to be read by Pandas
dbEngine=sqlalchemy.create_engine('sqlite:///trips_dw.db') # May differ if you are not using windows.
logging.info('Connected to DB.')

# Populates the CSV file into a Pandas DataFrame using chunks, to scale at large volumes of data without going out of memory.
chunksize = 10 ** 6
with pd.read_csv('trips.csv', header=0, chunksize=chunksize) as chunk:
    for df in chunk:
        logging.info('CSV file loaded.')

        logging.info('Working on the dimension tables transformations...')

        # Changes the datetime field to datetime type and round the hours.
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['datetime'] = df['datetime'].dt.round('D')

        # Get the weekly average per region and stores in a DB_table
        df_weekly_avg = df.groupby('region').apply(lambda x: x.resample('7D', on='datetime').count().div(7))
        df_weekly_avg.drop(['origin_coord', 'destination_coord', 'datasource'], axis=1, inplace=True)
        df_weekly_avg.rename(columns={'region': 'weekly_avg'}, inplace=True)
        df_weekly_avg.reset_index(inplace=True)

        df_weekly_avg['execution_datetime'] = etl_start_datetime
        df_weekly_avg.to_sql('WEEK_AVERGAGE_BY_REGION', dbEngine, if_exists='append')

        # Remove the all the characteres not related to the lat and lon for the origin and destine('POINT ()').
        df['origin_coord']=df['origin_coord'].apply(lambda st: st[st.find("(")+1:st.find(")")])
        df['destination_coord']=df['destination_coord'].apply(lambda st: st[st.find("(")+1:st.find(")")])

        # Split the lat and lon for origin and destine in 4 separate fields.
        df[['origin_coord_lat','origin_coord_lon']] = df['origin_coord'].str.split(' ',expand=True)
        df[['dst_coord_lat','dst_coord_lon']] = df['destination_coord'].str.split(' ',expand=True)

        # Drop the original origin and destination coordinates columns.
        df.drop('origin_coord',axis=1, inplace=True)
        df.drop('destination_coord',axis=1, inplace=True)

        # Set all coordinates as type float.
        df['origin_coord_lon'] = df['origin_coord_lon'].astype(float)
        df['origin_coord_lat'] = df['origin_coord_lat'].astype(float)
        df['dst_coord_lon'] = df['dst_coord_lon'].astype(float)
        df['dst_coord_lat'] = df['dst_coord_lat'].astype(float)


        # Group similar origin, destionation and datetime
        df['count_grouped'] = 0

        # Returns the distance in Kilometers from point 1 to point 2
        def check_distancies(lat, lon, lat2, lon2):
            point1 = (lat, lon)
            point2 = (lat2, lon2)
            return GD(point1, point2).km

        # Iterates the DataFrame to find near 10Km Origin Coordinates, near 10Km Destine Coordinates 
        # and equal dates based on the previous datetime transformation
        for i, r in df.iterrows():
            for i2, r2, in df.iterrows():
                ori_lat = r['origin_coord_lat']
                ori_lon = r['origin_coord_lon']
                ori_lat2 = r2['origin_coord_lat']
                ori_lon2 = r2['origin_coord_lon']

                dst_lat = r['dst_coord_lat']
                dst_lon = r['dst_coord_lon']
                dst_lat2 = r2['dst_coord_lat']
                dst_lon2 = r2['dst_coord_lon']
                if i != i2 and (check_distancies(ori_lat ,ori_lon, ori_lat2, ori_lon2) < 10) and i < i2:
                    if check_distancies(dst_lat, dst_lon, dst_lat2, dst_lon2) < 10:
                        if r['datetime'] == r2['datetime']:
                            df.drop(i2, inplace=True)
                            df.loc[i, 'count_grouped'] = df.loc[i, 'count_grouped'] + 1



        # Creates four other dataframes for region, coordinates, dates and datasources in order to model a snoflake schema.
        df_region = df[['region']].drop_duplicates()
        df_coord = df[['origin_coord_lat', 'origin_coord_lon', 'dst_coord_lat', 'dst_coord_lon', 'region']].drop_duplicates()
        df_dates = df[['datetime']].drop_duplicates()
        df_datasources = df[['datasource']].drop_duplicates()

        # Set index name as id and increseas all of it by 1, so it will be populated as an identificator in the database. 
        df_region.index.names = ['id']
        df_region.index += 1
        df_coord.index.names = ['id']
        df_coord.index += 1
        df_dates.index.names = ['id']
        df_dates.index += 1
        df_datasources.index.names = ['id']
        df_datasources.index += 1

        # Merges Coordinates DataFrame with Regions Dataframe in order to stablish an relation between then in the database.
        df_coord = df_coord.merge(df_region.reset_index(), how='inner', left_on='region', right_on='region')
        # Renames the id column from the Regions DataFrame to d_region_id in order to be created in the database.
        df_coord = df_coord.rename(columns={'id': 'd_region_id'})
        # Drop column so it will not bring duplicate columns into the database.
        df_coord = df_coord.drop(['region'], axis=1)
        # Drop column so it will not bring duplicate columns into the fact table in the database.
        df = df.drop(['region'], axis=1)

        # Persists all dimension tables into the database.
        df_region['execution_datetime'] = etl_start_datetime
        df_region.to_sql('D_REGION', dbEngine, if_exists='append', index=False)
        logging.info('D_REGION table loaded.')

        df_coord['execution_datetime'] = etl_start_datetime
        df_coord.to_sql('D_COORD', dbEngine, if_exists='append', index=False)
        logging.info('D_COORD table loaded.')

        df_dates['execution_datetime'] = etl_start_datetime
        df_dates.to_sql('D_DATES', dbEngine, if_exists='append', index=False)
        logging.info('D_DATES table loaded.')

        df_datasources['execution_datetime'] = etl_start_datetime
        df_datasources.to_sql('D_DATASOURCE', dbEngine, if_exists='append', index=False)
        logging.info('D_DATASOURCE table loaded.')

        # Gets all data from the db dimension tables
        df_region = pd.read_sql_table('D_REGION', dbEngine)
        df_region = df_region.loc[df_region['execution_datetime'] == str(etl_start_datetime)]

        df_coord = pd.read_sql_table('D_COORD', dbEngine)
        df_coord = df_coord.loc[df_coord['execution_datetime'] == str(etl_start_datetime)]

        df_dates = pd.read_sql_table('D_DATES', dbEngine)
        df_dates = df_dates.loc[df_dates['execution_datetime'] == str(etl_start_datetime)]
        df_dates['datetime'] = pd.to_datetime(df_dates['datetime'])

        df_datasources = pd.read_sql_table('D_DATASOURCE', dbEngine)
        df_datasources = df_datasources.loc[df_datasources['execution_datetime'] == str(etl_start_datetime)]

        

        logging.info('Working at the fact table transformations...')

        

        # Merge the trips fact table with the coordinates dimension table.
        df = df.merge(df_coord.reset_index(), how='inner', left_on=['origin_coord_lat', 'origin_coord_lon', 'dst_coord_lat', 'dst_coord_lon'], right_on=['origin_coord_lat', 'origin_coord_lon', 'dst_coord_lat', 'dst_coord_lon'])
        # Set the dimension id column to the wanted name in the fact table.
        df = df.rename(columns={'id': 'd_coord_id'})
        # Remove unwanted columns in the fact table.
        df = df.drop(['origin_coord_lat', 'origin_coord_lon', 'dst_coord_lat', 'dst_coord_lon', 'd_region_id'], axis=1)

        # Merge the trips fact table with the dates dimension table.
        df = df.merge(df_dates.reset_index(), how='inner', left_on='datetime', right_on='datetime')
        # Set the dimension id column to the wanted name in the fact table.
        df = df.rename(columns={'id': 'd_datetime_id'})
        # Remove unwanted columns in the fact table.
        df = df.drop(['datetime'], axis=1)

        # Merge the trips fact table with the datasources dimension table.
        df = df.merge(df_datasources.reset_index(), how='inner', left_on='datasource', right_on='datasource')
        # Set the dimension id column to the wanted name in the fact table.
        df = df.rename(columns={'id': 'd_datasource_id'})
        
        
        # Remove unwanted columns in the fact table.
        df = df.drop(['datasource'], axis=1)
        df = df.drop(['index', 'index_x', 'index_y', 'execution_datetime_x', 'execution_datetime_y'], axis=1)

        # Persists the fact table into the database.


        df['execution_datetime'] = etl_start_datetime
        df.to_sql('F_TRIPS', dbEngine, if_exists='append', index=False)

        logging.info('F_TRIPS fact table loaded.')

        etl_end_datetime = datetime.now()
        logging.info('Ending execution at: '+ str(etl_end_datetime))