from src.JSON_packaging import json_package
import numpy as np
import pandas as pd
import time
from src.build_ranking_data import build_ranking_data
from src.queries import base_fact_lcad_ride
from src.config import database_connect
from src.output_packaging import package_aa
from datetime import date, timedelta

start_time = time.time()
'''
rank_states = ['AL', 'AR', 'CA', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IL', 'IN',
               'KS', 'KY', 'LA', 'MA', 'ME', 'MI', 'MO', 'MS', 'NC', 'NE', 'NJ',
               'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'SC', 'TX', 'UT', 'VA',
               'WA', 'WI', 'WV']

excluded_markets = ['AK', 'AZ', 'CO', 'IA', 'ID', 'MD', 'MN', 'MT', 'ND', 'NH', 'RI', 'SD',
                   'TN', 'VT', 'WY']
'''
rank_states = ['OK']

min_date = str(date.today() - timedelta(days=120))  # Start Date "COST"
max_date = str(date.today() - timedelta(days=60))  # End Date "COST"
atms_min_date = str(date.today() - timedelta(days=60))  # Start Date "ATMS"
atms_max_date = str(date.today() - timedelta(days=0))  # End Date "ATMS"

# Setting thresholds that TP's must meet to qualify in rankings
number_min_rides = 5  # tps must have at least n rides by region and los
min_ride_share_ratio = 0.015  # tps must have at least 1.5% ratio by region/los
override_ratio_limit = 0.05  # remove providers with override ratios > 5%

# Setting parameters for the TP ranking, ATMS and Cost weights
cost_weighting = 0.4  # Percent of cost weight in ATMS based ranking score
atms_weighting = 0.6  # Percent of ATMS weight in ATMS based ranking score

con = database_connect()

# Creating a single dataframe for volume files, all markets upload to LCAD
single_vol_file = pd.DataFrame()
single_zip_file = pd.DataFrame()
single_city_file = pd.DataFrame()
single_county_file = pd.DataFrame()

# Specify required parameters for each level of granularity, and desired market
for rank_state in rank_states:

    params_zip = {
        'pu_state_filter': rank_state,
        'regional_granularity': 'pu_zip_code',
        'date_min': min_date,
        'date_max': max_date,
        'atms_date_min': atms_min_date,
        'atms_date_max': atms_max_date,
        'min_ride_share': min_ride_share_ratio,
        'override_ratio_maximum': override_ratio_limit,
        'n_ride_minimum': number_min_rides,
        'cost_weight': cost_weighting,
        'atms_weight': atms_weighting
    }
    params_city = {
        'pu_state_filter': rank_state,
        'regional_granularity': 'pu_city_code',
        'date_min': min_date,
        'date_max': max_date,
        'atms_date_min': atms_min_date,
        'atms_date_max': atms_max_date,
        'min_ride_share': min_ride_share_ratio,
        'override_ratio_maximum': override_ratio_limit,
        'n_ride_minimum': number_min_rides,
        'cost_weight': cost_weighting,
        'atms_weight': atms_weighting
    }
    params_county = {
        'pu_state_filter': rank_state,
        'regional_granularity': 'pu_county_code',
        'date_min': min_date,
        'date_max': max_date,
        'atms_date_min': atms_min_date,
        'atms_date_max': atms_max_date,
        'min_ride_share': min_ride_share_ratio,
        'override_ratio_maximum': override_ratio_limit,
        'n_ride_minimum': number_min_rides,
        'cost_weight': cost_weighting,
        'atms_weight': atms_weighting
    }
    # Get base query for the rest of the analysis,
    fact_lcad_ride_cost, fact_lcad_ride_volumes = base_fact_lcad_ride(
        con,
        rank_state,
        min_date,
        max_date,
        atms_min_date,
        atms_max_date,
        number_min_rides,
        False
    )
    # Build all of the ranks as per granularity
    pu_zip_code = build_ranking_data(
        params_zip,
        fact_lcad_ride_cost,
        fact_lcad_ride_volumes
    )
    pu_city_code = build_ranking_data(
        params_city,
        fact_lcad_ride_cost,
        fact_lcad_ride_volumes
    )
    pu_county_code = build_ranking_data(
        params_county,
        fact_lcad_ride_cost,
        fact_lcad_ride_volumes
    )
    # Package the results and write to excel files for each state
    package_aa(pu_zip_code, pu_city_code, pu_county_code, rank_state, con)

    # Concatenate current file with the running single df for all markets
    single_vol_file = pd.concat([single_vol_file, pu_zip_code['tp_volumes']])
    single_zip_file = pd.concat([single_zip_file, pu_zip_code['ranking_data']])
    single_city_file = pd.concat([single_city_file, pu_city_code['ranking_data']])
    single_county_file = pd.concat([single_county_file, pu_county_code['ranking_data']])

# prepare the output for all combined volume results
single_vol_file_lcad = single_vol_file[[
    'provider_code', 'los_code', 'max_daily_capacity']].copy()
single_vol_file_lcad.rename(columns={
    'provider_code': 'TP_CODE',
    'los_code': 'LOS_CODE',
    'max_daily_capacity': 'QUANTITY'
}, inplace=True)
# Create a quantity sum of tp/LOS if repeat values exist, happens for DC:VA
single_vol_file_lcad = single_vol_file_lcad.groupby(['TP_CODE', 'LOS_CODE'])[
    'QUANTITY'].sum().reset_index(name='QUANTITY')
# Creating and formatting the Excel volume file output
xlsx_name = f'./lcad_ready/AA_combined_volumes_{date.today()}.xlsx'
writer = pd.ExcelWriter(xlsx_name, engine='xlsxwriter')
single_vol_file_lcad.to_excel(writer, 'volume_lcad', index=False)
writer.save()
# Create the JSON format output
# json_package(
#     single_zip_file, single_city_file, single_county_file, 
#     single_vol_file_lcad, rank_states, con
# )

if con is not None:
    con.close()
    print('Database connection closed.')

print((time.time() - start_time)/60)
