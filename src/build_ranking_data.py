import pandas as pd
import numpy as np
from src.queries import *
from sys import exit


def build_ranking_data(
    params,
    fact_lcad_ride_cost,
    fact_lcad_ride_volumes
):

    # Unpack the parameters from params
    regional_granularity = params['regional_granularity']
    pu_state_filter = params['pu_state_filter']
    min_ride_share = params['min_ride_share']
    override_ratio_maximum = params['override_ratio_maximum']
    n_ride_minimum = params['n_ride_minimum']
    cost_weight = params['cost_weight']
    atms_weight = params['atms_weight']

    print('\n')
    print(f'Pickup State {pu_state_filter}, Regional Granularity:\
        {regional_granularity}')
    print('Inside build_ranking_data function, build_ranking_data.py')

    if not pu_state_filter:
        print('Appropriate pu_state_filter must be applied')
        exit()

    # Filter the cost data based on minimum rides and ratio
    fact_lcad_ride = filter_ride_share(
        fact_lcad_ride_cost,
        regional_granularity,
        min_ride_share,
        n_ride_minimum
    )

    # Grab information about overrides from data set
    override_ratios = fact_lcad_ride.copy()

    ovrr_cond1 = (
        override_ratios['override_cost_usd'] > override_ratios['cost_usd']) & (
        override_ratios['treatment_type_name'] != 'Possibly Infectious'
        )
    overr_cond2 = ~ovrr_cond1
    ovrr_conditions = [ovrr_cond1, overr_cond2]
    ovrr_values = [1, 0]

    override_ratios['is_override'] = np.select(ovrr_conditions, ovrr_values)

    override_ratios['total_rides'] = override_ratios.groupby(
        'provider_code')['ride_key'].transform('count')

    override_ratios['override_sum'] = override_ratios.groupby(
        'provider_code')['is_override'].transform('sum')

    override_ratios['override_ratio'] = override_ratios[
        'override_sum']/override_ratios['total_rides']

    override_ratios = override_ratios[['provider_code', 'override_ratio']]

    override_ratios.drop_duplicates(inplace=True)

    # Filter out high overrides based on the override ratio maximum parameter
    fact_lcad_ride = fact_lcad_ride.merge(
        override_ratios, how='left', on='provider_code')

    override_mask = fact_lcad_ride['override_ratio'] <= override_ratio_maximum

    fact_lcad_ride = fact_lcad_ride[override_mask]

    # Create a regional volume object. Used for unit_cost calculation, based on
    # frequency of rides in this data. Must use fact_lcad_ride (not volumes)
    regional_volumes = base_regional_volumes(
        fact_lcad_ride,
        regional_granularity
    )
    # Summarize the TP's information: Cost, Miles and Volume
    tp_summary = base_tp_summary(
        fact_lcad_ride,
        regional_granularity
    )
    # Merge the data frames together and cleanup - pull out providers which do
    # not service the appropriate mileage bins.
    ranking_data = merge_tp_data_with_regional_volume(
        regional_volumes,
        tp_summary,
        regional_granularity,
        cost_weight,
        atms_weight
    )

    ranking_data = add_rank(ranking_data, regional_granularity)
    # Creating a list of TPs that qualified for AA cost function, filtering the
    # Volume/Capacity output for only the same TPs
    qualified_tp_list = ranking_data['provider_code'].unique()
    # Source the ride volumes from the last 60 days, creating an estimate
    # for the Daily Capacity for each TP in a region/LOS
    ride_volumes = get_ride_volumes(
        fact_lcad_ride_volumes, n_ride_minimum, qualified_tp_list
    )

    output_object = clean_output(
        ranking_data,
        regional_granularity,
        ride_volumes
    )

    return output_object
