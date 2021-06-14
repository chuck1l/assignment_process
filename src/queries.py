import numpy as np
import pandas as pd
from src.lyft_change import lyft_inc_health
from src.tp_exclusion import excluded_tps


def base_fact_lcad_ride(
    con,
    pu_state_filter,
    date_min,
    date_max,
    atms_date_min,
    atms_date_max,
    n_ride_minimum,
    filter_costs=False
):
    print('Inside the base_fact_lcad_ride function, queries.py')
    # creating a los_code list for upcoming filtering withing filter_mask
    los_list = [0, 1]
    if pu_state_filter == 'CA':
        los_list.append(44)
    if pu_state_filter == 'NY':
        los_list.append(71)
        los_list.append(72)
        los_list.append(73)
    if pu_state_filter == 'NJ':
        los_list.append(46)
        los_list.append(47)
    los_tuple = tuple(los_list)
    
    # SQL query for fact_lcad_ride
    sql_flr = f'''
        SELECT
            flr.date_ride, flr.los_code, flr.ride_key,
            flr.ride_id, flr.tp_vendor_app_name, flr.provider_code,
            flr.four_bucket_fmv_bins, flr.advanced_notice, flr.pu_city,
            flr.pu_city_code, flr.pu_zip_code, flr.pu_county,
            flr.pu_county_code, flr.pu_state, flr.miles, flr.ride_status,
            flr.estimated_cost_usd, flr.provider_is_lcad, flr.cost_usd,
            flr.provider_contracted_status, flr.override_cost_usd,
            flr.treatment_type_name
        FROM
            dw.fact_lcad_ride flr
        LEFT JOIN dw.dim_date dd ON flr.date_dim_id = dd.date_dim_id
        LEFT JOIN dw_lcad.trans_provider tp ON flr.provider_code = tp.code
        LEFT JOIN dw_lcad.tp_type tt ON tp.tp_type_code = tt.code
        LEFT JOIN dw_lcad.call_center cc
            ON tp.belongs_to_call_center_code = cc.code
        LEFT JOIN dw_lcad.tp_operational tpo
            ON flr.provider_code = tpo.tp_code
        INNER JOIN dw_lcad.tp_to_los tol
            ON flr.provider_code = tol.tp_code
            AND flr.los_code = tol.los_code
        WHERE
            flr.pu_state = '{pu_state_filter}' AND
            tpo.start_date <= current_date AND tpo.end_date >= current_date AND
            cc.center_letter = flr.call_center_state AND
            flr.provider_code IS NOT NULL AND
            flr.miles IS NOT NULL AND
            tt.description IN ('Taxi Non-contracted', 'Non-Dedicated',
            'Dedicated') AND
            flr.ride_status = 'Verified-Paid' AND
            flr.provider_contracted_status IN ('Contracted', 'Non-Contracted') AND
            flr.provider_is_lcad = 'LCAD' AND
            flr.los_code IN {los_tuple} AND
            flr.ride_id = 'A' AND
            flr.cost_usd > 0 AND
            flr.advanced_notice NOT IN ('Urgent', '1 Day', 'Same Day') AND
            dd.weekend_flag = 'N' AND
            dd.holiday_flag = 'N' AND
            tp.status = 1 AND
            LOWER(tp.name) NOT LIKE 'zzz %' AND
            LOWER(tp.name) NOT LIKE 'ssi %' AND
            LOWER(tp.name) NOT LIKE 'sso %' AND
            NOT regexp_substr(tp.name, '^ZZZ') = 'ZZZ' AND
            NOT regexp_substr(tp.name, '^DNU') = 'DNU' AND
            NOT regexp_substr(tp.name, '^[*]') = '*' AND
            flr.date_ride BETWEEN '{date_min}' AND '{date_max}';'''

    # SQL query for fact_lcad_ride_volume file
    sql_vol = f'''
        SELECT
            flr.pu_state, flr.pu_zip_code, flr.provider_code, flr.los_code,
            flr.ride_key, flr.ride_status, flr.date_ride,
            flr.pu_county, flr.pu_city
        FROM
            dw.fact_lcad_ride flr
        LEFT JOIN dw.dim_date dd ON flr.date_dim_id = dd.date_dim_id
        LEFT JOIN dw_lcad.trans_provider tp ON flr.provider_code = tp.code
        LEFT JOIN dw_lcad.tp_type tt ON tp.tp_type_code = tt.code
        LEFT JOIN dw_lcad.call_center cc
            ON tp.belongs_to_call_center_code = cc.code
        LEFT JOIN dw_lcad.tp_operational tpo
            ON flr.provider_code = tpo.tp_code
        INNER JOIN dw_lcad.tp_to_los tol
            ON flr.provider_code = tol.tp_code
            AND flr.los_code = tol.los_code
        WHERE
            flr.pu_state = '{pu_state_filter}' AND
            tpo.start_date <= current_date AND tpo.end_date >= current_date AND
            cc.center_letter = flr.call_center_state AND
            flr.provider_code IS NOT NULL AND
            flr.miles IS NOT NULL AND
            tt.description IN ('Taxi Non-contracted', 'Non-Dedicated',
            'Dedicated') AND
            flr.provider_contracted_status IN ('Contracted', 'Non-Contracted')
            AND flr.provider_is_lcad = 'LCAD' AND
            flr.cost_usd > 0 AND
            tp.status = 1 AND
            LOWER(tp.name) NOT LIKE 'zzz %' AND
            LOWER(tp.name) NOT LIKE 'ssi %' AND
            LOWER(tp.name) NOT LIKE 'sso %' AND
            NOT regexp_substr(tp.name, '^ZZZ') = 'ZZZ' AND
            NOT regexp_substr(tp.name, '^DNU') = 'DNU' AND
            NOT regexp_substr(tp.name, '^[*]') = '*' AND
            flr.date_ride BETWEEN '{atms_date_min}' AND '{atms_date_max}';'''

    #  SQL query for base_atms_usage
    sql_atms = f'''
        SELECT
            flr.provider_code, flr.ride_status, flr.date_ride, flr.ride_id,
            flr.on_my_way_event, flr.pickup_started_event, flr.pickup_performed_event,
            flr.drop_off_started_event, flr.drop_off_performed_event,
            flr.omw_geo_event_percent_coverage, flr.scheduled_event, flr.geo_event
        FROM
            dw.fact_lcad_ride flr
        LEFT JOIN dw_lcad.trans_provider tp
            ON flr.provider_code = tp.code
        LEFT JOIN dw_lcad.call_center cc
            ON tp.belongs_to_call_center_code = cc.code
        LEFT JOIN dw_lcad.tp_operational tpo
            ON flr.provider_code = tpo.tp_code
        INNER JOIN dw_lcad.tp_to_los tol
            ON flr.provider_code = tol.tp_code
            AND flr.los_code = tol.los_code
        WHERE
            flr.pu_state = '{pu_state_filter}' AND
            tpo.start_date <= current_date AND tpo.end_date >= current_date AND
            cc.center_letter = flr.call_center_state AND
            flr.ride_status IN ('Verified', 'Verified-Paid', 'Verified-Denied',
            'Assigned')
            AND flr.date_ride BETWEEN '{atms_date_min}' AND '{atms_date_max}'
            AND flr.ride_id = 'A';'''

    #  Creating the flr, flr_volumes and atms dataframes with SQL queries
    fact_lcad_ride = pd.read_sql_query(sql_flr, con)
    fact_lcad_ride_volumes = pd.read_sql_query(sql_vol, con)
    base_atms_usage = pd.read_sql_query(sql_atms, con)

    # Function to create the digital level field, based on conditions for ATMS
    def digital_level_conditions(df):

        if df['on_my_way_event'] and (
            df['pickup_started_event'] or df['pickup_performed_event']) and (
            df['drop_off_started_event'] or df['drop_off_performed_event']) and (
             df['omw_geo_event_percent_coverage'] >= 50.0):

            return 'PLUS'

        elif (df['pickup_started_event'] or df['pickup_performed_event']) and (
                df['drop_off_started_event'] or df['drop_off_performed_event']):
            return 'DIGITAL'

        elif df['on_my_way_event'] or df['scheduled_event'] or df[
            'pickup_started_event'] or df['pickup_performed_event'] or df[
                'drop_off_started_event'] or df[
                    'drop_off_performed_event'] or df['geo_event']:
            return 'BELOW'

        else:
            return 'NONE'

    base_atms_usage['digital_level'] = base_atms_usage.apply(
        digital_level_conditions, axis=1
    )

    # Creating the ATMS usage feature to utilize in rank calculation
    base_atms_usage['atms_used'] = np.where(base_atms_usage[
        'digital_level'].isin(['PLUS', 'DIGITAL', 'BELOW']), 1.0, 0.0)
    base_atms_usage['atms_eligible'] = 1.0

    base_atms_usage['total_rides'] = base_atms_usage.groupby(
        'provider_code')['atms_eligible'].transform('sum')
    base_atms_usage['atms_usage'] = base_atms_usage.groupby(
        'provider_code')['atms_used'].transform('mean')

    atms_mask4 = (base_atms_usage['total_rides'] >= n_ride_minimum) & (
        base_atms_usage['provider_code'].notna())

    base_atms_usage = base_atms_usage[atms_mask4]

    base_atms_usage = base_atms_usage[
        ['provider_code', 'atms_usage']].drop_duplicates()

    if filter_costs:
        if pu_state_filter == 'ME':
            filter_cost_mask = (fact_lcad_ride['estimated_cost_usd'] >= fact_lcad_ride['cost_usd']) & (
            (fact_lcad_ride['estimated_cost_usd'].notna()) & (fact_lcad_ride['estimated_cost_usd'] > 0) & (
            fact_lcad_ride['cost_usd'] > 0))
        else:
            filter_cost_mask = (fact_lcad_ride['estimated_cost_usd'] == fact_lcad_ride['cost_usd']) & (
            (fact_lcad_ride['estimated_cost_usd'].notna()) & (fact_lcad_ride['estimated_cost_usd'] > 0))

        fact_lcad_ride = fact_lcad_ride[filter_cost_mask]

    required_cities = [
        'amityville', 'bay shore', 'brentwood', 'carle place', 'central islip',
        'coram', 'deer park', 'east islip', 'east meadow', 'east setauket',
        'elmont', 'freeport', 'garden city', 'glen cove', 'great neck',
        'hauppauge', 'hempstead', 'hicksville', 'huntington',
        'huntington station', 'long beach', 'mastic', 'mastic beach',
        'medford', 'mineola', 'new hyde park', 'oceanside', 'patchogue',
        'port jefferson', 'port jefferson station', 'riverhead', 'roosevelt',
        'shirley', 'smithtown', 'uniondale', 'valley stream', 'west babylon',
        'westbury', 'wyandanch'
    ]
    required_counties = [
        'westchester', 'bronx', 'new york', 'queens',
        'kings', 'richmond', 'nassau', 'suffolk'
    ]

    # Ensure that these columns are all lowercase for the NY criteria
    fact_lcad_ride['pu_city'] = fact_lcad_ride[
        'pu_city'].str.lower()
    fact_lcad_ride_volumes['pu_city'] = fact_lcad_ride_volumes[
        'pu_city'].str.lower()
    fact_lcad_ride['pu_county'] = fact_lcad_ride[
        'pu_county'].str.lower()
    fact_lcad_ride_volumes['pu_county'] = fact_lcad_ride_volumes[
        'pu_county'].str.lower()

    if pu_state_filter == 'NY':
        # Keep only specific NY Counties/Cities for Cost
        ny_mask_cost = (fact_lcad_ride.pu_city.isin(required_cities)) & (
            fact_lcad_ride.pu_county.isin(required_counties)
        )
        fact_lcad_ride = fact_lcad_ride[ny_mask_cost]
        # Keep only specific NY Counties/Cities for Volume
        ny_mask_vl = (fact_lcad_ride_volumes.pu_city.isin(required_cities)) & (
            fact_lcad_ride_volumes.pu_county.isin(required_counties)
        )
        fact_lcad_ride_volumes = fact_lcad_ride_volumes[ny_mask_vl]

    fact_lcad_ride['los_code'] = fact_lcad_ride['los_code'].astype(str)
    fact_lcad_ride['binning_cost'] = fact_lcad_ride['cost_usd']
    fact_lcad_ride['mile_bin_value'] = fact_lcad_ride['four_bucket_fmv_bins']

    # Generating bin label column
    conditions_bin_label = [
        fact_lcad_ride['miles'].between(0, 3),
        fact_lcad_ride['miles'].between(4, 6),
        fact_lcad_ride['miles'].between(7, 10),
        fact_lcad_ride['miles'] > 10
    ]
    values_bin_label = ['bin_one', 'bin_two', 'bin_three', 'bin_four']
    fact_lcad_ride['mile_bin_label'] = np.select(
        conditions_bin_label, values_bin_label)

    fact_lcad_ride_volumes['day_of_year'] = fact_lcad_ride_volumes[
        'date_ride'].dt.dayofyear

    fact_lcad_ride = fact_lcad_ride.merge(
        base_atms_usage, on='provider_code', how='left')

    fact_lcad_ride['atms_usage'].fillna(0, inplace=True)

    fact_lcad_ride['atms_usage'] = fact_lcad_ride['atms_usage'].round(3)

    # Convert all Lyft Inc tp_codes to Lyft Healthcare tp_codes short-term-----Remove Eventually
    fact_lcad_ride = lyft_inc_health(fact_lcad_ride)
    fact_lcad_ride_volumes = lyft_inc_health(fact_lcad_ride_volumes)
    fact_lcad_ride = excluded_tps(fact_lcad_ride)
    fact_lcad_ride_volumes = excluded_tps(fact_lcad_ride_volumes)
    # This code needs to be removed after lyft transformation -----------------Remove Eventually

    return fact_lcad_ride, fact_lcad_ride_volumes


def filter_ride_share(
    fact_lcad_ride,
    regional_granularity,
    min_ride_share,
    n_ride_minimum
):
    # Generate the necessary columns and then filter data based on ride share
    output_object = fact_lcad_ride.copy()
    ride_share_grouping = ['pu_state', 'los_code', regional_granularity]

    tp_share_grouping = [
        'pu_state', 'los_code', 'provider_code', regional_granularity]

    output_object['ride_count'] = output_object.groupby(ride_share_grouping)[
        'ride_key'].transform('count')

    output_object['tp_ride_count'] = output_object.groupby(tp_share_grouping)[
        'ride_key'].transform('count')

    output_object['tp_ride_ratio'] = (
        output_object['tp_ride_count'] / output_object['ride_count'])

    ride_share_mask = (output_object['tp_ride_count'] >= n_ride_minimum) & (
        output_object['tp_ride_ratio'] >= min_ride_share)

    output_object = output_object[ride_share_mask]

    output_object.drop(
        ['tp_ride_count', 'tp_ride_ratio', 'ride_count'], axis=1, inplace=True)

    return output_object


def base_regional_volumes(fact_lcad_ride, regional_granularity):
    '''
    This function generates regional volumes to a specified
    level of granularity (base_grouping), pivoted wider by
    regional bin_label, then identifies which bin label has
    the maximum volume (bin_number)

    Parameters are fact_lcad_ride data table and the desired
    regional granularity

    The output is the base regional volumes assigned to the
    regional_volumes variable on build_ranking_data.R
    '''
    print('Inside base_regional_volumes function, queries.py')
    base_grouping = [
        'pu_state', 'los_code', regional_granularity, 'mile_bin_label']
    base_cols = [
        'pu_state', 'los_code', regional_granularity,
        'mile_bin_label', 'ride_key']

    regional_volumes = fact_lcad_ride[base_cols].copy()

    regional_volumes = regional_volumes.groupby(
        base_grouping)['ride_key'].nunique().reset_index(name='volume')
    regional_volumes = regional_volumes.set_index(
        base_grouping).unstack(level='mile_bin_label')

    regional_volumes.columns = regional_volumes.columns.map('_'.join)
    regional_volumes.reset_index(inplace=True)

    regional_volumes.columns = regional_volumes.columns.str.replace(
        'volume_', '')

    regional_volumes.fillna(0, inplace=True)

    volume_grouping = regional_volumes.columns

    necessary_cols = [
        'pu_state', 'los_code', regional_granularity,
        'bin_one', 'bin_two', 'bin_three', 'bin_four'
    ]

    missing_cols = [
        item for item in necessary_cols if item not in volume_grouping]

    if len(missing_cols) > 0:
        for add_col in missing_cols:
            regional_volumes[add_col] = 0

    volume_grouping = regional_volumes.columns

    regional_volumes.columns = regional_volumes.columns.str.replace(
        'bin_', 'region_bin_')

    # Identify which bin has the greatest value for each row
    regional_volumes['bin_number'] = regional_volumes.loc[
        :, ['region_bin_four', 'region_bin_three',
            'region_bin_two', 'region_bin_one']].idxmax(axis=1)

    conditions_bin_num = [
        regional_volumes['bin_number'] == 'region_bin_one',
        regional_volumes['bin_number'] == 'region_bin_two',
        regional_volumes['bin_number'] == 'region_bin_three',
        regional_volumes['bin_number'] == 'region_bin_four'
    ]
    values_bin_num = [1, 2, 3, 4]
    # Reassign numeric values rather than bin names
    regional_volumes['bin_number'] = np.select(
        conditions_bin_num, values_bin_num)

    base_mutate = ['pu_state', 'los_code', regional_granularity]

    regional_volumes[base_mutate].astype(str)

    return regional_volumes


def base_tp_summary(fact_lcad_ride, regional_granularity):
    '''
    Group the fact_lcad_ride df to a desired level of granularity,
    summarize the transportation provider's cost, sum of miles and
    volume of rides then pivot the table wider basing the values on
    each mileage bin

    Parameters are fact_lcad_ride and the regional_granularity

    Output is the tp_summary to the variable tp_summary
    '''
    print('Inside base_tp_summary, queries.py')
    base_grouping = [
        'pu_state',
        'provider_code',
        'los_code',
        regional_granularity,
        'mile_bin_label',
        'atms_usage'
    ]

    tp_summary = fact_lcad_ride.copy()

    tp_summary = tp_summary.groupby(base_grouping).agg(
        {'binning_cost': 'sum', 'miles': 'sum', 'ride_key': 'nunique'}
        ).reset_index()

    tp_summary.rename(columns={
        'binning_cost': 'cost', 'miles': 'miles', 'ride_key': 'volume'
        }, inplace=True)

    pivot_index = [
        'pu_state',
        'provider_code',
        'los_code',
        regional_granularity,
        'atms_usage'
    ]
    # pivot the dataframe wider on mile_bin_label, for cost, miles and volume
    tp_summary = tp_summary.pivot_table(index=pivot_index,
                                        columns='mile_bin_label',
                                        values=['cost', 'miles', 'volume'],
                                        fill_value=0).reset_index()

    tp_summary.columns = [
        s1 + '_' + s2 for (s1, s2) in tp_summary.columns.tolist()
        ]
    tp_summary.reset_index(drop=True, inplace=True)

    tp_summary = tp_summary.rename(
        columns=lambda c: c.replace('code_', 'code') if c.endswith('code_') else c
    )
    tp_summary = tp_summary.rename(
        columns=lambda c: c.replace('state_', 'state') if c.endswith('state_') else c
    )
    tp_summary = tp_summary.rename(
        columns=lambda c: c.replace('usage_', 'usage') if c.endswith('usage_') else c
    )

    # The next few lines of code are for the low volume markets that might
    # not have data for each bin. Adding the column to th df with all zeros
    # to allow the code to continue. No change if the table is complete.
    tp_summary_cols = tp_summary.columns

    necessary_cols = [
        'pu_state', 'provider_code', 'los_code', regional_granularity,
        'atms_usage', 'cost_bin_one', 'cost_bin_two', 'cost_bin_three',
        'cost_bin_four', 'miles_bin_one', 'miles_bin_two', 'miles_bin_three',
        'miles_bin_four', 'volume_bin_one', 'volume_bin_two',
        'volume_bin_three', 'volume_bin_four'
    ]

    missing_cols = [
        item for item in necessary_cols if item not in tp_summary_cols
        ]

    if len(missing_cols) > 0:
        for add_col in missing_cols:
            tp_summary[add_col] = 0

    tp_summary.columns = tp_summary.columns.str.replace(
        'miles_bin', 'tp_miles_bin')
    tp_summary.columns = tp_summary.columns.str.replace(
        'cost_bin', 'tp_cost_bin')
    tp_summary.columns = tp_summary.columns.str.replace(
        'volume_bin', 'tp_volume_bin')

    base_mutate = ['pu_state', 'los_code', regional_granularity]

    numeric_cols = tp_summary.select_dtypes(include=['number']).columns
    tp_summary[numeric_cols] = tp_summary[numeric_cols].fillna(0)
    tp_summary[base_mutate].astype(str)

    return tp_summary


def merge_tp_data_with_regional_volume(
    regional_volumes,
    tp_summary,
    regional_granularity,
    cost_weight,
    atms_weight
):
    '''
    Merge the regional_volumes and tp_summary data frames, and
    create the unit_cost feature based cost/volume for the bin_number
    selected from max value

    Parameters are the data frames to join, regional_granularity and
    complaint_multiple_maximum

    Output is output_data to rankings_data in build_ranking_data.R
    '''
    print('Inside merge_tp_data_with_regional_volume, queries.py')

    output_data = regional_volumes.merge(
        tp_summary, how='left', on=[
            'pu_state', regional_granularity, 'los_code'])

    bin_num_conditions = [
        output_data['bin_number'] == 4,
        output_data['bin_number'] == 3,
        output_data['bin_number'] == 2,
        output_data['bin_number'] == 1
    ]
    bin_num_values = [
        output_data['tp_cost_bin_four'] / output_data['tp_miles_bin_four'],
        output_data['tp_cost_bin_three'] / output_data['tp_volume_bin_three'],
        output_data['tp_cost_bin_two'] / output_data['tp_volume_bin_two'],
        output_data['tp_cost_bin_one'] / output_data['tp_volume_bin_one']
    ]
    output_data['unit_cost'] = np.select(
        bin_num_conditions, bin_num_values)

    output_data['unit_cost'] = output_data['unit_cost'].round(3)

    unit_cost_mask = output_data['unit_cost'].notna()
    output_data = output_data[unit_cost_mask]

    output_data['atms_score'] = output_data.groupby(
        ['pu_state', regional_granularity, 'los_code']
    )['atms_usage'].transform(
        lambda x: (x - x.min()) / (x.max() - x.min())
    )

    output_data['atms_score'] = np.where(
        output_data['atms_score'].isna(), .5 * atms_weight,
        output_data['atms_score'] * atms_weight
    )

    output_data['cost_score'] = output_data.groupby(
        ['pu_state', regional_granularity, 'los_code']
    )['unit_cost'].transform(
        lambda x: 1 - ((x - x.min()) / (x.max() - x.min()))
    )

    output_data['cost_score'] = np.where(
        output_data['cost_score'].isna(), .5 * cost_weight,
        output_data['cost_score'] * cost_weight
    )

    output_data['weighted_ranking_score'] = (
        output_data['atms_score'] + output_data['cost_score']
    )

    base_select = [
        'los_code',
        'provider_code',
        regional_granularity,
        'pu_state',
        'unit_cost',
        'weighted_ranking_score',
        'bin_number',
        'atms_usage'
    ]

    output_data = output_data[base_select]

    return output_data


def add_rank(ranking_data, regional_granularity):
    '''
    This function takes the input data and groups it by the desired
    level of granularity, creates the preferred level ranking, and
    reorders the original data by decreasing preferred level

    Parameters are input data and regional granularity

    Output is returned to output_object in the original function

    '''
    print('Inside add_rank function, queries.py')
    data = ranking_data.copy()
    data = data.sort_values(
        by=[
            regional_granularity,
            'los_code',
            'provider_code',
            'weighted_ranking_score'
        ],
        ascending=(True, True, True, False)
    )

    data['preferred_level'] = data.groupby(
        ['pu_state', regional_granularity, 'los_code'], as_index=False
    )['weighted_ranking_score'].rank(method='first', ascending=False)

    return data


def get_ride_volumes(fact_lcad_ride_volumes, n_ride_minimum, tp_list):
    '''
    This function generates a maximum daily capacity figure
    summary for each combination of pu_state, provider_code,
    and los_code - per day. Also generating the average rate
    of canceled rides for each region as a buffer to be
    applied to each tp based on which region they operate in
    most often

    Parameter is the fact_lcad_ride_volumes data table

    Output is the daily ride summary to the ride_volumes variable
    on build_ranking_data.R
    '''
    print('Inside get_ride_volumes function, queries.py')
    # Identify which zip code tps operate in most often
    tps_zip_code = fact_lcad_ride_volumes.copy()
    tps_zip_code = tps_zip_code.groupby(
        [
            'pu_state', 'pu_zip_code', 'provider_code', 'los_code'
        ], as_index=False
    ).agg(volume_in_zip=('ride_key', 'nunique'))

    tps_zip_code = tps_zip_code.sort_values(
        'volume_in_zip', ascending=False
    ).groupby(['provider_code', 'los_code'], as_index=False).first()

    # Calculate the cancellation rate for each zip code
    zip_cancellation_rate = fact_lcad_ride_volumes.copy()

    zip_cancellation_rate = zip_cancellation_rate.groupby(
        ['pu_state', 'pu_zip_code', 'los_code'], as_index=False
    )['ride_status'].agg(
        {'cancellation_percentage': lambda x: x[x == 'Cancelled'].count()/x.count()}
    )

    buffer_conditions = [
        zip_cancellation_rate['cancellation_percentage'] >= .4,
        zip_cancellation_rate['cancellation_percentage'] < .4,
        np.isnan(zip_cancellation_rate['cancellation_percentage'].values)
    ]
    buffer_values = [
        1.4,
        zip_cancellation_rate['cancellation_percentage'] + 1,
        1
    ]
    zip_cancellation_rate['buffer'] = np.select(
        buffer_conditions, buffer_values)

    zip_can_cols = ['pu_state', 'pu_zip_code', 'los_code', 'buffer']

    zip_cancellation_rate = zip_cancellation_rate[zip_can_cols]

    provider_buffers = tps_zip_code.merge(
        zip_cancellation_rate,
        on=['pu_state', 'pu_zip_code', 'los_code'],
        how='left'
    )

    prov_buff_cols = ['provider_code', 'los_code', 'buffer']
    provider_buffers = provider_buffers[prov_buff_cols].drop_duplicates()
    provider_buffers['buffer'] = provider_buffers['buffer'].round(2)

    cancel_mask = fact_lcad_ride_volumes['ride_status'] != 'Cancelled'
    max_rides_summary = fact_lcad_ride_volumes[cancel_mask].copy()

    # Creating maximum daily capacity for only those TP's that qualify for AA
    qualified_tp_mask = max_rides_summary['provider_code'].isin(tp_list)
    max_rides_summary = max_rides_summary[qualified_tp_mask]

    max_rides_summary = max_rides_summary.groupby(
        ['pu_state', 'provider_code', 'los_code', 'day_of_year'],
        as_index=False
    )['ride_key'].agg(pd.Series.nunique)

    max_rides_summary = max_rides_summary.groupby(
        ['pu_state', 'provider_code', 'los_code'], as_index=False
    )['ride_key'].agg('max').drop_duplicates()

    max_rides_summary = max_rides_summary.sort_values(
        by=['pu_state', 'provider_code', 'los_code']
    )

    max_rides_summary.rename(
        columns={'ride_key': 'max_rides_per_day'}, inplace=True
    )

    max_rides_summary = max_rides_summary.merge(
        provider_buffers,
        on=['provider_code', 'los_code'],
        how='left'
    )

    max_rides_summary['max_daily_capacity'] = (max_rides_summary[
        'max_rides_per_day'] * max_rides_summary['buffer']).round().astype(int)

    max_rides_summary.drop('max_rides_per_day', axis=1, inplace=True)

    return max_rides_summary


def clean_output(data, regional_granularity, ride_volumes):
    '''
    This function simply cleans the output file to match the desired
    format and oragnization

    Parameters are ranking_data, regional_granularity and ride_volumes

    Output is the final documents in the dictionary form
    '''
    print('Inside clean_output function, queries.py')
    ranking_data = data.copy()
    ranking_data.sort_values(
        by=['pu_state', regional_granularity, 'los_code', 'preferred_level'],
        inplace=True
    )

    ranking_data_cols = [
        regional_granularity, 'los_code', 'provider_code', 'preferred_level'
    ]

    ranking_data = ranking_data[ranking_data_cols].copy()

    if regional_granularity == 'pu_county':
        ranking_data.rename(columns={'pu_county': 'county_code'}, inplace=True)

    ranking_data.rename(columns={'provider_code': 'tp_code'}, inplace=True)

    numeric_cols = [
        regional_granularity, 'los_code', 'tp_code', 'preferred_level'
    ]

    ranking_data[numeric_cols] = ranking_data[numeric_cols].astype(int)

    ranking_data.columns = ranking_data.columns.str.replace('pu_', '')

    ride_vol_cols = [
        'pu_state', 'provider_code', 'los_code', 'max_daily_capacity'
    ]

    tp_volumes = ride_volumes[ride_vol_cols].copy()
    tp_volumes.sort_values(
        by=['pu_state', 'provider_code', 'los_code'],
        inplace=True
    )

    output_dictionary = {
        'ranking_data': ranking_data, 'tp_volumes': tp_volumes
    }

    return output_dictionary
