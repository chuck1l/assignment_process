import pandas as pd


def add_names(con, data, region_granularity, state_id):
    '''
    This function simply adds tp_names to the output files for easier
    analysis by the local team, it is temporary until the files are
    automated, and no longer reviewed by local leadership

    Parameters are the data files and connection

    Returns a copy of the data with the tp names included
    '''
    output_df = data.copy()

    sql_tp = "SELECT DISTINCT code, name FROM dw_lcad.trans_provider;"

    sql_zcc = f'''
        SELECT DISTINCT
            bc.state,
            cz.zip_code,
            cz.city_code,
            bc.city,
            bc.county_code,
            c.name AS county
        FROM dw_lcad.city_zips cz
        JOIN dw_lcad.cities bc ON cz.city_code=bc.code
        JOIN dw_lcad.county c ON bc.county_code=c.code
        WHERE bc.state = '{state_id}';'''

    naming_codes = pd.read_sql_query(sql_zcc, con)
    trans_provider = pd.read_sql_query(sql_tp, con)

    output_df = output_df.merge(
        trans_provider, how='left', left_on='tp_code', right_on='code')

    output_df.rename(columns={'name': 'tp_name'}, inplace=True)

    output_cols = [
        region_granularity, 'los_code', 'tp_code', 'tp_name', 'preferred_level'
    ]

    output_df = output_df[output_cols]

    if region_granularity == 'zip_code':
        name_code_mask = ['zip_code', 'city', 'county']
        naming_codes = naming_codes[name_code_mask]
        output_df = output_df.merge(
            naming_codes, how='left', on='zip_code'
        )
        return output_df

    elif region_granularity == 'city_code':
        name_code_mask = ['city_code', 'city', 'county']
        naming_codes = naming_codes[name_code_mask]
        output_df = output_df.merge(
            naming_codes, how='left', on='city_code'
        )
        return output_df

    elif region_granularity == 'county_code':
        name_code_mask = ['county_code', 'county']
        naming_codes = naming_codes[name_code_mask]
        output_df = output_df.merge(
            naming_codes, how='left', on='county_code'
        )
        return output_df


def add_vol_names(con, data):
    '''
    This function simply adds tp_names to the output files for easier
    analysis by the local team, it is temporary until the files are
    automated, and no longer reviewed by local leadership

    Parameters are the data files and connection

    Returns a copy of the data with the tp names included
    '''
    output_df = data.copy()

    sql_tp = "SELECT DISTINCT code, name FROM dw_lcad.trans_provider;"

    trans_provider = pd.read_sql_query(sql_tp, con)

    output_df = output_df.merge(
        trans_provider, how='left', left_on='provider_code', right_on='code')

    output_df.rename(columns={'name': 'provider_name'}, inplace=True)

    output_cols = [
        'pu_state', 'provider_code', 'provider_name', 'los_code',
        'max_daily_capacity'
    ]

    output_df = output_df[output_cols]

    return output_df
