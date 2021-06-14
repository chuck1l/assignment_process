import numpy as np
import pandas as pd


def validated_codes(data, pu_state_filter, regional_granularity, con):
    '''
    This function takes in the ranking data from each regional granularity
    and checks the zip, city or county codes against the codes that currently
    exist in LCAD. The data file is returned with a new column "code_validated"
    Where a 1 indicates that the code already exists and is valid, a 0 suggests
    that team needs to validate the code and create a service request to get
    it entered into the system.

    The parameters are the ranking data (data), the state of interest and the
    regional granularity telling us which query to utilize

    The output is the same data file with the mutated column indicating if the
    zip, city or county code is valid
    '''

    print(f'Validating codes for: {regional_granularity}')
    if isinstance(pu_state_filter, list) and len(pu_state_filter)>1:
        print('Checking Codes for All Markets')
        state_filter = tuple(pu_state_filter)
    elif isinstance(pu_state_filter, list) and len(pu_state_filter)==1:
        print(f'Checking codes for Market: {pu_state_filter}')
        state_filter = "('" + pu_state_filter[0] + "')"
        print(state_filter)
    else:
        print(f'Checking codes for Market: {pu_state_filter}')
        state_filter = "('" + pu_state_filter + "')"
        print(state_filter)

    if regional_granularity == 'pu_zip_code':
        code_val_sql = f'''
            SELECT DISTINCT(cz.zip_code)
            FROM dw_lcad.city_zips cz
            JOIN dw_lcad.cities ct
            ON cz.city_code = ct.code
            JOIN dw_lcad.call_center cc
            ON ct.state = cc.center_letter
            WHERE ct.state IN {state_filter};'''

        #  Creating the zip code file to validate zips in pu_zip_code
        codes = pd.read_sql_query(code_val_sql, con)

        codes = codes['zip_code'].tolist()

        # Validate the codes
        data['code_validated'] = np.where(
            data['zip_code'].isin(codes), 1, 0)

        return data

    elif regional_granularity == 'pu_city_code':
        code_val_sql = f'''
            SELECT DISTINCT(ct.code)
            FROM dw_lcad.cities ct
            JOIN dw_lcad.call_center cc
            ON ct.state = cc.center_letter
            WHERE ct.state IN {state_filter};'''

        #  Creating the city code file to validate city codes in pu_city_code
        codes = pd.read_sql_query(code_val_sql, con)
        codes = codes['code'].tolist()

        # Validate the codes
        data['code_validated'] = np.where(
            data['city_code'].isin(codes), 1, 0)

        return data

    elif regional_granularity == 'pu_county_code':
        code_val_sql = f'''
            SELECT DISTINCT(code)
            FROM dw_lcad.county
            WHERE state IN {state_filter};'''
            
        codes = pd.read_sql_query(code_val_sql, con)
        codes = codes['code'].tolist()

        # Validate the codes
        data['code_validated'] = np.where(
            data['county_code'].isin(codes), 1, 0)

        return data


if __name__ == "__main__":
    pass
