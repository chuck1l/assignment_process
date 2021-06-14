import pandas as pd
import json
from datetime import date
from src.code_validations import validated_codes
from src.config import database_connect


def json_package(zip_data, city_data, county_data, volume_data, markets, con):
    '''
    This function takes the input data for each of the rankings files
    generated in the "build_ranking_data" function - for each region -
    and packages them into a JSON output format. Including the volume
    file.

    Parameters are pu_zip_code, pu_city_code, pu_county_code, and
    the state id's for all markets being ran in this iteration

    Output is writing the files into a combined JSON style format, in an 
    LCAD desired arrangement
    '''

    # Validate the codes for each regional granularity, all markets
    zip_data = validated_codes(zip_data, markets, "pu_zip_code", con)
    city_data = validated_codes(city_data, markets, "pu_city_code", con)
    county_data = validated_codes(county_data, markets, "pu_county_code", con)
    # Convert the data frames to dictionaries for JSON formatting, valid codes
    zip_dict = zip_data[
        ['zip_code', 'los_code', 'tp_code', 'preferred_level']
    ][zip_data['code_validated'] == 1].to_dict()
    city_dict = city_data[
        ['city_code', 'los_code', 'tp_code', 'preferred_level']
    ][city_data['code_validated'] == 1].to_dict()
    county_dict = county_data[
        ['county_code', 'los_code', 'tp_code', 'preferred_level']
    ][county_data['code_validated'] == 1].to_dict()

    volume_dict = volume_data.to_dict()
    # Convert the data frames to dictionaries for JSON formatting, invalid codes
    zip_inval_dict = zip_data[
        ['zip_code', 'los_code', 'tp_code', 'preferred_level']
    ][zip_data['code_validated'] == 0].to_dict()
    city_inval_dict = city_data[
        ['city_code', 'los_code', 'tp_code', 'preferred_level']
    ][city_data['code_validated'] == 0].to_dict()
    county_inval_dict = county_data[
        ['county_code', 'los_code', 'tp_code', 'preferred_level']
    ][county_data['code_validated'] == 0].to_dict()

    output = {}
    output['rankings'] = {
        'zip_code': zip_dict,
        'city_code': city_dict,
        'county_code': county_dict
    }
    output['volumes'] = {
        'volume': volume_dict
    }
    output['invalid_codes'] = {
        'invalid_zip_codes': zip_inval_dict,
        'invalid_city_codes': city_inval_dict,
        'invalid_county_codes': county_inval_dict
    }

    with open(f'./aa_final_files/json_output_{date.today()}.text', 'w') as outfile:
        json.dump(output, outfile)

    return None


if __name__ == '__main__':

    # Testing the code...
    con = database_connect()
    zip = pd.read_excel('./data/zip_data.xlsx')
    city =  pd.read_excel('./data/city_data.xlsx')
    county = pd.read_excel('./data/county_data.xlsx')
    volume = pd.read_excel('./data/volume_data.xlsx')
    markets = ['OK', 'DC']
    json_package(zip, city, county, volume, markets, con)

