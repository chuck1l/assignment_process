import os
import pandas as pd
import zipfile
from src.names import add_names, add_vol_names
from src.code_validations import validated_codes
from datetime import datetime


def create_volume_file(state_id, data, con):
    '''
    This function creates the volume file excel output in the
    package_data function

    Parameter is the daily maximum capacity file from only the
    pu_zip_code file created in the "build_ranking_data" function

    Output is the excel version of the daily maximum capacity
    dataframe
    '''
    add_names_toggle = True
    vol_file_cols = [
        'pu_state', 'provider_code', 'los_code', 'max_daily_capacity'
    ]
    data_lcad = data.copy()
    data_lcad = data_lcad[vol_file_cols]

    xlsx_name = f'files/volume_{state_id}.xlsx'
    writer = pd.ExcelWriter(xlsx_name, engine='xlsxwriter')
    # Write the data to the worksheets
    data_lcad.to_excel(writer, 'volume_lcad', index=False)
    # Delete this section of code after the initial rollout, below
    if add_names_toggle:
        volume_w_names = add_vol_names(con, data_lcad)
        volume_w_names.to_excel(writer, 'volumme_with_names', index=False)
    # Delete this section of code after the initial rollout, above
    writer.save()

    return None


def create_rank_file(state_id, pu_zip_code, pu_city_code, pu_county_code, con):
    '''
    This function creates the ranking files excel output in the
    package_data function

    Parameters are the state_id, pu_zip_code, pu_city_code, and the
    pu_county_code files

    Output is the excel version of the ranking files from the previous
    analysis in the "build_ranking_data" function
    '''
    # Temporary addition to the script to add the names of providers for local
    add_names_toggle = True

    # For Zip Code
    zip_rank = pu_zip_code['ranking_data']
    zip_rank['zip_code'] = zip_rank['zip_code'].values.astype(str)
    zip_rank['zip_code'] = zip_rank['zip_code'].str.zfill(5)

    zip_rank = validated_codes(zip_rank, state_id, "pu_zip_code", con)

    zip_invalid_mask = zip_rank['code_validated'] == 0
    zip_valid_mask = zip_rank['code_validated'] == 1

    zip_rank_inval = zip_rank[zip_invalid_mask]
    zip_rank_val = zip_rank[zip_valid_mask]
    # For City Code
    city_rank = pu_city_code['ranking_data']

    city_rank = validated_codes(city_rank, state_id, "pu_city_code", con)

    city_invalid_mask = city_rank['code_validated'] == 0
    city_valid_mask = city_rank['code_validated'] == 1

    city_rank_inval = city_rank[city_invalid_mask]
    city_rank_val = city_rank[city_valid_mask]
    # For County Code
    county_rank = pu_county_code['ranking_data']

    county_rank = validated_codes(county_rank, state_id, "pu_county_code", con)

    county_invalid_mask = county_rank['code_validated'] == 0
    county_valid_mask = county_rank['code_validated'] == 1

    county_rank_inval = county_rank[county_invalid_mask]
    county_rank_val = county_rank[county_valid_mask]
    # Output only desired columns for the LCAD format
    zip_output_cols = [
        'zip_code', 'los_code', 'tp_code', 'preferred_level'
    ]
    city_output_cols = [
        'city_code', 'los_code', 'tp_code', 'preferred_level'
    ]
    county_output_cols = [
        'county_code', 'los_code', 'tp_code', 'preferred_level'
    ]

    zip_ranking_lcad = zip_rank_val[zip_output_cols]
    city_ranking_lcad = city_rank_val[city_output_cols]
    county_ranking_lcad = county_rank_val[county_output_cols]

    # Sending the files for LCAD straight to one document to help Diane-----Remove me from this code
    lcad_zip_path = f'./lcad_ready/{state_id}_ranking_lcad_zipCode.xlsx'
    lcad_city_path = f'./lcad_ready/{state_id}_ranking_lcad_cityCode.xlsx'
    lcad_county_path = f'./lcad_ready/{state_id}_ranking_lcad_countyCode.xlsx'
    lcad_writer_zip = pd.ExcelWriter(lcad_zip_path)
    lcad_writer_city = pd.ExcelWriter(lcad_city_path)
    lcad_writer_county = pd.ExcelWriter(lcad_county_path)

    zip_ranking_lcad.to_excel(lcad_writer_zip, 'zip_file', index=False)
    city_ranking_lcad.to_excel(lcad_writer_city, 'city_file', index=False)
    county_ranking_lcad.to_excel(lcad_writer_county, 'county_file', index=False)

    lcad_wb_zip = lcad_writer_zip.book
    lcad_wb_city = lcad_writer_city.book
    lcad_wb_county = lcad_writer_county.book

    lcad_worksheet_zip = lcad_writer_zip.sheets['zip_file']
    lcad_worksheet_city = lcad_writer_city.sheets['city_file']
    lcad_worksheet_county = lcad_writer_county.sheets['county_file']

    lcad_cell_format_zip = lcad_wb_zip.add_format()
    lcad_cell_format_zip.set_border(1)
    lcad_cell_format_zip.set_border_color('black')

    lcad_cell_format_city = lcad_wb_city.add_format()
    lcad_cell_format_city.set_border(1)
    lcad_cell_format_city.set_border_color('black')

    lcad_cell_format_county = lcad_wb_county.add_format()
    lcad_cell_format_county.set_border(1)
    lcad_cell_format_county.set_border_color('black')

    lcad_worksheet_zip.set_column(0, 3, 10.5, lcad_cell_format_zip)
    lcad_worksheet_city.set_column(0, 3, 10.5, lcad_cell_format_city)
    lcad_worksheet_county.set_column(0, 3, 10.5, lcad_cell_format_county)

    lcad_writer_zip.save()
    lcad_writer_city.save()
    lcad_writer_county.save()
    # Sending the files for LCAD straight to one document to help Diane-----Remove me from this code

    # Creating and formatting the Excel file output
    xlsx_name = f'files/ranks_{state_id}.xlsx'
    writer = pd.ExcelWriter(xlsx_name, engine='xlsxwriter')
    # Write the data to the worksheets
    zip_ranking_lcad.to_excel(writer, 'zip_ranking_lcad', index=False)
    city_ranking_lcad.to_excel(writer, 'city_ranking_lcad', index=False)
    county_ranking_lcad.to_excel(writer, 'county_ranking_lcad', index=False)

    # Delete this section of code after the initial rollout, below
    if add_names_toggle:
        zip_w_names = add_names(con, zip_ranking_lcad, 'zip_code', state_id)
        city_w_names = add_names(con, city_ranking_lcad, 'city_code', state_id)
        county_w_names = add_names(
            con, county_ranking_lcad, 'county_code', state_id
        )
        zip_w_names.to_excel(writer, 'zip_with_names', index=False)
        city_w_names.to_excel(writer, 'city_with_names', index=False)
        county_w_names.to_excel(writer, 'county_with_names', index=False)
    # Delete this section of code after the initial rollout, above

    zip_rank_inval.to_excel(writer, 'invalid zip codes', index=False)
    city_rank_inval.to_excel(writer, 'invalid city codes', index=False)
    county_rank_inval.to_excel(writer, 'invalid county codes', index=False)

    wb = writer.book
    # Naming the worksheets to call for formatting
    worksheet_zip = writer.sheets['zip_ranking_lcad']
    worksheet_city = writer.sheets['city_ranking_lcad']
    worksheet_county = writer.sheets['county_ranking_lcad']

    # Setting the column widths and border format for LCAD
    cell_format = wb.add_format()
    cell_format.set_border(1)
    cell_format.set_border_color('black')
    worksheet_zip.set_column(0, 0, 10.5, cell_format)
    worksheet_city.set_column(0, 0, 10.5, cell_format)
    worksheet_county.set_column(0, 0, 10.5, cell_format)
    worksheet_zip.set_column(1, 2, 8, cell_format)
    worksheet_city.set_column(1, 2, 8, cell_format)
    worksheet_county.set_column(1, 2, 8, cell_format)
    worksheet_zip.set_column(3, 3, 13, cell_format)
    worksheet_city.set_column(3, 3, 13, cell_format)
    worksheet_county.set_column(3, 3, 13, cell_format)

    writer.save()

    return None


def package_data(pu_zip_code, pu_city_code, pu_county_code, state_id, con):
    '''
    This function creates the volume and rankings excel files, and zips them
    for the package_aa function

    Parameters are pu_zip_code, pu_city_code, pu_county_code, state_id, and
    the connection to Redshift

    No output, saves the files to the appropriate directory
    '''

    if os.path.exists('./files'):
        os.system('rm -rf files')
    if os.path.exists('./files.zip'):
        os.system('rm -rf files.zip')
    if os.path.exists('./data.zip'):
        os.system('rm -rf data.zip')

    os.system('mkdir files')
    # Create and save excel files to "files" dir - for volume and rankings
    create_volume_file(state_id, pu_zip_code['tp_volumes'], con)
    create_rank_file(state_id, pu_zip_code, pu_city_code, pu_county_code, con)
    # Create a zip file of the documents in the "files" directory
    file_paths = [
        f'./files/ranks_{state_id}.xlsx',
        f'./files/volume_{state_id}.xlsx'
    ]
    with zipfile.ZipFile('data.zip', 'w') as zip:
        for file in file_paths:
            zip.write(file)

    return None


def package_aa(pu_zip_code, pu_city_code, pu_county_code, state_id, con):
    '''
    This function takes the input data for each of the rankings files
    generated in the "build_ranking_data" function - for each region -
    and packages them in the excel file output. Including the volume
    file.

    Parameters are pu_zip_code, pu_city_code, pu_county_code, and
    the state_id for the state currently running

    Output is writing the files to the excel files, in the LCAD desired
    format
    '''

    os.system('rm -rf files')
    os.system('rm -rf *.zip')

    package_data(pu_zip_code, pu_city_code, pu_county_code, state_id, con)

    if os.path.isdir('./aa_final_files'):
        print('The aa_final_files directory is prepared')
    else:
        os.system('mkdir ./aa_final_files')

    now = str(datetime.now())
    now = now.replace(' ', '_')
    mv_zf = f'mv data.zip ./aa_final_files/aa_generation_{state_id}_{now}.zip'
    os.system(mv_zf)
    os.system('rm -rf files')
    os.system('rm -rf *.zip')

    return None
