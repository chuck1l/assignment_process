import pandas as pd
import numpy as np
from sys import exit


def excluded_tps(data):
    '''
    This function is a short-term solution to excluding tps from the
    ranking and volume files. Necessary until the exclude tp from auto-
    assign logic in functional in the appropriate RedShift table/LCAD.
    Simply takes in the dataframe for the assocaiated market, compares the
    list of tp/los combinations, and filters out any existing in the list.

    Input is the df

    Output is the df with the necessary tp/los combinations removed
    '''
    df = data.copy()

    exclusion_df = pd.read_excel('./data/tp_exclusions.xlsx')

    nan_present = exclusion_df[['los_code', 'provider_code']].isna().any()

    if nan_present.any():
        print('NaN values present in the tp exclusion pertinent columns')
        print(nan_present)
        exit()

    exclusion_df['los_code'] = exclusion_df['los_code'].astype(int)
    # Create the tp
    exclusion_df['tp_los_combination'] = exclusion_df[
        ['provider_code', 'los_code']].astype(str).agg('-'.join, axis=1)

    exclusion_list = exclusion_df['tp_los_combination'].values.tolist()

    df['tp_los_combination'] = df[
        ['provider_code', 'los_code']].astype(str).agg('-'.join, axis=1)

    remove_tp_mask = ~df.tp_los_combination.isin(exclusion_list)
    df = df[remove_tp_mask]

    df.drop(columns='tp_los_combination', inplace=True)

    return df
