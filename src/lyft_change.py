import numpy as np


def lyft_inc_health(data):
    '''
    This function takes the Lyft Inc tp_codes and replaces them with
    the Lyft Healthcare tp_codes as the transition for the organization
    moves forward

    Parameters the the data file, volume or rank

    Returns a copy of the data with the lyft codes changed
    '''
    print('Entering lyft transform function')
    df = data.copy()
    # If the tp code is:
    lyft_con = [
        df['provider_code'] == 504839, df['provider_code'] == 505412,
        df['provider_code'] == 503903, df['provider_code'] == 504055,
        df['provider_code'] == 503905, df['provider_code'] == 507099,
        df['provider_code'] == 504253, df['provider_code'] == 504204,
        df['provider_code'] == 504134, df['provider_code'] == 503904,
        df['provider_code'] == 510638, df['provider_code'] == 507095,
        df['provider_code'] == 503910, df['provider_code'] == 504406,
        df['provider_code'] == 504279, df['provider_code'] == 503906,
        df['provider_code'] == 508421, df['provider_code'] == 504321,
        df['provider_code'] == 510406, df['provider_code'] == 504329,
        df['provider_code'] == 504079, df['provider_code'] == 507529,
        df['provider_code'] == 504700, df['provider_code'] == 504797,
        df['provider_code'] == 507926, df['provider_code'] == 503907,
        df['provider_code'] == 507093, df['provider_code'] == 504205,
        df['provider_code'] == 504016, df['provider_code'] == 504691,
        df['provider_code'] == 507244, df['provider_code'] == 503793,
        df['provider_code'] == 503909, df['provider_code'] == 504278,
        df['provider_code'] == 507676, df['provider_code'] == 504217,
        df['provider_code'] == 504560, df['provider_code'] == 504662,
        df['provider_code'] == 507980, df['provider_code'] == 507518,
        df['provider_code'] == 504673, df['provider_code'] == 506510,
        df['provider_code'] == 507097, df['provider_code'] == 503908,
        df['provider_code'] == 504135, df['provider_code'] == 510609
    ]
    # Replace the tp code with:
    lyft_val = [
        511114, 511117, 511119, 511107, 511122, 511125, 511100, 511144,
        511110, 511128, 511109, 511130, 511112, 511111, 511108, 511133,
        511151, 511101, 511148, 511147, 511140, 511134, 511102, 511137,
        511138, 511135, 511136, 511132, 511099, 511131, 511129, 511146,
        511126, 511105, 511127, 511149, 511123, 511145, 511106, 511121,
        511120, 511124, 511116, 511115, 511113, 511150
    ]

    df['provider_code'] = np.select(
        lyft_con, lyft_val, df['provider_code'])

    return df
