#import our modules

import numpy as np
import requests
from datetime import datetime
import pandas as pd
from operator import itemgetter
import json

from utils import _find_numeric_suffix


###########################################################################################

'''
xmacis_parser.py
Written by: Erik Green, WFO Little Rock, Aug 2024

xmacis_parser.py fetches climate data from the XMACIS API for each CLM product 
for a site, then determines ranking and recency statements to be appended to 
the main dictionary in formatter_main.py.

'''


###########################################################################################


# ----------------------------------------------------------
# Helping Sub-Functions
# ----------------------------------------------------------
# def _find_numeric_suffix(myDate):
#     '''
#     This function will take a string date, formatted as 'nn', e.g. '05', and assign a suffix based on the number.
#
#     Parameters
#     ----------
#     myDate : str, a date number, formatted as 'nn', e.g. '05'
#
#     Returns
#     ----------
#     myDate : str, formatted as 'nnTH', 'nnST', 'nnND', 'nnRD'
#
#     '''
#
#     date_suffix = ["TH", "ST", "ND", "RD"]
#
#     if int(myDate) % 10 in [1, 2, 3] and int(myDate) not in [11, 12, 13]:
#         return f'{myDate}{date_suffix[int(myDate) % 10]}'
#     else:
#         return f'{myDate}{date_suffix[0]}'

# ----------------------------------------------------------

# ----------------------------------------------------------
# Main Working Functions
# ----------------------------------------------------------
def _get_xmacis_data(pil):
    '''
    Data comes out as follows, a lists of lists formatted as such:

    [yr, [[jan avg max T, msng cnt],[feb avg max T, msng cnt], etc...],
         [[jan avg min T, msng cnt],[feb avg min T, msng cnt], etc...],
         [[jan avg mean T, msng cnt],[feb avg mean T, msng cnt], etc...],
         [[jan total precip, msng cnt], [feb total precip, msng cnt], etc...],
         [[jan total snow, msng cnt], [feb total snow, msng cnt], etc...],
         ]

    Parameters
    ----------
    pil : str, the pil for the climate product that is being parsed in formatter_main.py, e.g. CLMLZK

    Returns
    ----------
    r : dict, a json dictionary of climate data formatted as the description notes


    '''

    # a dictionary that contains the json syntax to the xmacis api for the four climate sites
    json_dict = {
        'lit': '{"sid":"LITthr","sdate":"por","edate":"por","output":"json","elems":[{"name":"maxt","interval":[0,1],"duration":1,"reduce":{"add":"mcnt","reduce":"mean"},"maxmissing":"7","groupby":"year"},{"name":"mint","interval":[0,1],"duration":1,"reduce":{"add":"mcnt","reduce":"mean"},"maxmissing":"7","groupby":"year"},{"name":"avgt","interval":[0,1],"duration":1,"reduce":{"add":"mcnt","reduce":"mean"},"maxmissing":"7","groupby":"year","prec":0},{"name":"pcpn","interval":[0,1],"duration":1,"reduce":{"add":"mcnt","reduce":"sum"},"maxmissing":"7","groupby":"year","prec":2},{"name":"snow","interval":[0,1],"duration":1,"reduce":{"add":"mcnt","reduce":"sum"},"maxmissing":"7","groupby":"year","prec":1}]}',
        'lzk': '{"sid":"LZK","sdate":"por","edate":"por","output":"json","elems":[{"name":"maxt","interval":[0,1],"duration":1,"reduce":{"add":"mcnt","reduce":"mean"},"maxmissing":"7","groupby":"year"},{"name":"mint","interval":[0,1],"duration":1,"reduce":{"add":"mcnt","reduce":"mean"},"maxmissing":"7","groupby":"year"},{"name":"avgt","interval":[0,1],"duration":1,"reduce":{"add":"mcnt","reduce":"mean"},"maxmissing":"7","groupby":"year","prec":0},{"name":"pcpn","interval":[0,1],"duration":1,"reduce":{"add":"mcnt","reduce":"sum"},"maxmissing":"7","groupby":"year","prec":2},{"name":"snow","interval":[0,1],"duration":1,"reduce":{"add":"mcnt","reduce":"sum"},"maxmissing":"7","groupby":"year","prec":1}]}',
        'pbf': '{"sid":"PBFthr","sdate":"por","edate":"por","output":"json","elems":[{"name":"maxt","interval":[0,1],"duration":1,"reduce":{"add":"mcnt","reduce":"mean"},"maxmissing":"7","groupby":"year"},{"name":"mint","interval":[0,1],"duration":1,"reduce":{"add":"mcnt","reduce":"mean"},"maxmissing":"7","groupby":"year"},{"name":"avgt","interval":[0,1],"duration":1,"reduce":{"add":"mcnt","reduce":"mean"},"maxmissing":"7","groupby":"year","prec":0},{"name":"pcpn","interval":[0,1],"duration":1,"reduce":{"add":"mcnt","reduce":"sum"},"maxmissing":"7","groupby":"year","prec":2},{"name":"snow","interval":[0,1],"duration":1,"reduce":{"add":"mcnt","reduce":"sum"},"maxmissing":"7","groupby":"year","prec":1}]}',
        'hro': '{"sid":"HROthr","sdate":"por","edate":"por","output":"json","elems":[{"name":"maxt","interval":[0,1],"duration":1,"reduce":{"add":"mcnt","reduce":"mean"},"maxmissing":"7","groupby":"year"},{"name":"mint","interval":[0,1],"duration":1,"reduce":{"add":"mcnt","reduce":"mean"},"maxmissing":"7","groupby":"year"},{"name":"avgt","interval":[0,1],"duration":1,"reduce":{"add":"mcnt","reduce":"mean"},"maxmissing":"7","groupby":"year","prec":0},{"name":"pcpn","interval":[0,1],"duration":1,"reduce":{"add":"mcnt","reduce":"sum"},"maxmissing":"7","groupby":"year","prec":2},{"name":"snow","interval":[0,1],"duration":1,"reduce":{"add":"mcnt","reduce":"sum"},"maxmissing":"7","groupby":"year","prec":1}]}'

        # old format not being used anymore
        # 'lit' : '{"sid":"LITthr","sdate":"por","edate":"por","output":"json","elems":[{"name":"maxt","interval":[0,1],"duration":1,"reduce":"mean","maxmissing":"7","groupby":"year"},{"name":"mint","interval":[0,1],"duration":1,"reduce":"mean","maxmissing":"7","groupby":"year"},{"name":"avgt","interval":[0,1],"duration":1,"reduce":"mean","maxmissing":"7","groupby":"year", "prec":0},{"name":"pcpn","interval":[0,1],"duration":1,"reduce":"sum","maxmissing":"7","prec":2,"groupby":"year"},{"name":"snow","interval":[0,1],"duration":1,"reduce":"sum","maxmissing":"7","groupby":"year","prec":1}],"output":"json"}',
        # 'lzk' : '{"sid":"LZKthr","sdate":"por","edate":"por","output":"json","elems":[{"name":"maxt","interval":[0,1],"duration":1,"reduce":"mean","maxmissing":"7","groupby":"year"},{"name":"mint","interval":[0,1],"duration":1,"reduce":"mean","maxmissing":"7","groupby":"year"},{"name":"avgt","interval":[0,1],"duration":1,"reduce":"mean","maxmissing":"7","groupby":"year", "prec":0},{"name":"pcpn","interval":[0,1],"duration":1,"reduce":"sum","maxmissing":"7","prec":2,"groupby":"year"},{"name":"snow","interval":[0,1],"duration":1,"reduce":"sum","maxmissing":"7","groupby":"year","prec":1}],"output":"json"}',
        # 'pbf' : '{"sid":"PBFthr","sdate":"por","edate":"por","output":"json","elems":[{"name":"maxt","interval":[0,1],"duration":1,"reduce":"mean","maxmissing":"7","groupby":"year"},{"name":"mint","interval":[0,1],"duration":1,"reduce":"mean","maxmissing":"7","groupby":"year"},{"name":"avgt","interval":[0,1],"duration":1,"reduce":"mean","maxmissing":"7","groupby":"year", "prec":0},{"name":"pcpn","interval":[0,1],"duration":1,"reduce":"sum","maxmissing":"7","prec":2,"groupby":"year"},{"name":"snow","interval":[0,1],"duration":1,"reduce":"sum","maxmissing":"7","groupby":"year","prec":1}],"output":"json"}',
        # 'hro' : '{"sid":"HROthr","sdate":"por","edate":"por","output":"json","elems":[{"name":"maxt","interval":[0,1],"duration":1,"reduce":"mean","maxmissing":"7","groupby":"year"},{"name":"mint","interval":[0,1],"duration":1,"reduce":"mean","maxmissing":"7","groupby":"year"},{"name":"avgt","interval":[0,1],"duration":1,"reduce":"mean","maxmissing":"7","groupby":"year", "prec":0},{"name":"pcpn","interval":[0,1],"duration":1,"reduce":"sum","maxmissing":"7","prec":2,"groupby":"year"},{"name":"snow","interval":[0,1],"duration":1,"reduce":"sum","maxmissing":"7","groupby":"year","prec":1}],"output":"json"}',

    }

    json_text = json_dict[f'{pil[3:].lower()}']

    # the xmacis api server for station data
    url = f"https://data.rcc-acis.org/StnData?params={json_text}"

    # A GET request to the API
    r = requests.get(url)
    return r.json()

# ----------------------------------------------------------
def _sort_annual_temp_data(data, nrml_df, df_key, index):
    '''

    This will sort the annual temperature data that is retrieved from the XMACIS API, and return a string with
    a ranking and recency statement.

    Parameters
    ----------
    data : list of lists, the main dataframe that is obtained from the XMACIS API
    nrml_df : dataframe, a dataframe of normal values data for the station in question
    df_key : str, key value that determines what station normals to use from the normal values dataframe
    index : int, an index value tha determines what parameter to parse values out of from data

    Returns
    -------
    sorted_vals : a list of lists that is sorted from highest to lowest values, and is also sorted by years for equal value instances
    '''

    # strip the years
    yrs = [int(lst[0]) for lst in data]

    # strip the yearly data for a parameter
    yrly_data = [lst[index] for lst in data]

    # strip the monthly parameter data
    mnly_data = [[mnly_lst[0] for mnly_lst in yrly_lst] for yrly_lst in
                 yrly_data]  # this parses the monthly values out into a list of lists

    # calculate the total number of missing days for each year
    # msng_cnt = [np.sum([mnly_lst[1] for mnly_lst in yrly_lst]) for yrly_lst in yrly_data]

    # # calculate the total number of missing days for each year
    # had to add an exception here for mnly_list, mainly for LZK, where in the first year, data was not available until 12/31/1975,
    # so all other months for 1975 were nan, but not in format [monthly val, missing days], rather just monthly val
    msng_cnt = [np.sum([mnly_lst[1] if isinstance(mnly_lst, list) else 0 for mnly_lst in yrly_lst]) for yrly_lst in
                yrly_data]

    # convert missing to nan's
    param = [[val if val != 'M' else np.nan for val in lst] for lst in mnly_data]

    # this will only work if 'M' values have been converted to nans, will need to address precip 'T' values as well...
    param = [[np.float64(val) for val in lst] for lst in param]

    # calculate the mean value of the paramter (either maxt, mint, or avgt)
    _param = []

    for lst in param:
        if not np.isnan(lst).all():
            _param.append(np.round(np.nanmean(lst), 1))

        else:
            _param.append(np.nan)
    # param = [np.round(np.nanmean(lst),1) for lst in param]

    # convert _param back to param
    param = _param

    # now lets calculate the dfn's
    dfn = [np.round(val - nrml_df[df_key].values[-1], 1) if not np.isnan(val) else np.nan for val in param]

    # now we need to put the data back together...
    # in the form of [yr, val, dfn, msng_count]
    yr_annparam_dfn = [[y, val, dfn, msng_ct] for y, val, dfn, msng_ct in zip(yrs, param, dfn, msng_cnt)]

    # this will remove years where the number of missing days exceeds 7
    yr_annparam_dfn_filtered = [lst for lst in yr_annparam_dfn if lst[-1] <= 7]

    # get the number of excluded years
    num_excluded_yrs = len(yrs) - len(yr_annparam_dfn_filtered)

    # this will create a sorted list, with the largest values first
    # note, in itemgetter, the tuple (1,0) will sort by the paramter value, and then sort by year, placing the most recent year ahead
    return sorted(yr_annparam_dfn_filtered, key=itemgetter(1, 0), reverse=True), str(num_excluded_yrs)

# ----------------------------------------------------------
def _sort_annual_precip_data(data, nrml_df, df_key, index, prec):
    '''

    This will sort the annual precip or snow data that is retrieved from the XMACIS API, and return a string with
    a ranking and recency statement.

    Parameters
    ----------
    data : list of lists, the main dataframe that is obtained from the XMACIS API
    nrml_df : dataframe, a dataframe of normal values data for the station in question
    df_key : str, key value that determines what station normals to use from the normal values dataframe
    index : int, an index value tha determines what parameter to parse values out of from data
    prec : int, the number of decimals to provide rounding for

    Returns
    -------
    sorted_vals : a list of lists that is sorted from highest to lowest values, and is also sorted by years for equal value instances
    '''

    # strip the years
    yrs = [int(lst[0]) for lst in data]

    # strip the yearly data for a parameter
    yrly_data = [lst[index] for lst in data]

    # strip the monthly parameter data
    mnly_data = [[mnly_lst[0] for mnly_lst in yrly_lst] for yrly_lst in
                 yrly_data]  # this parses the monthly values out into a list of lists

    # # calculate the total number of missing days for each year
    # msng_cnt = [np.sum([mnly_lst[1] for mnly_lst in yrly_lst]) for yrly_lst in yrly_data]

    # # calculate the total number of missing days for each year
    # had to add an exception here for mnly_list, mainly for LZK, where in the first year, data was not available until 12/31/1975,
    # so all other months for 1975 were nan, but not in format [monthly val, missing days], rather just monthly val
    msng_cnt = [np.sum([mnly_lst[1] if isinstance(mnly_lst, list) else 0 for mnly_lst in yrly_lst]) for yrly_lst in
                yrly_data]

    # convert missing to nan's
    param = [[val if val != 'M' else np.nan for val in lst] for lst in mnly_data]

    # # this will only work if 'M' values have been converted to nans, will need to address precip 'T' values as well...
    param = [[np.float64(val) if val != 'T' else val for val in lst] for lst in param]

    # iterate through the snowfall data to handle trace values, there will be numerous cases of trace values...
    _param_filtered = []

    for lst in param:
        # if there are no trace values, then calculate sum as normal
        if 'T' not in lst:

            if not np.isnan(lst).all():
                _param_filtered.append(np.round(np.nansum(lst), prec))

                # add a nan value if all monthly values for the year are nan
            else:
                _param_filtered.append(np.nan)

        # if there are trace values in the list, then filter them out
        else:
            filtered_lst = [val if val != 'T' else 0.00001 for val in lst]

            # if the sum is < 0.1, then annual snowfall was only trace 'T'
            if np.nansum(filtered_lst) < 0.1:
                _param_filtered.append(0.00001)

            # if the sum is >= 0.1 after filtering out trace values, find the sum
            elif np.nansum(filtered_lst) >= 0.1:
                _param_filtered.append(np.round(np.nansum(filtered_lst), prec))

        # reclassify the temporary list as the main list again
        param_filtered = _param_filtered

        # now lets calculate the dfn's, handle trace values, else just normal*-1
        dfn = [np.round(val - nrml_df[df_key].values[-1], prec) if val != 0.00001 or val is not np.isnan(val) else
               nrml_df['SNOW'].values[-1] * -1 for val in param_filtered]

        # now we need to put the data back together...
    # in the form of [yr, val, dfn, msng_count]
    yr_annparam_dfn = [[y, val, dfn, msng_ct] for y, val, dfn, msng_ct in zip(yrs, param_filtered, dfn, msng_cnt)]

    # this will remove years where the number of missing days exceeds 7
    yr_annparam_dfn_filtered = [lst for lst in yr_annparam_dfn if lst[-1] <= 7]

    # get the number of excluded years
    num_excluded_yrs = len(yrs) - len(yr_annparam_dfn_filtered)

    # this will create a sorted list, with the largest values first
    # note, in itemgetter, the tuple (1,0) will sort by the paramter value, and then sort by year, placing the most recent year ahead
    return sorted(yr_annparam_dfn_filtered, key=itemgetter(1, 0), reverse=True), str(num_excluded_yrs)

# ----------------------------------------------------------
def _write_rank_text(lst, text_dict, key, year):
    '''

    Parameters
    ----------
    lst : list of lists, the sorted data that is obtained from one of the sorting functions above, depending on temp or precip
    text_dict : dict, the default dictionary of text statements for rank and recency
    key : str, the key value that determines which key of the text_dict dictionary to pull from
    year : int, the year that data is being run for.

    Returns
    -------
    _text : str, a string of text that includes ranking and recency information for a climate variable for a station

    '''

    # get the current year value and dfn for a paramter
    yrly_val = [val[1] for val in lst if val[0] == year][0]
    yrly_dfn = [val[2] for val in lst if val[0] == year][0]

    # logic to determine if the annual value is a tied value
    tie_bool = False
    n = 0
    for sublst in lst:
        if yrly_val in sublst:
            n += 1
    if n > 1:
        tie_bool = True

    # here's the logic for ranking a value amongst the sorted years...
    rank = 1
    for idx, sublst in enumerate(lst):

        # check if our yearly value matches the value in the rank
        # if it does not, then keep counting the rank, if it does, then its a tie and don't count it
        if sublst[1] != yrly_val:
            rank = rank + 1

        # if its our search year, then break the loop
        if sublst[0] == year:
            break

    # check the tie_bool variable, if we need to change the value of idx to incorporate the tied values
    if tie_bool:
        for i, sublst in enumerate(reversed(lst)):
            if yrly_val in sublst:
                idx = len(lst) - i  # return the index in the original list
                break

    # if the yrly val's rank is not #1 but also not last...
    if key in ['maxt', 'mint', 'avgt', 'pcpn']:
        if 1 < rank < (len(lst) - 1):
            # if rank != 1:
            # if the departure from normal is >= 0.0(0), cut to the top half of the dataset
            if yrly_dfn >= 0.0:
                yrly_dfn = f'+{yrly_dfn}'  # format the dfn to include '+'
                filtered_data = lst[:idx]

            # if the departure from normal is < 0.0(0), cut to the lower half of the dataset
            elif yrly_dfn < 0.0:
                filtered_data = lst[idx:]

            filtered_data = [lst for lst in filtered_data if
                             lst[0] != year]  # check and remove if the list has the current year

        # if the rank is last
        elif rank == (len(lst) - 1):
            filtered_data = lst
            filtered_data = [lst for lst in filtered_data if
                             lst[0] != year]  # check and remove if the list has the current year
            prev_rec = filtered_data[-2][1]
            prev_rec_yr = filtered_data[-2][0]

        # if the rank is number 1
        elif rank == 1:
            # if yrly_dfn >= 0.0:
            yrly_dfn = f'+{yrly_dfn}'  # format the dfn to include '+'
            filtered_data = lst
            filtered_data = [lst for lst in filtered_data if
                             lst[0] != year]  # check and remove if the list has the current year
            prev_rec = filtered_data[0][1]
            prev_rec_yr = filtered_data[0][0]

    elif key == 'snow':
        if rank > 1 and float(yrly_val) >= 0.1:
            # if rank != 1:
            # if the departure from normal is >= 0.0(0), cut to the top half of the dataset
            if yrly_dfn >= 0.0:
                yrly_dfn = f'+{yrly_dfn}'  # format the dfn to include '+'
                filtered_data = lst[:idx]

            # if the departure from normal is < 0.0(0), cut to the lower half of the dataset
            elif yrly_dfn < 0.0:
                filtered_data = lst[idx:]

            filtered_data = [lst for lst in filtered_data if
                             lst[0] != year]  # check and remove if the list has the current year

        # if the rank is number 1
        elif rank == 1:
            # if yrly_dfn >= 0.0:
            yrly_dfn = f'+{yrly_dfn}'  # format the dfn to include '+'
            filtered_data = lst
            filtered_data = [lst for lst in filtered_data if
                             lst[0] != year]  # check and remove if the list has the current year
            prev_rec = filtered_data[0][1]
            prev_rec_yr = filtered_data[0][0]

        # if the rank is last
        elif float(yrly_val) <= 0.00001:
            # redeclare an index value here
            for idx, val in enumerate(lst):
                if val[0] == year:
                    break  # this should get us the index value we want
            filtered_data = lst[idx:]
            # filtered_data = lst
            filtered_data = [lst for lst in filtered_data if
                             lst[0] != year]  # check and remove if the list has the current year
            # prev_rec = filtered_data[-2][1]
            # prev_rec_yr = filtered_data[-2][0]

    # calculate residules over the filtered list to find the min residual and thus, the next closest year
    res = []
    for sublst in filtered_data:
        res.append(abs(year - sublst[0]))

    next_closest_year = filtered_data[res.index(min(res))][0]

    # words to fill into the default text strings
    if key in ['maxt', 'mint', 'avgt', 'pcpn']:

        # if rank is not 1st or last, then proceed normally...
        if 1 < rank < (len(lst) - 1):
            replacement_words = [f'{yrly_val}', f'{yrly_dfn}', f'{_find_numeric_suffix(rank)}', f'{next_closest_year}']

        else:
            replacement_words = [f'{yrly_val}', f'{yrly_dfn}', f'{prev_rec}', f'{prev_rec_yr}']

    elif key == 'snow':

        # if rank is not 1st or last, then proceed normally...
        if rank > 1 and float(yrly_val) >= 0.1:
            replacement_words = [f'{yrly_val}', f'{yrly_dfn}', f'{_find_numeric_suffix(rank)}', f'{next_closest_year}']

        elif rank == 1:
            replacement_words = [f'{yrly_val}', f'{yrly_dfn}', f'{prev_rec}', f'{prev_rec_yr}']

        elif float(yrly_val) <= 0.00001:  # for trace values
            # elif rank == (len(lst)-1):
            replacement_words = [f'{yrly_val}', f'{yrly_dfn}', f'{next_closest_year}']

            # if dfn >= 0.0, use 'warmest', 'wettest', 'snowiest' terminology
    if float(yrly_dfn) >= 0.0:
        i = 0  # default statement
        # if it is a tie, use 'tied' terminology
        if tie_bool:
            i = 1
        elif rank == 1:
            i = 4
        _text = text_dict[key.lower()][i].format(*replacement_words).upper()

    # if dfn < 0.0, use 'coldest', 'driest', 'least snowiest' terminology
    elif float(yrly_dfn) < 0.0:
        i = 2  # default statement
        if tie_bool:
            if key == 'snow' and float(
                    yrly_val) <= 0.00001:  # a yearly value of trace will always be a tie, but is also a least snowiest year event
                i = 5

            else:
                # if it is a tie, use 'tied' terminology
                i = 3

        #        elif key == 'snow' and float(yrly_val) <= 0.00001:
        #        #elif rank == 1:
        #            i = 5

        elif key != 'snow' and rank == (len(lst) - 1):
            i = 5

        _text = text_dict[key.lower()][i].format(*replacement_words).upper()

    return _text

# -----------------------------------------------------------
# Main Script - xmacis_mainfunc()
# -----------------------------------------------------------
def xmacis_mainfunc(pil, year):
    '''

    The main function in xmacis_parser, that will fetch XMACIS data through the API for a station,
    and return ranked and ordered climate data.

    Parameters
    ----------
    pil : str, the climate product PIL for which the main function in cla_formatter is called for
    year : int, the year of climate data that is being parsed by cla_formatter

    Returns
    -------
    rank_text : list of lists, a list of rank text data for climate parameters of a station

    '''

    print(f'Obtaining XMACIS Data for {pil[3:]}...\n')

    # pull in the data from the XMACIS API
    main_data = _get_xmacis_data(pil)

    # this should be irrelevant now, as we inhibit the xmacis_mainfunc from running if a climate year is not complete
    # drop the current year (last list in the array) if it is the current year and we are running for a different year...
    # technically, this shouldn't be run in the current year anyways, only for troubleshooting...
    if year != datetime.now().year:
        diff = year - datetime.now().year
        data = main_data['data'][
               :diff]  # this should ensure the correct number of years are taken off for when it is not the current year...

    else:
        data = main_data['data']

    # string for the period of record to be included in the final data dictionary
    por = [data[0][0], data[-1][0], int(data[-1][0]) - int(data[0][0])]

    # -----------------------------------------------------------
    # lets import normals data
    nrml_file = pd.read_csv(f'./normals/{pil[3:]}_Normals.csv')
    nrml_df = pd.DataFrame(nrml_file)

    # -----------------------------------------------------------
    # now lets create the sorted data for each parameter
    sorted_maxt, n1 = _sort_annual_temp_data(data, nrml_df, df_key='MAX', index=1)
    sorted_mint, n2 = _sort_annual_temp_data(data, nrml_df, df_key='MIN', index=2)
    sorted_avgt, n3 = _sort_annual_temp_data(data, nrml_df, df_key='MEAN', index=3)

    sorted_pcpn, n4 = _sort_annual_precip_data(data, nrml_df, df_key='PCPN', index=4, prec=2)
    sorted_snow, n5 = _sort_annual_precip_data(data, nrml_df, df_key='SNOW', index=5, prec=1)

    # -----------------------------------------------------------
    # now lets assemble all the data into a dictionary, to be exported to a text file as json text
    _sorted_data = {
        'sorted_avg_maxt': [[str(val) for val in subl] for subl in sorted_maxt],
        'no_yrs_excluded_avg_maxt': n1,

        'sorted_avg_mint': [[str(val) for val in subl] for subl in sorted_mint],
        'no_yrs_excluded_avg_mint': n2,

        'sorted_avg_meant': [[str(val) for val in subl] for subl in sorted_avgt],
        'no_yrs_excluded_avg_meant': n3,

        'sorted_total_pcpn': [[str(val) for val in subl] for subl in sorted_pcpn],
        'no_yrs_excluded_total_pcpn': n4,

        'sorted_total_snow': [[str(val) for val in subl] for subl in sorted_snow],
        'no_yrs_excluded_total_snow': n5,

    }

    _meta_data = {
        'site_name': main_data['meta']['name'],
        'period_of_record': por,
        'syntax': '[year, avg or total value, departure from normal, no. missing days for year]'
    }

    # declare the new dictionary for sorted data
    xmacis_sorted_data = {}

    # append the sub-dictionaries
    xmacis_sorted_data['meta'] = _meta_data
    xmacis_sorted_data['data'] = _sorted_data

    # export the sorted data to a dictionary for review later if necessary
    with open(f'./output/XMACIS_SortedData_{pil[3:]}.txt', 'w') as f:
        f.write(json.dumps(xmacis_sorted_data, indent=4))
    f.close()

    # -----------------------------------------------------------
    # instantiate the default rank text dictionary
    text_dict = {
        'maxt': [
            'THE YEARLY AVERAGE HIGH TEMPERATURE OF {} ({}) WAS THE {} WARMEST YEAR ON RECORD, AND THE WARMEST SINCE {}.',
            'THE YEARLY AVERAGE HIGH TEMPERATURE OF {} ({}) WAS TIED FOR THE {} WARMEST YEAR ON RECORD, AND THE WARMEST SINCE {}.',
            'THE YEARLY AVERAGE HIGH TEMPERATURE OF {} ({}) WAS THE {} COLDEST YEAR ON RECORD, AND THE COLDEST SINCE {}.',
            'THE YEARLY AVERAGE HIGH TEMPERATURE OF {} ({}) WAS TIED FOR THE {} COLDEST YEAR ON RECORD, AND THE COLDEST SINCE {}.',

            # for extreme values, i.e. new number 1 values
            'THE YEARLY AVERAGE HIGH TEMPERATURE OF {} ({}) WAS THE WARMEST YEAR ON RECORD WHICH SURPASSES THE PREVIOUS RECORD OF {} SET IN {}.',
            'THE YEARLY AVERAGE HIGH TEMPERATURE OF {} ({}) WAS THE COLDEST YEAR ON RECORD WHICH SURPASSES THE PREVIOUS RECORD OF {} SET IN {}.',

            # for tied extreme values, i.e. the current number 1 is tied
        ],
        'mint': [
            'THE YEARLY AVERAGE LOW TEMPERATURE OF {} ({}) WAS THE {} WARMEST YEAR ON RECORD, AND THE WARMEST SINCE {}.',
            'THE YEARLY AVERAGE LOW TEMPERATURE OF {} ({}) WAS TIED FOR THE {} WARMEST YEAR ON RECORD, AND THE WARMEST SINCE {}.',
            'THE YEARLY AVERAGE LOW TEMPERATURE OF {} ({}) WAS THE {} COLDEST YEAR ON RECORD, AND THE COLDEST SINCE {}.',
            'THE YEARLY AVERAGE LOW TEMPERATURE OF {} ({}) WAS TIED FOR THE {} COLDEST YEAR ON RECORD, AND THE COLDEST SINCE {}.',

            # for extreme values, i.e. new number 1 values
            'THE YEARLY AVERAGE LOW TEMPERATURE OF {} ({}) WAS THE WARMEST YEAR ON RECORD WHICH SURPASSES THE PREVIOUS RECORD OF {} SET IN {}.',
            'THE YEARLY AVERAGE LOW TEMPERATURE OF {} ({}) WAS THE COLDEST YEAR ON RECORD WHICH SURPASSES THE PREVIOUS RECORD OF {} SET IN {}.',

        ],
        'avgt': [
            'THE YEARLY AVERAGE MEAN TEMPERATURE OF {} ({}) WAS THE {} WARMEST YEAR ON RECORD, AND THE WARMEST SINCE {}.',
            'THE YEARLY AVERAGE MEAN TEMPERATURE OF {} ({}) WAS TIED FOR THE {} WARMEST YEAR ON RECORD, AND THE WARMEST SINCE {}.',
            'THE YEARLY AVERAGE MEAN TEMPERATURE OF {} ({}) WAS THE {} COLDEST YEAR ON RECORD, AND THE COLDEST SINCE {}.',
            'THE YEARLY AVERAGE MEAN TEMPERATURE OF {} ({}) WAS TIED FOR THE {} COLDEST YEAR ON RECORD, AND THE COLDEST SINCE {}.',

            # for extreme values, i.e. new number 1 values
            'THE YEARLY AVERAGE MEAN TEMPERATURE OF {} ({}) WAS THE WARMEST YEAR ON RECORD WHICH SURPASSES THE PREVIOUS RECORD OF {} SET IN {}.',
            'THE YEARLY AVERAGE MEAN TEMPERATURE OF {} ({}) WAS THE COLDEST YEAR ON RECORD WHICH SURPASSES THE PREVIOUS RECORD OF {} SET IN {}.',

        ],
        'pcpn': [
            'THE YEARLY RAINFALL TOTAL OF {} INCHES ({}) WAS THE {} WETTEST YEAR ON RECORD AND THE WETTEST SINCE {}.',
            'THE YEARLY RAINFALL TOTAL OF {} INCHES ({}) WAS TIED FOR THE {} WETTEST YEAR ON RECORD AND THE WETTEST SINCE {}.',
            'THE YEARLY RAINFALL TOTAL OF {} INCHES ({}) WAS THE {} DRIEST YEAR ON RECORD AND THE DRIEST SINCE {}.',
            'THE YEARLY RAINFALL TOTAL OF {} INCHES ({}) WAS TIED FOR THE {} DRIEST YEAR ON RECORD AND THE DRIEST SINCE {}.',

            # for extreme values, i.e. new number 1 values
            'THE YEARLY RAINFALL TOTAL OF {} ({}) WAS THE WETTEST YEAR ON RECORD WHICH SURPASSES THE PREVIOUS RECORD OF {} SET IN {}.',
            'THE YEARLY RAINFALL TOTAL OF {} ({}) WAS THE DRIEST YEAR ON RECORD WHICH SURPASSES THE PREVIOUS RECORD OF {} SET IN {}.',
        ],
        'snow': [
            'THE YEARLY SNOWFALL TOTAL OF {} INCHES ({}) WAS THE {} SNOWIEST YEAR ON RECORD AND THE SNOWIEST SINCE {}.',
            'THE YEARLY SNOWFALL TOTAL OF {} INCHES ({}) WAS TIED FOR THE {} SNOWIEST YEAR ON RECORD AND THE SNOWIEST SINCE {}.',
            'THE YEARLY SNOWFALL TOTAL OF {} INCHES ({}) WAS THE {} LEAST SNOWIEST YEAR ON RECORD AND THE LEAST SNOWIEST SINCE {}.',
            'THE YEARLY SNOWFALL TOTAL OF {} INCHES ({}) WAS TIED FOR THE {} LEAST SNOWIEST YEAR ON RECORD AND THE LEAST SNOWIEST SINCE {}.',

            # for extreme values, i.e. new number 1 values
            'THE YEARLY SNOWFALL TOTAL OF {} INCHES ({}) WAS THE SNOWIEST YEAR ON RECORD WHICH SURPASSES THE PREVIOUS RECORD OF {} SET IN {}.',
            'THE YEARLY SNOWFALL TOTAL OF {} |*INCHES/TRACE*| ({}) WAS TIED FOR THE LEAST SNOWIEST YEAR ON RECORD AND THE LEAST SNOWIEST SINCE {}.',
        ]
    }

    # turn the sorted data back into a list of lists for easy iteration
    sorted_data = [
        sorted_maxt,
        sorted_mint,
        sorted_avgt,
        sorted_pcpn,
        sorted_snow]

    # now lets iterate and make the rank and recency text strings, and add them to a list
    rank_text = []
    for lst, key in zip(sorted_data, text_dict.keys()):
        rank_text.append(_write_rank_text(lst, text_dict, key, year))

    print(f'Successfully Obtained XMACIS Data for {pil[3:]}...\n')

    return rank_text

