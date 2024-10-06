# IMPORT MODULES

# BUILT IN
import calendar
import copy
#import itertools
from itertools import groupby
from operator import itemgetter
#import os, sys
import requests
from urllib.request import urlopen

# BEAUTIFUL SOUP
from bs4 import BeautifulSoup

# MISC
#import argparse
from datetime import datetime
import json
import numpy.ma as ma
import numpy as np
import pandas as pd
import textwrap


#@title **Step 4 - Run the Script!**

#@markdown Run this cell to execute the script, and text files will be sent to both 'output' and 'json_dict'.


'''
This is the script we will use to run the script in a google colab notebook

Unfortunately, we need EVERYTHING in this notebook

'''

# ----------------------------------------------------------
# declare some globals as constants that will be used in the
# functions above quite frequently...
table_sep = f'-'*93 + f'\n'
header = f'='*93 + f'\n'
space = ' '




# ##################################################################
# From utils.py
# ##################################################################
# ----------------------------------------------------------
def _find_numeric_suffix(myDate):
    '''
    This function will take a string date, formatted as 'nn', e.g. '05', and assign a suffix based on the number.

    Parameters
    ----------
    myDate : str, a date number, formatted as 'nn', e.g. '05'

    Returns
    ----------
    myDate : str, formatted as 'nnTH', 'nnST', 'nnND', 'nnRD'

    '''

    date_suffix = ["TH", "ST", "ND", "RD"]

    if int(myDate) % 10 in [1, 2, 3] and int(myDate) not in [11, 12, 13]:
        return f'{myDate}{date_suffix[int(myDate) % 10]}'
    else:
        return f'{myDate}{date_suffix[0]}'

# ----------------------------------------------------------
def _get_station_name(pil):

    '''
    Parameters
    ----------
    pil : str, the six letter climate product pil, e.g. 'CLMLZK'

    Returns
    -------
    stn_name : str, the three letter station ID, parsed from pil, e.g. 'LZK'
    '''

    if pil[3:] == 'LZK':
        stn_name = 'NORTH LITTLE ROCK'
    elif pil[3:] == 'LIT':
        stn_name = 'LITTLE ROCK'
    elif pil[3:] == 'HRO':
        stn_name = 'HARRISON'
    elif pil[3:] == 'PBF':
        stn_name = 'PINE BLUFF'

    return stn_name

# ----------------------------------------------------------
def _month_range(year):
    '''
    This function will define a range of months, based on the entered param, year.

    Parameters
    ----------
    year : int, the year for which a range of months is being defined

    Returns
    ----------
    months : list, a list of months as integers
    current_year_flag : bool, set to True when we are in the current year, False when not the current year

    '''

    # set up current date parameters to check with
    current_date = datetime.now()
    current_year = current_date.year
    current_month = current_date.month

    # check if current year
    # if it is current year, then we need to automatically define a
    # range of months based on the current month
    if year == current_year:
        return [x for x in range(current_month + 1)][2:], True  # set up the months to be the last most recently completed month, starting with Feb for Jan product

    # if it is not the current year, then set up a list with all months
    elif year != current_year:
        return [x for x in range(13)][2:], False  # start in feb for jan CLM product

# ----------------------------------------------------------
def _make_supplemental_data_text(data_dict, f, year, char_lim):

    '''

    Parameters
    ----------
    data_dict : dict, the main data dictionary that includes all necessary information
    f : file, the open text file that is being written to
    char_lim : int, the character limit for word wrapping in the text file

    Returns
    -------
    None

    '''


    # -------------------------------------------------
    print('Writing in Supplemental Summary Text...\n...')

    # ----------------------------------------------------------
    # instantiate a text wrapper for writing to the textfile
    wrapper = textwrap.TextWrapper(width = char_lim)  # limits character wrapping

    # ----------------------------------------------------------
    # lets add the XMACIS Temp Data here
    if not ma.is_masked(data_dict['xmacis_data']):
        maxt_text = data_dict['xmacis_data'][0]
        mint_text = data_dict['xmacis_data'][1]
        avgt_text = data_dict['xmacis_data'][2]

        f.write('\n.YEARLY TEMPERATURES...\n')
        f.write(f'{wrapper.fill(maxt_text)}\n\n')
        f.write(f'{wrapper.fill(mint_text)}\n\n')
        f.write(f'{wrapper.fill(avgt_text)}\n\n')

        # -------------------------------------------------
        # add ranking statement
        rank_statement = '**FOR RANKING PURPOSES SHOWN ABOVE, A YEAR WAS OMITTED IF IT WAS MISSING MORE THAN 7 DAYS WORTH OF DATA**'
        f.write(f'{wrapper.fill(rank_statement)}\n\n\n')

    else:
        f.write('\n.YEARLY TEMPERATURES...\n')
        f.write(f'\nUPDATED RANKED ANNUAL TEMPERATURE DATA FOR {year} IS NOT AVAILABLE YET.\n\n')

    # -------------------------------------------------
    # write in additional significant phenomena
    f.write('.ADDITIONAL SIGNIFICANT EVENTS DURING THE YEAR...\n')

    # -------------------------------------------------
    # get the min/max SLP data
    def _get_maxmin(val_lst, dt_lst):

        _temp_lst = [val for val in val_lst if val != 'M']
        idx = val_lst.index(np.max(_temp_lst))
        dt = dt_lst[idx]

        return np.max(_temp_lst), dt

    # if the data exists and is not an empty list
    if data_dict['cf6_data']['pres']['min_pres']:

        min_slp, min_slp_dt = _get_maxmin(data_dict['cf6_data']['pres']['min_pres'],
                                          data_dict['cf6_data']['pres']['min_pres_dates'])

        f.write(f'MINIMUM SEA LEVEL PRESSURE WAS MEASURED AT {min_slp:.2f} INCHES ON {min_slp_dt}.\n\n')

    if data_dict['cf6_data']['pres']['max_pres']:
        max_slp, max_slp_dt = _get_maxmin(data_dict['cf6_data']['pres']['max_pres'],
                                          data_dict['cf6_data']['pres']['max_pres_dates'])

        f.write(f'MAXIMUM SEA LEVEL PRESSURE WAS MEASURED AT {max_slp:.2f} INCHES ON {max_slp_dt}.\n\n')


    # -------------------------------------------------
    # now lets add in wind data if it exists...

    wsp = data_dict['cf6_data']['wind']['max_gst']
    wdr = data_dict['cf6_data']['wind']['max_wdr']
    dates = data_dict['cf6_data']['wind']['max_wd_dates']

    def _format_windgusts(wsp, wdr, dates):

        # format the date
        m = [datetime.strptime(d, '%Y-%m-%d').strftime('%B').upper() for d in dates]
        d = [_find_numeric_suffix(d.split('-')[-1]) for d in dates]

        dts = [f'{m} {d}' for m, d in zip(m, d)]

        for wsp, wdr, dts in zip(wsp, wdr, dts):
            f.write(f'WINDS GUSTED TO {wsp} MPH/{wdr} DEGREES ON {dts}.\n\n')

    if wsp:
        _format_windgusts(wsp, wdr, dates)


    # -------------------------------------------------
    # write in the yearly rainfall and snowfall data
    # get the XMACIS rain and snow data
    if not ma.is_masked(data_dict['xmacis_data']):
        pcpn_text = data_dict['xmacis_data'][3]
        snow_text = data_dict['xmacis_data'][4]

        f.write('\n.YEARLY RAINFALL...\n')
        f.write(f'{wrapper.fill(pcpn_text)}\n\n')

        f.write('\n.YEARLY SNOWFALL...\n')
        f.write(f'{wrapper.fill(snow_text)}\n\n')

    else:
        f.write('\n.YEARLY RAINFALL...\n')
        f.write(f'\nUPDATED RANKED ANNUAL RAINFALL DATA FOR {year} IS NOT AVAILABLE YET.\n')

        f.write('\n.YEARLY SNOWFALL...\n')
        f.write(f'\nUPDATED RANKED ANNUAL SNOWFALL DATA FOR {year} IS NOT AVAILABLE YET.\n')

# ----------------------------------------------------------
def _make_temp_table(data_dict, f, months, year, stn_name):
    '''
    This function will write in the annual temperature summary table to the supplemental CLA product text file.

    Parameters
    ----------
    data_dict : dict, the main dictionary that includes all our main CLM product data
    f : file, the text file that is currently open that is being written to
    months : list, a list of months as integers
    year : int, the climate year
    stn_name : str, the three letter climate station ID

    Returns
    ----------
    None

    '''

    # -------------------------------------------------
    def consecutive(lst):

        '''

        This function will take a list of date strings, and organize consecutive or sequential days into tuples,
        with the first and last day of the sequence in the tuple, or will simply create tuple with matching start
        and end days for non-consecutive values.

        Parameters
        ----------
        lst : list, list of date values as strings

        Returns
        -------
        consec_days : list, a list of consecutive days as tuples
        nonconsec_days : list, a list of non-consecutive days as tuples

        '''

        ranges = []
        lst = sorted(lst) # sometimes the values are not properly sorted

        for k, g in groupby(enumerate(lst), lambda x: int(x[0]) - int(x[1])):
            group = (map(itemgetter(1), g))
            group = list(map(int, group))
            ranges.append([group[0], group[-1]])

        consec_days = ['-'.join([f'{i[0]:02}', f'{i[1]:02}']) for i in ranges if i[0] != i[-1]]
        nonconsec_days = [f'{i[0]:02}' for i in ranges if i[0] == i[-1]]

        return consec_days, nonconsec_days

    # -------------------------------------------------
    def _find_annual_extreme_dates(temp_dates_lst, idx):

        '''
        This function will parse a temp dates list with a given index and return a string of the extreme dates

        Parameters
        ----------
        temp_dates_lst : list, a list of the extreme temperature dates for a month
        idx : int, the index value of the extreme temperature date for the year

        Returns
        ----------
        dt : str, a string of the formatted extreme temp dates

        '''

        if isinstance(temp_dates_lst[idx], list):
            # sort the days in ascending order
            dt = sorted([int(i.split('/')[-1]) for i in temp_dates_lst[idx]])

            # check for consecutive days here and format accordingly
            consec_days, nonconsec_days = consecutive(dt)

            if consec_days:
                dt = sorted(consec_days + nonconsec_days)  # add the two lists back together and sort them
            dt = ','.join(f'{d}' for d in dt)  # join all items in dt into one string

        else:
            dt = temp_dates_lst[idx].split('/')[-1]
        return dt

    # -------------------------------------------------
    print('Writing in Annual Temp Table...\n...')

    # put together the data for iterating quickly
    months = [calendar.month_name[i-1][:3].upper() for i in months]

    avg_high_temps = [data_dict[m]['monthly_avg_temps_dfn'][0] for m in months]
    avg_low_temps = [data_dict[m]['monthly_avg_temps_dfn'][1] for m in months]
    avg_temps = [data_dict[m]['monthly_avg_temps_dfn'][2] for m in months]
    avg_temps_dfn = [data_dict[m]['monthly_avg_temps_dfn'][3] for m in months]

    high_temps = [data_dict[m]['monthly_max_min'][0] for m in months]
    high_temp_dates = [data_dict[m]['high_dates'] for m in months]

    low_temps = [data_dict[m]['monthly_max_min'][1] for m in months]
    low_temp_dates = [data_dict[m]['low_dates'] for m in months]

    # now lets write the table into the text file
    arr = [f'{year} TEMPERATURE AVERAGES AND EXTREMES', f'{stn_name}, ARKANSAS']
    f.write('\n{:60}{:60}\n'.format(*arr))
    f.write(table_sep)
    f.write('             AVERAGE TEMPERATURES             |            TEMPERATURE EXTREMES\n')

    arr = ['MONTH', 'HIGH', 'LOW', 'MONTHLY', 'DFN', '|', 'MAX', 'DATE(S)', 'MIN', 'DATE(S)']
    f.write('{:9}{:9}{:9}{:12}{:7}{:5}{:7}{:15}{:7}{:9}\n'.format(*arr))

    f.write(table_sep)

    # for loop begins
    # order is as follows: month, avg max, avg min, avg mean, avg dfn, max, max dates, min, min dates
    for m, avgmx, avgmn, avgt, dfn, mx, mxdt, mn, mndt in zip(
                                                            months,
                                                            avg_high_temps, avg_low_temps, avg_temps, avg_temps_dfn,
                                                            high_temps, high_temp_dates,
                                                            low_temps, low_temp_dates
                                                             ):

        if dfn > 0.0:
            dfn = f'+{dfn}'

        # format the max temp dates
        if isinstance(mxdt, list):
            mxdt = [i.split('/')[-1] for i in mxdt]

            # check for consecutive days here and format accordingly
            consec_days, nonconsec_days = consecutive(mxdt)
            mxdt = sorted(consec_days + nonconsec_days)  # add the two lists back together and sort them
            mxdt = ','.join(f'{d}' for d in mxdt)  # join all items in dt into one string

            #mxdt = ",".join(sorted(mxdt, key=int))  # sort the days in ascending order
        else:
            mxdt = mxdt.split('/')[-1]

        # format the min temp dates
        if isinstance(mndt, list):
            mndt = [i.split('/')[-1] for i in mndt]

            # check for consecutive days here and format accordingly
            consec_days, nonconsec_days = consecutive(mndt)
            mndt = sorted(consec_days + nonconsec_days)  # add the two lists back together and sort them
            mndt = ','.join(f'{d}' for d in mndt)  # join all items in dt into one string

            #mndt = ",".join(sorted(mndt, key=int))  # sort the days in ascending order
        else:
            mndt = mndt.split('/')[-1]

        # now write in the data to the text file
        arr = [f'{m}', f'{avgmx}', f'{avgmn}', f'{avgt}', f'{dfn}', '|', f'{mx}', f'{mxdt}', f'{mn}', f'{mndt}']
        f.write('{:9}{:9}{:9}{:11}{:8}{:5}{:7}{:15}{:7}{:9}\n'.format(*arr))
        # for loop ends

    # -------------------------------------------------
    # next add in the annual summary data
    f.write(table_sep)  # add a table separator

    # compile the yearly avg max,min,avg temps and avg temp dfn
    yrly_avgmx = np.round(np.mean(avg_high_temps), 1)
    yrly_avgmn = np.round(np.mean(avg_low_temps), 1)
    yrly_avg = np.round(np.mean(avg_temps), 1)
    yrly_avg_dfn = np.round(np.mean(avg_temps_dfn), 1)

    # if the dfn is > 0, then add '+' to the string
    if yrly_avg_dfn > 0.0:
        yrly_avg_dfn = f'+{yrly_avg_dfn}'


    # -------------------------------------------------
    # format the annual max temp date(s)
    yrly_mx = np.max(high_temps)       # yearly max temp
    idx = high_temps.index(yrly_mx)    # index value of the max temp

    # if there are multiple values in idx
    if high_temps.count(yrly_mx) > 1:
        # index values of all occurrences
        idx = [i for i, x in enumerate(high_temps) if x == yrly_mx]

        # temp list
        _dt_lst = []
        for i in idx:
            _dt_lst.append(_find_annual_extreme_dates(high_temp_dates, i))

        yrly_mxdt = [f'{calendar.month_name[i + 1][:3].upper()} {dt}' for i, dt in zip(idx, _dt_lst)]
        #yrly_mxdt = '/'.join([i for i in yrly_mxdt])

    # for when there is only one idx
    else:
        mxdt = _find_annual_extreme_dates(high_temp_dates, idx) # return value with the extreme value date(s)
        yrly_mxdt = f'{calendar.month_name[idx + 1][:3].upper()} {mxdt}' # formatted string of the extreme value date(s)


    # -------------------------------------------------
    # format the annual min temp date(s)
    yrly_mn = np.min(low_temps)
    idx = low_temps.index(yrly_mn)

    # if there are multiple values in idx
    if low_temps.count(yrly_mn) > 1:
        # index values of all occurrences
        idx = [i for i, x in enumerate(low_temps) if x == yrly_mn]

        # temp list
        _dt_lst = []
        for i in idx:
            _dt_lst.append(_find_annual_extreme_dates(low_temp_dates, i))

        yrly_mndt = [f'{calendar.month_name[i + 1][:3].upper()} {dt}' for i, dt in zip(idx, _dt_lst)]
        #yrly_mndt = '/'.join([i for i in yrly_mndt])

    # for when there is only one idx
    else:
        mndt = _find_annual_extreme_dates(low_temp_dates, idx)
        yrly_mndt = f'{calendar.month_name[idx + 1][:3].upper()} {mndt}'


    # -------------------------------------------------
    # add in handling for excessive numbers of same matching high/low days
    # boolean flags
    mx_flag = False
    mn_flag = False

    # if these values arrive as lists, then there are multiple months, otherwise, they'll be one string
    if isinstance(yrly_mxdt, list):
        mx_flag = True

    if isinstance(yrly_mndt, list):
        mn_flag = True

    # -------------------------------------------------
    # more than one date after splitting max and min values...
    if mx_flag and mn_flag:

        # if more values in yrly_mndt, then add extra to yrly_mxdt
        if len(yrly_mxdt) < len(yrly_mndt):
            if isinstance(yrly_mxdt, list): # we can only append if it is a list
                for i in range(np.abs(len(yrly_mxdt) - len(yrly_mndt))):
                    yrly_mxdt.append('')
            else:
                yrly_mxdt = [yrly_mxdt] # convert to a list if it is not one
                for i in range(np.abs(len(yrly_mxdt) - len(yrly_mndt))):
                    yrly_mxdt.append('')

        # if more values in yrly_mxdt, then add extra to yrly_mndt
        elif len(yrly_mxdt) > len(yrly_mndt):
            if isinstance(yrly_mndt, list): # we can only append if it is a list
                for i in range(np.abs(len(yrly_mxdt) - len(yrly_mndt))):
                    yrly_mndt.append('')
            else:
                yrly_mndt = [yrly_mndt] # convert to a list if it is not one
                for i in range(np.abs(len(yrly_mxdt) - len(yrly_mndt))):
                    yrly_mndt.append('')

        # lets try to conglomerate the two lists into one here with list comprehension...
        mxmn = [[mx, mn] for mx, mn in zip(yrly_mxdt[1:], yrly_mndt[1:])]

        arr = ['ANNUAL', f'{yrly_avgmx}', f'{yrly_avgmn}', f'{yrly_avg}', f'{yrly_avg_dfn}',
               '|', f'{yrly_mx}', f'{yrly_mxdt[0]}', f'{yrly_mn}', f'{yrly_mndt[0]}']
        f.write('{:9}{:9}{:9}{:11}{:8}{:5}{:7}{:15}{:7}{:9}\n'.format(*arr))

        for i in mxmn:
            arr = ['', f'{i[0]}', f'{i[1]}']
            f.write('{:58}{:22}{:22}\n'.format(*arr))

    # -------------------------------------------------
    # only the max temps flagged for multiple values
    elif mx_flag:
        arr = ['ANNUAL', f'{yrly_avgmx}', f'{yrly_avgmn}', f'{yrly_avg}', f'{yrly_avg_dfn}',
               '|', f'{yrly_mx}', f'{yrly_mxdt[0]}', f'{yrly_mn}', f'{yrly_mndt}']
        f.write('{:9}{:9}{:9}{:11}{:8}{:5}{:7}{:15}{:7}{:9}\n'.format(*arr))

        for mx in yrly_mxdt[1:]:
            arr = ['', f'{mx}']
            f.write('{:58}{:58}\n'.format(*arr))

    # -------------------------------------------------
    # only the min temps flagged for multiple values
    elif mn_flag:
        arr = ['ANNUAL', f'{yrly_avgmx}', f'{yrly_avgmn}', f'{yrly_avg}', f'{yrly_avg_dfn}',
               '|', f'{yrly_mx}', f'{yrly_mxdt}', f'{yrly_mn}', f'{yrly_mndt[0]}']
        f.write('{:9}{:9}{:9}{:11}{:8}{:5}{:7}{:15}{:7}{:9}\n'.format(*arr))

        for mn in yrly_mndt[1:]:
            arr = ['', f'{mn}']
            f.write('{:80}{:80}\n'.format(*arr))

    # -------------------------------------------------
    # 'normal cases', no extra dates
    else:
        arr = ['ANNUAL', f'{yrly_avgmx}', f'{yrly_avgmn}', f'{yrly_avg}', f'{yrly_avg_dfn}',
               '|', f'{yrly_mx}', f'{yrly_mxdt}', f'{yrly_mn}', f'{yrly_mndt}']
        f.write('{:9}{:9}{:9}{:11}{:8}{:5}{:7}{:15}{:7}{:9}\n'.format(*arr))

# ----------------------------------------------------------
def _make_precip_table(data_dict, f, months, year, stn_name):
    '''
    This function will write in the annual rain summary table to the supplemental CLA product text file.

    Parameters
    ----------
    data_dict : dict, the main dictionary that includes all our main CLM product data
    f : file, the text file that is currently open that is being written to
    months : list, a list of months as integers
    year : int, the climate year
    stn_name : str, the three letter climate station ID

    Returns
    ----------
    None

    '''

    print('Writing in Annual Precip Table...\n...')

    # # --------------------------------------------
    # add table headers and column information

    arr = [f'{year} RAINFALL, DEPARTURES, AND EXTREMES', f'{stn_name}, ARKANSAS']
    f.write('\n\n{:60}{:60}\n'.format(*arr))
    f.write(table_sep)

    arr = ['MONTH', 'RAINFALL', 'DFN', 'MAX/CALENDAR DAY', 'MAX/24 HOURS']
    f.write('{:13}{:15}{:18}{:28}{:28}\n'.format(*arr))
    f.write(table_sep)

    # --------------------------------------------
    # now lets compile and organize the data
    # put together the data for iterating quickly
    months = [calendar.month_name[i-1][:3].upper() for i in months]

    precip = [data_dict[m]['monthly_rain_and_dfn'][0] for m in months]
    precip_dfn = [data_dict[m]['monthly_rain_and_dfn'][1] for m in months]

    # the calendar day max is actually index 3 and 4
    precip_mx_clndr = [data_dict[m]['max_clndr_24hr_rain'][2] for m in months] # previously idx 0
    precip_mx_clndr_dates = [data_dict[m]['max_clndr_24hr_rain'][3] for m in months] # previously idx 1

    # these are the true 24 hr totals, can overlap days
    precip_mx_storm_total = [data_dict[m]['max_clndr_24hr_rain'][0] for m in months] # previously idx 2
    precip_mx_storm_total_dates = [data_dict[m]['max_clndr_24hr_rain'][1] for m in months] # previously idx 3

    # for loop begins
    for m, pcp, dfn, pcp_mx_clndr, pcp_mx_clndr_dates, pcp_mx_storm_total, pcp_mx_storm_total_dates in zip(
            months,
            precip, precip_dfn,
            precip_mx_clndr, precip_mx_clndr_dates,
            precip_mx_storm_total, precip_mx_storm_total_dates
    ):
        # --------------------------------------------
        # lets do some formatting here
        # assign a sign to dfn if it is positive
        if dfn >= 0.0:
            dfn = f'+{dfn:.2f}'

        # --------------------------------------------
        # lets format the max calendar day rainfall dates here...
        # for date in pcp_mx_clndr_dates:
        bdt = list(filter(None, (pcp_mx_clndr_dates.split('TO')[0].split(' '))))[0]
        edt = list(filter(None, (pcp_mx_clndr_dates.split('TO')[-1].split(' '))))[0]

        # if the dates are the same, then format for one day
        if bdt == edt:
            clndr_mxdt = _find_numeric_suffix(bdt.split('/')[-1])

        # if the dates are not the same, then format for multiple dates
        elif bdt != edt:
            bdt = _find_numeric_suffix(bdt.split('/')[-1])
            edt = _find_numeric_suffix(edt.split('/')[-1])
            clndr_mxdt = f'{bdt}-{edt}'

        # clean up the final string to be entered
        pcp_mx_clndr = f'{pcp_mx_clndr:.2f}/{clndr_mxdt}'


        # --------------------------------------------
        # lets format the max storm total rainfall dates here...
        bdt = list(filter(None, (pcp_mx_storm_total_dates.split('TO')[0].split(' '))))[0]
        edt = list(filter(None, (pcp_mx_storm_total_dates.split('TO')[-1].split(' '))))[0]

        # if the dates are the same, then format for one day
        if bdt == edt:
            stormtotal_mxdt = _find_numeric_suffix(bdt.split('/')[-1])

        # if the dates are not the same, then format for multiple dates
        elif bdt != edt:
            bdt = _find_numeric_suffix(bdt.split('/')[-1])
            edt = _find_numeric_suffix(edt.split('/')[-1])
            stormtotal_mxdt = f'{bdt}-{edt}'

        # clean up the final string to be entered
        stormtotal_mx = f'{pcp_mx_storm_total:.2f}/{stormtotal_mxdt}'

        # write in the line of monthly data
        #arr = [f'{m}', f'{pcp:.2f}', f'{dfn}', f'{pcp_mx_clndr}', f'{clndr_mxdt}', f'{pcp_mx_storm_total:.2f}', f'{stormtotal_mxdt}']
        arr = [f'{m}', f'{pcp:.2f}', f'{dfn}', f'{pcp_mx_clndr}', f'{stormtotal_mx}']
        f.write('{:13}{:14}{:19}{:28}{:28}\n'.format(*arr))
        # end of for loop

    # --------------------------------------------
    # now lets add the annual data
    yrly_rain = np.round(np.sum(precip), 2)
    yrly_dfn = np.round(np.sum(precip_dfn), 2)

    # if the yrly precip dfn is >= 0.0, then assign a positive sign in the string
    if yrly_dfn >= 0.0:
        yrly_dfn = f'+{yrly_dfn}'

    # -------------------------------------------------
    def _find_annual_extreme_dates(pcp_dates_lst, idx):

        '''
        This function will parse a precip dates list with a given index and return a string of the extreme dates

        pcp_dates_lst : list, a list of the extreme precip dates for a month
        idx : int, the index value of the extreme precip date for the year

        returns:
        yrly_mxdt : str, a string of the formatted extreme precip dates

        '''

        bdt = list(filter(None, (pcp_dates_lst[idx].split('TO')[0].split(' '))))[0]
        edt = list(filter(None, (pcp_dates_lst[idx].split('TO')[-1].split(' '))))[0]

        if bdt == edt:
            yrly_mxdt = bdt.split('/')[-1]

        elif bdt != edt:
            bdt = bdt.split('/')[-1]
            edt = edt.split('/')[-1]
            yrly_mxdt = f'{bdt}-{edt}'

        return yrly_mxdt

    # -------------------------------------------------

    # yearly calendar day max precip date
    clndr_pcp_mx = np.max(precip_mx_clndr)
    idx = precip_mx_clndr.index(clndr_pcp_mx)  # index value of the calendar day max precip

    clndr_max_pcp_dt = _find_annual_extreme_dates(precip_mx_clndr_dates, idx)
    yrly_clndr_pcp_mx = f'{calendar.month_name[idx + 1][:3].upper()} {clndr_max_pcp_dt}'

    # -------------------------------------------------

    # yearly daily storm total max precip date
    storm_total_pcp_mx = np.max(precip_mx_storm_total)
    idx = precip_mx_storm_total.index(storm_total_pcp_mx)

    storm_total_pcp_max_dt = _find_annual_extreme_dates(precip_mx_storm_total_dates, idx)
    yrly_storm_total_pcp_max_dt = f'{calendar.month_name[idx + 1][:3].upper()} {storm_total_pcp_max_dt}'

    # -------------------------------------------------
    # write in the annual precip summary data
    f.write(table_sep)
    arr = ['ANNUAL', f'{yrly_rain:.2f}', f'{yrly_dfn}', f'{clndr_pcp_mx:.2f}/{yrly_clndr_pcp_mx}', f'{storm_total_pcp_mx:.2f}/{yrly_storm_total_pcp_max_dt}']
    f.write('{:13}{:14}{:19}{:28}{:28}\n'.format(*arr))

# ----------------------------------------------------------
def _make_snow_table(data_dict, f, months, year, stn_name):
    '''
    This function will write in the annual snow summary table to the supplemental CLA product text file.

    Parameters
    ----------
    data_dict : dict, the main dictionary that includes all our main CLM product data
    f : file, the text file that is currently open that is being written to
    months : list, a list of months as integers
    year : int, the climate year
    stn_name : str, the three letter climate station ID

    Returns
    ----------
    None

    '''
    # --------------------------------------------
    def consecutive(lst):

        '''

        This function will take a list of date strings, and organize consecutive or sequential days into tuples,
        with the first and last day of the sequence in the tuple, or will simply create tuple with matching start
        and end days for non-consecutive values.

        Parameters
        ----------
        lst : list, list of date values as strings

        Returns
        -------
        consec_days : list, a list of consecutive days as tuples
        nonconsec_days : list, a list of non-consecutive days as tuples

        '''

        # format the incoming dates to integers and sort them
        # sometimes our list will need this, sometimes it will not, so add error handling
        try:
            lst = sorted([int(x.split('/')[-1]) for x in lst])
        except AttributeError:
            pass

        ranges = []
        for k, g in groupby(enumerate(lst), lambda x: x[0] - x[1]):
            group = (map(itemgetter(1), g))
            group = list(map(int, group))
            ranges.append([group[0], group[-1]])

        consec_days = ['-'.join([f'{i[0]:02}', f'{i[1]:02}']) for i in ranges if i[0] != i[-1]]
        nonconsec_days = [f'{i[0]:02}' for i in ranges if i[0] == i[-1]]

        return consec_days, nonconsec_days
    # --------------------------------------------
    print('Writing in Annual Snow Table...\n...')

    # --------------------------------------------
    # add table headers and column information
    arr = [f'{year} SNOWFALL, DEPARTURES, AND EXTREMES', f'{stn_name}, ARKANSAS']
    f.write('\n\n{:60}{:60}\n'.format(*arr))
    f.write(table_sep)

    arr = ['MONTH', 'SNOW', 'DFN', 'MAX/CALENDAR DAY', 'MAX/24 HOUR', 'GREATEST DEPTH/DATE(S)']
    f.write('{:8}{:10}{:10}{:23}{:18}{:18}\n'.format(*arr))
    f.write(table_sep)

    # --------------------------------------------
    # now lets compile and organize the data
    # put together the data for iterating quickly
    snow_months = ['JAN', 'FEB', 'MAR', 'APR', 'OCT', 'NOV', 'DEC']  # these are the months we need for the snow table summary

    # filter out snow_months based on our input of months
    snow_months = [calendar.month_name[m - 1][:3].upper() for m in months if
                       calendar.month_name[m - 1][:3].upper() in snow_months]

    snow = [data_dict[m]['monthly_snow_sdepth_dfn'][0] for m in snow_months]
    snow_dfn = [data_dict[m]['monthly_snow_sdepth_dfn'][1] for m in snow_months]

    # data from the CLM, this tends to be flaky sometimes....
    # clm_mx_sdepth = [data_dict[m]['monthly_snow_sdepth_dfn'][2] for m in snow_months]
    # clm_mx_sdepth_dt = [data_dict[m]['monthly_snow_sdepth_dfn'][3] for m in snow_months]
    #
    # clm_mx_24hr_snow = [data_dict[m]['max_clndr_24hr_snow'][0] for m in snow_months]
    # clm_mx_24hr_snow_dt = [data_dict[m]['max_clndr_24hr_snow'][1] for m in snow_months]

    # data from the CF6 sheet
    cf6_mx_sdepth = [data_dict['cf6_data'][m]['max_sdepth'][0] for m in snow_months]
    cf6_mx_sdepth_dt = [data_dict['cf6_data'][m]['max_sdepth'][1] for m in snow_months]

    cf6_mx_24hr_snow = [data_dict['cf6_data'][m]['max_clndr_24hr_snow'][0] for m in snow_months]
    cf6_mx_24hr_snow_dt = [data_dict['cf6_data'][m]['max_clndr_24hr_snow'][1] for m in snow_months]

    # for loop begins
    for m, sn, dfn, mx_sd, mx_sd_dt, mx_24hr_sn, mx_24hr_sn_dt in zip(
            snow_months,
            snow, snow_dfn,
            # clm_mx_sdepth, clm_mx_sdepth_dt,
            # clm_mx_24hr_snow, clm_mx_24hr_snow_dt

            # lets try using the snow data from the CF6
            cf6_mx_sdepth, cf6_mx_sdepth_dt,
            cf6_mx_24hr_snow, cf6_mx_24hr_snow_dt
    ):

        # adjust dfn parameters
        if dfn > 0.0:
            dfn = f'+{dfn}'

        elif dfn == 0.0:
            dfn = f' {dfn}'

        # --------------------------------------------
        # lets format the max calendar day and 24 hr snowfall dates here...
        # first check if max 24 hr snow is > 0.0
        if not isinstance(mx_24hr_sn, str) and mx_24hr_sn != 0.0: # we have to include exception for string 'T'
            dt = _find_numeric_suffix(mx_24hr_sn_dt.split('/')[-1])
            mx_24hr_sn = f'{mx_24hr_sn:.1f}/{dt}'

        # for when max 24 hr snow is 0.0
        elif not isinstance(mx_24hr_sn, str) and mx_24hr_sn == 0.0: # we have to include exception for string 'T'
            mx_24hr_sn = '0.0'

        # for when max 24 hr snow is 'T'
        elif isinstance(mx_24hr_sn, str): # this should handle Trace 'T' values
            dt = _find_numeric_suffix(mx_24hr_sn_dt.split('/')[-1])
            mx_24hr_sn = f'{mx_24hr_sn}/{dt}'

        # --------------------------------------------
        # lets format the max snow depth/dates
        if mx_sd > 0.0:
            if isinstance(mx_sd_dt, list):

                consec_days, nonconsec_days = consecutive(mx_sd_dt) # find sequences of consecutive days if they exist
                if consec_days:
                    _consec_days = [i.split('-') for i in consec_days] # split the consec day strings

                    # use list comprehension to add the numeric suffices to the consec day strings
                    _consec_days = ['-'.join([_find_numeric_suffix(i[0]), _find_numeric_suffix(i[1])]) for i in _consec_days]

                    # use list comprehension to add numeric suffices to the non consec day strings
                    nonconsec_days = [_find_numeric_suffix(i) for i in nonconsec_days]

                    # now lets add them together and join to form one string
                    dt = _consec_days + nonconsec_days
                    dt = ",".join(d for d in dt)

                # consec_days is an empty list, then all values will be in nonconsec_days
                else:
                    dt = [_find_numeric_suffix(i) for i in nonconsec_days]
                    dt = ",".join(d for d in dt)

            # if there is only one date to handle, not a list of values
            else:
                dt = _find_numeric_suffix(mx_sd_dt.split('/')[-1])

            # set up the max snow depth and the date(s) string
            mx_sd = f'{int(mx_sd)}/{dt}'

        # if max snow depth is just 0, then set to '0' string
        else:
            mx_sd = '0'

        # now let's write in the monthly total/max data to the table
        arr = [f'{m}', f'{sn}', f'{dfn}', f'{mx_24hr_sn}', f'{mx_24hr_sn}', f'{mx_sd}']
        f.write('{:8}{:9}{:11}{:23}{:18}{:18}\n'.format(*arr))
        # end of for loop

    # -----------------------------------------------------------
    # now lets add the annual data max values at the bottom of the table
    # -----------------------------------------------------------
    # Function for finding sum or max values of snow parameters
    def _snow_total(snow, key):
        '''

        This function will perform three operations, including finding annual snow total, finding max
        calendar day snow total, and will find the annual max snow depth value, for values in the arg snow.

        Parameters
        ----------
        snow : list, list of snow fall data, including monthly totals, daily max values, and monthly snow depth
        key : str, key value that determines which operation to perform, key values include:
                    -- 'annual_snow'
                    -- 'daily_max_snow'
                    -- 'snow_depth'

        Returns
        -------
        total or max value : float or int, depending on key, float (snow total or max), int (snow depth)

        '''

        # ----------------------------------------------------
        # if we are handling annual snow fall totals
        if key in 'annual_snow':
            # get the annual snowfall total
            sn_filtered = [s for s in snow if s != 'T']  # filter out trace values

            # if the two lists are equal after filtering, then we filtered no T's
            if len(snow) == len(sn_filtered):
                return np.round(np.sum(sn_filtered), 1)

            # if the two lists are not equal after filtering, then we filtered T's out...
            elif len(snow) != len(sn_filtered):
                # if the sum is still 0.0, then annual snowfall was only trace 'T'
                if np.sum(sn_filtered) == 0.0:
                    return 'T'

                # if the sum is not 0.0 after filtering out trace values, find the sum
                elif np.sum(sn_filtered) > 0.0:
                    return np.round(np.sum(sn_filtered), 1)

        # ----------------------------------------------------
        # this will find the single calendar day max
        if key in 'daily_max_snow':
            sn_filtered = [s for s in snow if s != 'T']  # filter out trace values

            # if the two lists are equal after filtering, then we filtered no T's
            if len(snow) == len(sn_filtered):
                return np.max(sn_filtered)

            # if the two lists are not equal after filtering, then we filtered T's out...
            elif len(snow) != len(sn_filtered):
                # if the max is still 0.0 after filtering out trace values, then max was trace 'T'
                if np.max(sn_filtered) == 0.0:
                    return 'T'

                # if the max is greater than 0.0,
                elif np.max(sn_filtered) > 0.0:
                    return np.max(sn_filtered)

        # ----------------------------------------------------
        # this will find the max snow depth
        if key in 'snow_depth':
            # should not be a problem with snow depth, since it is in whole inches...
            sn_filtered = [s for s in snow if s != 'T']  # filter out trace values
            return np.max(sn_filtered)

    # -------------------------------------------
    # get the annual snowfall total
    yrly_sn = _snow_total(snow, key = 'annual_snow')

    # -------------------------------------------
    # get the annual snow dfn, and format accordingly
    yrly_sn_dfn = np.round(np.sum(snow_dfn), 1)
    if yrly_sn_dfn > 0.0:
        yrly_sn_dfn = f'+{yrly_sn_dfn}'

    elif yrly_sn_dfn == 0.0:
        yrly_sn_dfn = ' 0.0'

    # -------------------------------------------------
    # get the annual max calendar day snow
    sn_clndr_mx = _snow_total(cf6_mx_24hr_snow, key = 'daily_max_snow')


    # -------------------------------------------
    # get the max calendar day snow value dates, which will also be the 24 hr max value...
    def _find_annual_extreme_dates(snow_dates_lst, idx):

        '''
        This function will parse a snow dates list with a given index and return a string of the extreme dates

        Parameters
        ----------
        temp_dates_lst : list, a list of the extreme snow dates for a month
        idx : int, the index value of the extreme snow date for the year

        Returns
        ----------
        dt : str, a string of the formatted extreme snow dates

        '''

        if isinstance(snow_dates_lst[idx], list):
            # sort the days in ascending order
            dt = sorted([int(i.split('/')[-1]) for i in snow_dates_lst[idx]])
            dt = '/'.join(f'{d:02}' for d in dt)
        else:
            dt = snow_dates_lst[idx].split('/')[-1]
        return dt
    # -------------------------------------------
    def _find_annual_extreme_sd_dates(temp_dates_lst, idx):

        '''
        This function will parse a temp dates list with a given index and return a string of the extreme dates

        Parameters
        ----------
        temp_dates_lst : list, a list of the extreme temperature dates for a month
        idx : int, the index value of the extreme temperature date for the year

        Returns
        ----------
        dt : str, a string of the formatted extreme temp dates

        '''

        if isinstance(temp_dates_lst[idx], list):
            # sort the days in ascending order
            dt = sorted([int(i.split('/')[-1]) for i in temp_dates_lst[idx]])
            # check for consecutive days here and format accordingly
            consec_days, nonconsec_days = consecutive(dt)

            if consec_days:
                dt = sorted(consec_days + nonconsec_days)  # add the two lists back together and sort them
            dt = ','.join(f'{d}' for d in dt)  # join all items in dt into one string

        else:
            dt = temp_dates_lst[idx].split('/')[-1]
        return dt
    # -------------------------------------------------
    # First check if multiple instances...
    if cf6_mx_24hr_snow.count(sn_clndr_mx) > 1:
        # index values of all occurrences
        idx = [i for i, x in enumerate(cf6_mx_24hr_snow) if x == sn_clndr_mx]

        # temporary list
        _dt_lst = []
        for i in idx:
            _dt_lst.append(_find_annual_extreme_dates(cf6_mx_24hr_snow_dt, i))
            #_dt_lst.append(_find_annual_extreme_dates(_dt_lst, i))

        sn_clndr_mx = [f'{calendar.month_name[i + 1][:3].upper()} {dt}' for i, dt in zip(idx, _dt_lst)]
        sn_clndr_mx = '/'.join([i for i in sn_clndr_mx])

    # -------------------------------------------------
    # if there is only one instance of the annual max calendar day snow total
    else:
        idx = cf6_mx_24hr_snow.index(sn_clndr_mx) # index value of the annual max calendar day snowfall

        # if the annual calendar day max snow value is > 0.0, and not trace
        if not isinstance(sn_clndr_mx, str) and sn_clndr_mx != 0.0: # we have to include exception for string 'T'
            dt = cf6_mx_24hr_snow_dt[idx].split('/')[-1]
            sn_clndr_mx = f'{sn_clndr_mx:.1f}/{calendar.month_name[idx + 1][:3].upper()} {dt}'

        # for when max calendar day snow is 0.0
        elif not isinstance(sn_clndr_mx, str) and sn_clndr_mx == 0.0: # we have to include exception for string 'T'
            sn_clndr_mx = '0.0'

        # for when max calendar day snow is 'T'
        elif isinstance(sn_clndr_mx, str): # this should handle Trace 'T' values
            dt = cf6_mx_24hr_snow_dt[idx].split('/')[-1]
            sn_clndr_mx = f'{sn_clndr_mx:.1f}/{calendar.month_name[idx + 1][:3].upper()} {dt}'


    # -------------------------------------------------
    # get the annual max snow depth and the date
    yrly_mx_sd = np.max(cf6_mx_sdepth)

    if yrly_mx_sd > 0:
        idx = cf6_mx_sdepth.index(yrly_mx_sd)

        yrly_mx_sd_dt = _find_annual_extreme_sd_dates(cf6_mx_sdepth_dt, idx)
        yrly_mx_sd = f'{int(yrly_mx_sd)}/{calendar.month_name[idx + 1][:3].upper()} {yrly_mx_sd_dt}'

    # if yearly max snow depth is just 0, then set to string '0'
    else:
        yrly_mx_sd = '0'


    # -------------------------------------------------
    # Now lets write the annual summary data to the table
    f.write(table_sep)
    arr = ['ANN', f'{yrly_sn}', f'{yrly_sn_dfn}', f'{sn_clndr_mx}', f'{sn_clndr_mx}', f'{yrly_mx_sd}']
    f.write('{:8}{:9}{:11}{:23}{:18}{:18}\n'.format(*arr))

# ----------------------------------------------------------
def _make_misc_data(data_dict, f):

    '''

    Parameters
    ----------
    data_dict : dict, the main dictionary that includes all our main CLM product data
    f : file, the text file that is currently open that is being written to

    Returns
    -------
    None

    '''

    print('Writing in Annual Miscellaneous Data...\n...')

    # lets assemble some data
    n1 = data_dict['misc_temp_data']['n days - minT <= 32']
    n2 = data_dict['misc_temp_data']['n days - minT <= 20']
    n3 = data_dict['misc_temp_data']['n days - minT <= 0']
    n4 = data_dict['misc_temp_data']['n days - maxT <= 32']
    n5 = data_dict['misc_temp_data']['n days - maxT >= 90']
    n6 = data_dict['misc_temp_data']['n days - maxT >= 100']
    n7 = data_dict['misc_temp_data']['n days - maxT >= 105']
    n8 = data_dict['misc_temp_data']['n days - maxT >= 110']

    # first/last dates
    first_freeze = data_dict['misc_temp_data']['FIRST FREEZE']
    last_freeze = data_dict['misc_temp_data']['LAST FREEZE']

    first_80F = data_dict['misc_temp_data']['FIRST 80F']
    last_80F = data_dict['misc_temp_data']['LAST 80F']

    first_90F = data_dict['misc_temp_data']['FIRST 90F']
    last_90F = data_dict['misc_temp_data']['LAST 90F']

    first_100F = data_dict['misc_temp_data']['FIRST 100F']
    last_100F = data_dict['misc_temp_data']['LAST 100F']

    # Now lets write in the data
    f.write(f'\n\n{table_sep}')
    f.write('MISCELLANEOUS DATA (FIRST/LAST DATES, ETC.)\n')
    f.write(table_sep)

    # add in the misc data
    f.write(f'DAYS WITH MINIMUMS AT OR BELOW 32 DEGREES.........................{n1:02}\n')
    f.write(f'DAYS WITH MINIMUMS AT OR BELOW 20 DEGREES.........................{n2:02}\n')
    f.write(f'DAYS WITH MINIMUMS AT OR BELOW 0 DEGREES..........................{n3:02}\n')
    f.write(f'DAYS WITH MAXIMUMS AT OR BELOW 32 DEGREES.........................{n4:02}\n')
    f.write(f'DAYS WITH MAXIMUMS AT OR ABOVE 90 DEGREES.........................{n5:02}\n')
    f.write(f'DAYS WITH MAXIMUMS AT OR ABOVE 100 DEGREES........................{n6:02}\n')
    f.write(f'DAYS WITH MAXIMUMS AT OR ABOVE 105 DEGREES........................{n7:02}\n')
    f.write(f'DAYS WITH MAXIMUMS AT OR ABOVE 110 DEGREES........................{n8:02}\n')
    f.write(f'LAST FREEZE.......................................................{last_freeze}\n')
    f.write(f'FIRST 80-DEGREE DAY...............................................{first_80F}\n')
    f.write(f'FIRST 90-DEGREE DAY...............................................{first_90F}\n')
    f.write(f'FIRST 100-DEGREE DAY..............................................{first_100F}\n')
    f.write(f'LAST 100-DEGREE DAY...............................................{last_100F}\n')
    f.write(f'LAST 90-DEGREE DAY................................................{last_90F}\n')
    f.write(f'LAST 80-DEGREE DAY................................................{last_80F}\n')
    f.write(f'FIRST FREEZE......................................................{first_freeze}\n')

# ----------------------------------------------------------
def _make_records_table(f, pil, year):

    '''

    This function will compile all the new daily records for a climate site, which are stored in
    './records' and format them into a new table on the text file.

    Parameters
    ----------
    f : file, the text file that is currently open that is being written to
    pil : str, the climate site that is being parsed, e.g. 'CLMLZK'
    year : int, the year of climate data that is being parsed, YYYY

    Returns
    -------
    None

    '''

    print('Writing in New Daily Records Data...\n...')

    # first, create the record data dictionary
    records_dict = assemble_records(pil, year)

    # if records_dict is None, then we are running for a not current year, or don't have the correct record data saved in ./records
    # write into f that no record data is available
    if records_dict is None:
        f.write(f'\n\n{table_sep}NEW DAILY RECORDS\n{table_sep}')
        f.write(f'\nDAILY RECORD DATA {year} IS NOT AVAILABLE, OR THERE IS NOT MATCHING DAILY RECORD DATA IN ./records FOR {year}.\n')

    # if records_dict was successfully created, then process and write records data to f
    elif records_dict is not None:
        new_records = records_dict['new_recs'] # new record info
        prev_records = records_dict['old_recs'] # previous record info

        # declare the constant header for temperature records
        temp_rec_header = ['DATE', 'RECORD TYPE', 'NEW RECORD', 'OLD RECORD']


        # -----------------------------------------------------------
        def _write_temp_records(key1, key2, title):

            '''

            Parameters
            ----------
            key1 : str, a key value for the new_records and prev_records dictionaries,
                        will fall under 'rec_mx' or 'rec_lw'
            key2 : str, a key value for the new_records and prev_records dictionaries,
                        will fall under 'rec_lwmx' or 'rec_mxlw'
            title : str, the title header for what record category is being generated,
                        i.e., 'RECORD HIGHS' or 'RECORD LOWS'

            Returns
            -------
            None
            '''

            # the new record highs/lows
            new_recs = [int(item.split(',')[1].split(' ')[0]) for item in new_records[key1]]
            new_recs = [f'{rec} IN {year}' for rec in new_recs]
            # the date of the new daily record
            new_rec_dt = [item.split(',')[0] for item in new_records[key1]]
            # the record type
            rec_type1 = [item.split(',')[-1] for item in new_records[key1]]

            # the new record low-highs or high-lows
            new_rec_opp = [int(item.split(',')[1].split(' ')[0]) for item in new_records[key2]]
            new_rec_opp = [f'{rec} IN {year}' for rec in new_rec_opp]
            # the date of the new daily record
            new_rec_opp_dt = [item.split(',')[0] for item in new_records[key2]]
            # the record type
            rec_type2 = [item.split(',')[-1] for item in new_records[key2]]

            # previous record data
            # the previous record data from the dictionary, includes value and year(s) as list of lists
            prev_rec_data = prev_records[key1]

            # the previous record values
            prev_rec = [rec[0] for rec in prev_records[key1]]

            # now get the previous record year(s) sorted
            prev_yrs = []
            for item in prev_rec_data:
                if len(item) > 2:
                    item = item[1:]
                    item = [str(item) for item in item]
                    prev_yrs.append(', '.join(item))

                elif len(item) == 2:
                    prev_yrs.append(str(item[1]))

            # our final list of formatted strings for previous value and year(s)
            prev_recs1 = []

            for rec, dt in zip(prev_rec, prev_yrs):
                if len(dt) > 1:
                    dt = ''.join(dt)
                    prev_recs1.append(f'{rec} IN {dt}')

                else:
                    prev_recs1.append(f'{rec} IN {dt}')

            # record low highs
            # because there is no easy way to get previous low-high or high-low data,
            # simply append a Note statement for every extra low-high/high-low record
            prev_recs2 = [f'|*CHECK {pil[3:]} CLIMATE BOOK OR RER FOR PREVIOUS RECORD/YEAR(S)*|' for i in new_rec_opp]

            # combine the high and low-high data
            new_recs = new_recs + new_rec_opp
            new_rec_dt = new_rec_dt + new_rec_opp_dt
            rec_type = rec_type1 + rec_type2
            prev_recs = prev_recs1 + prev_recs2

            # now sort the data
            new_rec_dt, rec_type, new_recs, prev_recs = zip(*sorted(zip(new_rec_dt, rec_type, new_recs, prev_recs)))

            if new_recs:
                f.write(f'\n{title}\n')
                f.write('{:8}{:32}{:18}{:18}\n'.format(*temp_rec_header))

                # for loop begins
                for dt, type, r, prev_r in zip(new_rec_dt, rec_type, new_recs, prev_recs):
                    arr = [f'{dt}', f'{type}', f'{r}', f'{prev_r}']
                    f.write('{:8}{:32}{:18}{:18}\n'.format(*arr))
        # -----------------------------------------------------------

        f.write(f'\n\n{table_sep}NEW DAILY RECORDS\n{table_sep}')

        # write in record highs and low-highs
        _write_temp_records('rec_mx', 'rec_lwmx', '.RECORD HIGHS...')

        # write in record lows and high-lows
        _write_temp_records('rec_lw', 'rec_mxlw', '.RECORD LOWS...')

        # -----------------------------------------------------------
        # now lets write in precip records
        # declare the constant header for temperature records
        pcp_rec_header = ['DATE', 'NEW RECORD', 'OLD RECORD']

        # -----------------------------------------------------------
        def _write_precip_records(key1, title):

            '''

            Parameters
            ----------
            key1 : str, a key value for the new_records and prev_records dictionaries,
                        will fall under 'rec_pcp' or 'rec_sn'
            title : str, the title header for what record category is being generated,
                        i.e., 'RECORD RAINFALL' or 'RECORD SNOWFALL'

            Returns
            -------
            None
            '''

            # the new record rainfall or snowfall

            if key1 in 'rec_pcp':
                new_recs = [float(item.split(',')[-1].split(' ')[0]) for item in new_records[key1]]
                new_recs = [f'{rec:.2f} IN {year}' for rec in new_recs]

            elif key1 in 'rec_sn':
                new_recs = [item.split(',')[-1].split(' ')[0] for item in new_records[key1]]
                new_recs = [f'{float(item):.1f}' if item != 'T' else item for item in new_recs]  # handle trace values 'T'
                new_recs = [f'{rec} IN {year}' for rec in new_recs]

            # the date of the new daily record
            new_rec_dt = [item.split(',')[0] for item in new_records[key1]]

            # previous record data
            # the previous record data from the dictionary, includes value and year(s) as list of lists
            prev_rec_data = prev_records[key1]

            # the previous record values
            prev_rec = [rec[0] for rec in prev_records[key1]]

            # now get the previous record year(s) sorted
            prev_yrs = []
            for item in prev_rec_data:
                # means there are multiple previous years that share the previous daily record
                if len(item) > 2:
                    item = item[1:]
                    item = [str(item) for item in item]
                    prev_yrs.append(', '.join(item))

                # only one year for the previous daily record
                elif len(item) == 2:
                    prev_yrs.append(str(item[1]))

                # happens for daily snow in the summer sometimes, where new daily record is 'T' for hail
                # simply add in note about no previous daily record
                elif item[0] == 0.0:
                    prev_yrs.append('NO PREV DAILY RECORD')

            # our final list of formatted strings for previous value and year(s)
            prev_recs = []

            for rec, dt in zip(prev_rec, prev_yrs):

                # multiple previous years, e.g. '2012, 2014, ...'
                if ',' in dt:
                    prev_recs.append(f'{rec} IN {dt}')

                # no previous daily record, mainly for snow in summer (hail)
                elif dt == 'NO PREV DAILY RECORD':
                    prev_recs.append(dt)

                else:
                    prev_recs.append(f'{rec} IN {dt}')


            # now lets write in the new record data
            if new_recs:
                f.write(f'\n{title}\n')
                f.write('{:8}{:18}{:18}\n'.format(*pcp_rec_header))

                # for loop begins
                for dt, r, prev_r in zip(new_rec_dt, new_recs, prev_recs):
                    arr = [f'{dt}', f'{r}', f'{prev_r}']
                    f.write('{:8}{:18}{:18}\n'.format(*arr))
                    # for loop ends
        # -----------------------------------------------------------

        # write in the record rainfall
        _write_precip_records('rec_pcp', '.RECORD RAINFALL...')

        # write in the record snow
        _write_precip_records('rec_sn', '.RECORD SNOWFALL...')





# ##################################################################
# From dict_constructor.py
# ##################################################################
def _find_numeric_suffix(myDate):
    '''
    This function will take a string date, formatted as 'nn', e.g. '05', and assign a suffix based on the number.

    Parameters
    ----------
    myDate : str, a date number, formatted as 'nn', e.g. '05'

    Returns
    ----------
    myDate : str, formatted as 'nnTH', 'nnST', 'nnND', 'nnRD'

    '''

    date_suffix = ["TH", "ST", "ND", "RD"]

    if int(myDate) % 10 in [1, 2, 3] and int(myDate) not in [11, 12, 13]:
        return f'{myDate}{date_suffix[int(myDate) % 10]}'
    else:
        return f'{myDate}{date_suffix[0]}'

# ----------------------------------------------------------
def _get_clm_product_links(pil, months, y):
    '''
    A function that will query the IEM API for CLM products, and return the corresponding API links to those products

    Parameters
    ----------
    pil : str, 6 character pil for CLM products, e.g., CLMLIT
    months : list, a list of months as integers, for which to find urls for
    y : int, year in YYYY format

    Returns
    ----------
    api_links : list, a list of API link strings

    '''

    print(f'\nSearching for {y} {pil} products...')
    print('*NOTE*, CLM products for any given month are issued in the following month...\n')

    # y = datetime.now().year
    # months = [x for x in range(13)][2:]  # start in feb for jan CLM product
    days = [x for x in range(15)][1:]

    # empty list to store api_links for each monthly product
    api_links = []

    for m in months:
        print(f'---{calendar.month_name[m - 1][:3]} {y}---')
        for d in days:

            # this is the api link that returns a json file with info, either including product issuance info, or little info, in json format
            # url = 'https://mesonet.agron.iastate.edu/api/1/nws/afos/list.json?cccc=KLZK&pil=CLMLIT&date=2023-02-04'
            url = f'https://mesonet.agron.iastate.edu/api/1/nws/afos/list.json?cccc=KLZK&pil={pil}&date={y}-{m:02}-{d:02}'

            # A GET request to the API
            response = requests.get(url)

            # turn the request into a json, and subsequently a dictionary
            response = response.json()

            if not response['data']:
                print(f'   -No {pil} product issued on {m:02}/{d:02}/{y}')
                continue

            elif response['data']:
                print(f'   -{pil} Product issued on {m:02}/{d:02}/{y}\n')
                api_links.append(response['data'][0]['text_link'])
                break

    # ------------------------------------------------------------
    # Check if we need December
    # small section to do dec, because the dec CLM product is issued in jan of the following year,
    # but only run for non current year settings, if it is the current year,
    # then its possible we are not there yet
    if y != datetime.now().year:

        m = 1
        print(f'---{calendar.month_name[m][:3]} {y}---')
        for d in days:

            url = f'https://mesonet.agron.iastate.edu/api/1/nws/afos/list.json?cccc=KLZK&pil={pil}&date={y + 1}-{m:02}-{d:02}'

            # A GET request to the API
            response = requests.get(url)

            # turn the request into a json, and subsequently a dictionary
            response = response.json()

            if not response['data']:
                print(f'   -No {pil} product issued on {m:02}/{d:02}/{y}')
                continue

            elif response['data']:
                print(f'   -{pil} Product issued on {m:02}/{d:02}/{y}\n')
                api_links.append(response['data'][0]['text_link'])
                break

    print(f'Finished searching... Found {len(api_links)} {y} {pil} products...\n')
    return api_links

# ----------------------------------------------------------
def _get_cf6_product_links(pil, months, y):
    '''
    A function that will query the IEM API for CF6 products, and return the corresponding API links to those products

    Parameters
    ----------
    pil : str, 6 character pil for CLM products, e.g., CLMLIT
    months : list, a list of months as integers, for which to find urls for
    y : int, year in YYYY format

    Returns
    ----------
    api_links : list, a list of API link strings

    '''

    # ------------------------------------------------------------
    def _check_cf6_sheet(url):
        '''
        This function will parse the lines in a CF6 sheet from the IEM API
        text page, and check if it is the final issuance for the previous month.

        Parameters
        ----------
        url : str, the url to the IEM text API for a product

        Returns
        -------
        bool : True if conditions are met, False if conditions are not met

        '''

        html = urlopen(url).read()
        soup = BeautifulSoup(html, features="html.parser")

        # kill all script and style elements
        for script in soup(["script", "style"]):
            script.extract()  # rip it out

        # get text
        text = soup.get_text()

        # this will be the last line of the text
        rmk = text.splitlines()[-1]

        if '#FINAL' in rmk:
            return True
        else:
            return False

    # ------------------------------------------------------------

    print(f'\nSearching for {y} CF6{pil[3:]} Sheets...')
    print('*NOTE*, Final CF6 sheet for any given month are issued on the first of the following month...\n')

    # empty list to store api_links for each monthly product
    api_links = []

    for m in months:
        print(f'---{calendar.month_name[m - 1][:3]} {y}---')
        # this is the api link that returns a json file with info, either including product issuance info, or little info, in json format
        url = f'https://mesonet.agron.iastate.edu/api/1/nws/afos/list.json?cccc=KLZK&pil=CF6&date={y}-{m:02}-01'

        # A GET request to the API
        response = requests.get(url)

        # turn the request into a json, and subsequently a dictionary
        response = response.json()

        # for loop begins
        for item in response['data']:
            if pil[3:] in item['pil']:

                # we need to check if the sheet says FINAL in the remarks...
                # pose sub url request here for the text link of the product
                _url = item['text_link']  # link to the api text page

                # check if this is the final issuance of the previous month's CF6
                if _check_cf6_sheet(_url):
                    api_links.append(_url) # add to the list if this final CF6
                    break
                else:
                    continue
                # for loop ends

    # ------------------------------------------------------------
    # Check if we need December
    # small section to do dec, because the dec CLM product is issued in jan of the following year,
    # but only run for non current year settings, if it is the current year,
    # then its possible we are not there yet
    if y != datetime.now().year:

        m = 1
        print(f'---{calendar.month_name[12][:3]} {y}---')
        url = f'https://mesonet.agron.iastate.edu/api/1/nws/afos/list.json?cccc=KLZK&pil=CF6&date={y + 1}-{m:02}-01'

        # A GET request to the API
        response = requests.get(url)

        # turn the request into a json, and subsequently a dictionary
        response = response.json()

        # for loop begins
        for item in response['data']:
            if pil[3:] in item['pil']:

                # we need to check if the sheet says FINAL in the remarks...
                # pose sub url request here for the text link of the product
                _url = item['text_link']  # link to the api text page

                # check if this is the final issuance of the previous month's CF6
                if _check_cf6_sheet(_url):
                    api_links.append(_url) # add to the list if this final CF6
                    break
                else:
                    continue
                # for loop ends

    print(f'Finished searching... Found {len(api_links)} {y} CF6{pil[3:]} Sheets...\n')
    return api_links

# ----------------------------------------------------------
def _parse_timestamp(url):
    '''
    This function will take a url string, and create a datetime object from the timestamp in the url.

    Parameters
    ----------
    url : str, the API url with the timestamp in the url

    Returns
    ----------
    timestamp : a datetime object with the url timestamp
    station : str, the three letter station ID

    '''

    timestamp = url.split('nwstext/')[-1].split('-KLZK')[0]

    y = int(timestamp[:4])
    m = int(timestamp[4:6])
    # if the month of the timestamp is january, then this is the dec product
    if m == 1:
        m = 12
    # otherwise, apply the standard correction of one month subtracted
    else:
        m = m - 1
    d = int(timestamp[6:8])
    hh = int(timestamp[8:10])
    mm = int(timestamp[10:])

    station = url.split('CLM')[-1]

    return datetime(y, m, d, hh, mm), station

# ----------------------------------------------------------
def _parse_clm_text(url):
    '''
    This function will parse the API url text for climate data, and return the unfiltered text info to keys in a dictionary.

    Parameters
    ----------
    url : str, the API url

    Returns
    ----------
    text_dict : dictionary, the dictionary with all unfiltered climate text info

    '''

    html = urlopen(url).read()
    soup = BeautifulSoup(html, features="html.parser")

    # kill all script and style elements
    for script in soup(["script", "style"]):
        script.extract()  # rip it out

    # get text
    text = soup.get_text()

    # parse out the highest and lowest temp data for the month from the text
    high_temp_data = text.split('HIGHEST         ')[-1].split('LOWEST')[0]
    low_temp_data = text.split('LOWEST           ')[-1].split('AVG. MAXIMUM')[0]

    # parse out the avg high and low temp data, and the avg monthly temp data
    avg_high_temp_data = text.split('AVG. MAXIMUM  ')[-1].split('\nAVG. MINIMUM  ')[0]
    avg_low_temp_data = text.split('AVG. MINIMUM  ')[-1].split('\nMEAN')[0]
    avg_monthly_data = text.split('MEAN  ')[-1].split('\nDAYS MAX >= 90')[0]

    # parse out the precip and snow data
    monthly_precip_data = text.split('SNOWFALL (INCHES)')[0].split('PRECIPITATION (INCHES)')[-1].split('\nTOTALS')[-1]
    monthly_snow_data = text.split('SNOWFALL (INCHES)')[-1].split('\nTOTALS')[-1].split('\nDEGREE DAYS')[0]

    # misc temp data
    misc_temp_data = text.split('\nTEMPERATURE (F)\n')[-1].split('\n\nPRECIPITATION (INCHES)')[0]

    values = [high_temp_data, low_temp_data,
                avg_high_temp_data, avg_low_temp_data, avg_monthly_data,
                monthly_precip_data, monthly_snow_data,
                misc_temp_data]

    keys = ['high_temp_data_text', 'low_temp_data_text',
            'avg_high_temp_data_text', 'avg_low_temp_data_text', 'avg_monthly_data_text',
            'monthly_precip_data_text', 'monthly_snow_data_text',
            'misc_temp_data']

    text_dict = dict(zip(keys, values))

    return text_dict

# ----------------------------------------------------------
def _get_temp_data(data_dict, high_temp_data, low_temp_data,
                   avg_high_temp_data, avg_low_temp_data,
                   avg_monthly_data, month):
    '''
    Worker function to extract high and low data/dates from the CLM products.

    Parameters
    ----------
    data_dict : dict, the main dictionary that includes all our main CLM product data
    high_temp_data : str, a parsed string that contains the monthly high temp data from the CLM
    low_temp_data : str, a parsed string that contains the monthly low temp data from the CLM
    avg_high_temp_data : str, a parsed string that contains the avg monthly high temp data from the CLM
    avg_low_temp_data : str, a parsed string that contains the avg monthly low temp data from the CLM
    avg_monthly_data : str, a parsed string that contains the avg monthly mean temp data from the CLM
    (no longer needed) station : str, the three character station ID for each climate site
    month : int, the numerical month number, can be obtained from the timestamp variable

    Returns
    ----------
    None

    '''

    # Helper functions
    # --------------------------------------------------------
    def _count_spaces(line):

        # this should be the date in the line
        split_date = list(filter(None, (line.split(' '))))[0]

        # this is the number of spaces for it to be a date occurred that month
        if line.split(split_date)[0].count(' ') < 30:
            return True

        else:
            return False

    # --------------------------------------------------------

    month = calendar.month_name[month][:3].upper()

    # first filter the line of temp data
    filtered_low_temp_data = list(filter(None, low_temp_data.split(' ')))  # low temp data
    filtered_high_temp_data = list(filter(None, high_temp_data.split(' ')))  # high temp data

    # lets add the high and low values to the dictionary
    data_dict[month]['monthly_max_min'].append(int(filtered_high_temp_data[0]))
    data_dict[month]['monthly_max_min'].append(int(filtered_low_temp_data[0]))

    # now add the monthly avg high, low, and avg temps, and dfn
    data_dict[month]['monthly_avg_temps_dfn'].append(
        float(list(filter(None, (avg_high_temp_data.split(' '))))[0]))  # monthly avg high
    data_dict[month]['monthly_avg_temps_dfn'].append(
        float(list(filter(None, (avg_low_temp_data.split(' '))))[0]))  # monthly avg low
    data_dict[month]['monthly_avg_temps_dfn'].append(
        float(list(filter(None, (avg_monthly_data.split(' '))))[0]))  # monthly avg mean temp
    data_dict[month]['monthly_avg_temps_dfn'].append(
        float(list(filter(None, (avg_monthly_data.split(' '))))[2]))  # monthly avg mean temp dfn

    ## LOW TEMP DATA DATES ##
    # ---------------------------------------------------------------
    # check how many lines exist in the list
    # if only one line, then just one date to grab
    if len(low_temp_data.splitlines()) == 1:
        data_dict[month].update({'low_dates': str(filtered_low_temp_data[1])})

    # if there are multiple lines of low temp data, then check if these are the dates (occurrence dates, not records) that we want
    elif len(low_temp_data.splitlines()) > 1 and not _count_spaces(low_temp_data.splitlines()[1]):
        data_dict[month].update({'low_dates': str(filtered_low_temp_data[1])})

    # if there are multiple rows of low temp data, then multiple dates to grab that are valid
    else:
        # grab the first date in the line
        data_dict[month].update({'low_dates': [filtered_low_temp_data[1]]})

        # now iterate and find the rest of the dates
        for line in low_temp_data.splitlines()[1:]:
            if _count_spaces(line):
                data_dict[month]['low_dates'].append(list(filter(None, (line.split(' '))))[0])

    ## HIGH TEMP DATA DATES ##
    # ---------------------------------------------------------------
    # check how many lines exist in the list
    # if only one line, then just one date to grab
    if len(high_temp_data.splitlines()) == 1:
        data_dict[month].update({'high_dates': str(filtered_high_temp_data[1])})

    # if there are multiple lines of high temp data, then check if these are the dates (occurrence dates, not records) that we want
    elif len(high_temp_data.splitlines()) > 1 and not _count_spaces(high_temp_data.splitlines()[1]):
        data_dict[month].update({'high_dates': str(filtered_high_temp_data[1])})

    # if there are multiple rows of high temp data, then multiple dates to grab
    else:

        # grab the first date in the line
        data_dict[month].update({'high_dates': [filtered_high_temp_data[1]]})

        # now iterate and find the rest of the dates
        for line in high_temp_data.splitlines()[1:]:
            if _count_spaces(line):
                data_dict[month]['high_dates'].append(list(filter(None, (line.split(' '))))[0])

# ----------------------------------------------------------
def _get_precip_data(data_dict, monthly_precip_data, month):
    '''
    This function will parse the precip text info from the API url, and will append precip data
    to the main working dictionary.

    Parameters
    ----------
    data_dict : dict, the main dictionary that includes all our main CLM product data
    monthly_precip_data : str, the parsed monthly precip data as a string
    month : int, the month as an integer

    Returns
    ----------
    None

    '''

    # Helper Sub-function of the main function
    # --------------------------------------------------------

    def _check_record(param):
        if 'R' in str(param):
            return param.split('R')[0]
        else:
            return param

    # --------------------------------------------------------

    # convert the integer month to the three letter month name
    month = calendar.month_name[month][:3].upper()

    # ---- Monthly Rain Data -----
    monthly_total_rain = _check_record(list(filter(None, (monthly_precip_data.split(' '))))[0])  # monthly total rain
    monthly_rain_dfn = _check_record(list(filter(None, (monthly_precip_data.split(' '))))[2])

    # add the monthly precip total and dfn
    data_dict[month]['monthly_rain_and_dfn'].append(float(monthly_total_rain))  # monthly total
    data_dict[month]['monthly_rain_and_dfn'].append(float(monthly_rain_dfn))  # monthly dfn

    # now lets get the calendar max, 24 hr max values and dates for rain
    # Note: On the CLM,
    # the greatest 24 hr total is the true 24 hr total(can overlap days)
    # the greatest storm total is the actual calendar day max
    for idx, line in enumerate(monthly_precip_data.split('\nGREATEST\n')[-1].splitlines()):
        # print(list(filter(None, (line.split(' ')))))
        # the first line is the true 24 hr total (can overlap days)
        if idx == 0:
            max_24hr_rain = float(list(filter(None, (line.split(' '))))[3])
            max_24hr_rain_dates = " ".join(list(filter(None, (line.split(' '))))[4:])

        # this is the true calendar day max (reads as storm total on the CLM)
        elif idx == 1:
            max_clndr_day_rain = float(list(filter(None, (line.split(' '))))[-1])

    # check for a different max storm total rainfall date
    lst = monthly_precip_data.split('\nGREATEST\n')[-1][:-2].splitlines()
    empty_date = '(MM/DD(HH))'  # string we are searching for that designates no storm total date i.e. it equals the 24 hr total

    # if we find the empty_date search string, then add the 24 hr total dates to storm total dates
    if any(empty_date in x for x in lst):
        # max_stormtotal_rain_dates = " ".join(list(filter(None, (monthly_precip_data.split('\nGREATEST\n')[-1][:-2].splitlines()[0].split(' '))))[4:])
        max_clndr_day_rain_dates = max_24hr_rain_dates

    # if we do not find the empty_date search string, then handle adding a potentially different storm total date
    else:
        max_clndr_day_rain_dates = " ".join(
            list(filter(None, (monthly_precip_data.split('\nGREATEST\n')[-1][:-2].splitlines()[1].split(' '))))[3:])

    # 24 hour (calendar day max) rainfall data
    data_dict[month]['max_clndr_24hr_rain'].append(max_24hr_rain)  # 24 hr max (can overlap dates)
    data_dict[month]['max_clndr_24hr_rain'].append(max_24hr_rain_dates)  # 24 hr max dates

    data_dict[month]['max_clndr_24hr_rain'].append(max_clndr_day_rain)  # max storm total (calendar day max)
    data_dict[month]['max_clndr_24hr_rain'].append(max_clndr_day_rain_dates)  # max storm total dates (set equal to 24 hr max for now)

# ----------------------------------------------------------
def _get_snow_data(pil, data_dict, monthly_snow_data, month):
    '''
    This function will parse the precip text info from the API url, and will append snow data
    to the main working dictionary.

    Parameters
    ----------
    data_dict : dict, the main dictionary that includes all our main CLM product data
    monthly_snow_data : str, the parsed monthly snow data as a string
    month : int, the month as an integer

    Returns
    ----------
    None

    '''

    # Helper Sub-functions of the main function
    # --------------------------------------------------------
    def _check_record(param):
        if 'R' in str(param):
            return param.split('R')[0]
        else:
            return param

    # --------------------------------------------------------
    def _check_missing_snow(param, month):
        try:
            data_dict[month]['monthly_snow_sdepth_dfn'].append(float(param))

        except ValueError:
            if 'T' in str(param):
                data_dict[month]['monthly_snow_sdepth_dfn'].append(param)
            else:
                data_dict[month]['monthly_snow_sdepth_dfn'].append(0.0)

    # --------------------------------------------------------

    # first, if month is not in a primary cool season month, then auto fill values and end the function call
    if month not in [1, 2, 3, 4, 10, 11, 12]:
        # convert the integer month to the three letter month name
        month = calendar.month_name[month][:3].upper()

        # this will add [0.0, 0.0, 0.0, np.nan]
        blank_values = [0.0, 0.0, 0.0, np.nan]
        data_dict[month].update({'monthly_snow_sdepth_dfn': blank_values})

        # this will add [0.0, np.nan]
        blank_values = [0.0, np.nan]
        data_dict[month].update({'max_clndr_24hr_snow': blank_values})

        return

    # if we do have cool season month, then proceed

    # convert the integer month to the three letter month name
    month = calendar.month_name[month][:3].upper()

    # ---- Monthly Snow Data ----
    # add the monthly snow total and dfn
    monthly_total_snow = _check_record(list(filter(None, (monthly_snow_data.split(' '))))[0])  # monthly total snow
    monthly_grtst_sdepth = _check_record(
        list(filter(None, (monthly_snow_data.split('\nGREATEST\n SNOW DEPTH')[-1].splitlines()[0].split(' '))))[0])  # monthly greatest snow depth
    monthly_snow_dfn = _check_record(list(filter(None, (monthly_snow_data.split(' '))))[2])  # monthly total snow dfn
    monthly_grtst_24hr_snow = _check_record(list(filter(None, (
        monthly_snow_data.split('\nGREATEST\n')[-1].split(' 24 HR TOTAL')[-1].splitlines()[0].split(' '))))[0])

    # Add the data to the main dictionary
    # for the non winter months, monthly snow may just be set to 'MM' in the CLM product
    _check_missing_snow(monthly_total_snow, month)  # this will add the monthly total snow

    # adjust departure from normal values for PBF or HRO, because the normals are not listed in the CLM...
    if pil[3:] in ['PBF', 'HRO']:
        # import the csv with pandas
        nrml_f = f'./normals/{pil[3:]}_Normals.csv'
        nrml_df = pd.read_csv(nrml_f)
        nrml_df = nrml_df.loc[nrml_df['MONTH'] == month].reset_index()
        #print(nrml_df)
        sn_nrml = nrml_df['SNOW'][0]
        #print(sn_nrml)


        # now lets calculate the dfn
        if monthly_total_snow not in ['MM', 'T']:
            sn_dfn = np.round(float(monthly_total_snow) - float(sn_nrml), 1)
            data_dict[month]['monthly_snow_sdepth_dfn'].append(sn_dfn)
        elif monthly_total_snow in ['T']:
            sn_dfn = np.round(0.0 - float(sn_nrml), 1)
            data_dict[month]['monthly_snow_sdepth_dfn'].append(sn_dfn)
        else:
            _check_missing_snow(monthly_snow_dfn, month)  # this will add the monthly total snow dfn
    else:
        _check_missing_snow(monthly_snow_dfn, month)  # this will add the monthly total snow dfn

    _check_missing_snow(monthly_grtst_sdepth, month)  # this will add the greatest monthly snow depth

    # ---- Greatest Snow Depth Dates ----
    # Now lets check for dates on greatest snow depth and 24 max total
    lst = monthly_snow_data.split('\nGREATEST\n')[-1].splitlines()

    # if monthly max snow depth is missing, set dates to nan
    if monthly_grtst_sdepth in ['0', 'MM']:
        # if monthly_grtst_sdepth == 'MM':
        monthly_grtst_sdepth_dates = np.nan

    # for greatest snow depth date (multiple dates)
    if any('/' in x for x in lst[0]) and len(lst) > 2:

        monthly_grtst_sdepth_dates = []

        # get the first date and make sure it is valid
        split_date = list(filter(None, (lst[0].split(' '))))[-1]
        if lst[0].split(split_date)[0].split(monthly_grtst_sdepth)[-1].count(' ') <= 3:
            monthly_grtst_sdepth_dates.append(split_date)

        # now iterate over the remainder of possible dates
        for idx, item in enumerate(
                lst[1:-1]):  # skip the first, and the last line because the last line is the 24 hr max data
            split_date = list(filter(None, (item.split(' '))))[0]
            if lst[idx].split(split_date).count(' ') <= 25:
                monthly_grtst_sdepth_dates.append(split_date)

    # for greatest snow depth date (only one date)
    elif any('/' in x for x in lst[0]) and len(lst) == 2:

        # get the first date and make sure it is valid
        split_date = list(filter(None, (lst[0].split(' '))))[-1]
        if lst[0].split(split_date)[0].split(monthly_grtst_sdepth)[-1].count(' ') <= 3:
            monthly_grtst_sdepth_dates = split_date
    else:
        monthly_grtst_sdepth_dates = np.nan

    # ---- Max 24 hr Snow Dates ----
    # lets follow similar logic here...
    # if monthly max 24 hr snow is missing, then set dates to nan
    if monthly_grtst_24hr_snow in ['0.0', 'MM']:
        # if monthly_grtst_24hr_snow == 'MM':
        monthly_grtst_24hr_snow_dates = np.nan

    # for greatest snow depth date
    elif any('/' in x for x in lst[-1]):

        # lets get the first occurrence of a date and use that to split
        first_date = list(filter(None, (lst[-1].split(monthly_grtst_24hr_snow)[-1].split('TO')[0].split(' '))))
        first_date = list(filter(lambda k: 'R' not in k,
                                 first_date))  # in case there is a record value for 24 hr snow, there will still be an 'R'

        # now lets split based on the first date and count the spaces
        #if lst[-1].split(first_date[0]).count(' ') <= 4:
        try:
            if lst[-1].split(first_date[0]).count(' ') <= 4:
                monthly_grtst_24hr_snow_dates = " ".join(
                list(filter(None, (monthly_snow_data.split('\nGREATEST\n')[-1].splitlines()[1].split(' '))))[4:7])
        except IndexError:
        #else:
            monthly_grtst_24hr_snow_dates = np.nan

    else:
        monthly_grtst_24hr_snow_dates = np.nan

    # now add the data to the main dictionary
    data_dict[month]['monthly_snow_sdepth_dfn'].append(
        monthly_grtst_sdepth_dates)  # add the greatest snow depth date(s) to the dictionary

    try:
        data_dict[month]['max_clndr_24hr_snow'].append(
            float(monthly_grtst_24hr_snow))  # add the greatest 24 hr snow total to the dictionary
    except ValueError:
        if monthly_grtst_24hr_snow in ['0.0', 'MM']:
            data_dict[month]['max_clndr_24hr_snow'].append(0.0)  # add the greatest 24 hr snow total to the dictionary
        elif monthly_grtst_24hr_snow in ['T']:
            data_dict[month]['max_clndr_24hr_snow'].append(
                'T')  # add trace for the greatest 24 hr snow total to the dictionary

    data_dict[month]['max_clndr_24hr_snow'].append(
        monthly_grtst_24hr_snow_dates)  # add the greatest 24 hr snow total dates to the dictionary

# ----------------------------------------------------------
def _get_cf6_snow_pres_data(pil, data_dict, url, idx):

    html = urlopen(url).read()
    soup = BeautifulSoup(html, features="html.parser")

    # kill all script and style elements
    for script in soup(["script", "style"]):
        script.extract()  # rip it out

    # get text
    text = soup.get_text()

    # parse out the snow info in the text
    cf6_snow_text = text.split('\nPRELIMINARY LOCAL CLIMATOLOGICAL DATA (WS FORM: F-6) , PAGE 2\n\n')[-1]
    cf6_snow_text = \
    cf6_snow_text.split('\n                        SNOW, ICE PELLETS, HAIL    4 = ICE PELLETS              \n')[-1]
    # new_text.splitlines()

    # get the max 24 hr snow text
    max24hr_sn = cf6_snow_text.split('GRTST 24HR')[-1].split('6 = FREEZING RAIN OR DRIZZLE')[0]
    max24hr_sn = list(filter(None, (max24hr_sn.split(' '))))

    # get the max snow depth text
    max_sd = cf6_snow_text.split('GRTST DEPTH:')[-1].split('7 = DUSTSTORM OR SANDSTORM:')[0]
    max_sd = list(filter(None, (max_sd.split(' '))))

    # establish our idx
    # idx = calendar.month_name[idx+1][:3].upper()

    # mini function for getting dates from the text
    def _find_date(lst, sd=False, sn=True):

        # this will take care of odd syntax issues that come up, e.g. 2, 1 on the dates when they span multiple dates
        if len(lst) > 3:
            _lst = lst[:2]
            lst_ = lst[2:]
            _lst.append("".join(lst_))
            lst = _lst

        dt = lst[2]  # should be index value 2, sometimes, 'nn-nn', or can be 'nn-', 'nn', if one day

        # ----------------------------
        # for max 24 hr snow
        if sn:
            # syntax for a single date range
            if '-' in dt:
                dt = dt.split('-')[0]
                dt = f'{(idx+1):02}/{dt}'
                data_dict['cf6_data'][calendar.month_name[idx + 1][:3].upper()]['max_clndr_24hr_snow'].append(dt)
                return
            else:
                return

        # ----------------------------
        # for max snow depth
        elif sd:
            if ',' in dt:  # for multiple days

                # split the string and sort if necessary
                days = sorted([int(dt.split(',')[0]), int(dt.split(',')[-1])])
                days = np.arange(days[0], days[1] + 1)

                days = [f'{(idx+1):02}/{d:02}' for d in days]  # convert the days to 'mm/dd'
                data_dict['cf6_data'][calendar.month_name[idx + 1][:3].upper()]['max_sdepth'].append(days)

            else:  # this should be single day stats
                # dt = dt.split('-')[0]
                dt = f'{(idx+1):02}/{int(dt):02}'
                data_dict['cf6_data'][calendar.month_name[idx + 1][:3].upper()]['max_sdepth'].append(dt)

                # ---------------------------------------------------

    # Max 24 hr snow values/date(s)
    # for when max 24 hr snow is trace
    if max24hr_sn[0] in 'T':
        data_dict['cf6_data'][calendar.month_name[idx + 1][:3].upper()]['max_clndr_24hr_snow'].append(max24hr_sn[0])
        # get the dates
        _find_date(max24hr_sn, sd=False, sn=True)

    # for missing data
    elif max24hr_sn[0] in 'M':
        blank_data = [np.nan, np.nan]  # for missing max 24 hr snow data, and no date
        data_dict['cf6_data'][calendar.month_name[idx + 1][:3].upper()].update({'max_clndr_24hr_snow': blank_data})

        # for when max 24 hr snow is 0.0, then no dates either...
    elif float(max24hr_sn[0]) == 0.0:
        blank_data = [0.0, np.nan]  # no max 24 hr snow data, and no date
        data_dict['cf6_data'][calendar.month_name[idx + 1][:3].upper()].update({'max_clndr_24hr_snow': blank_data})

        # for when max 24 hr snow is > 0.0
    elif float(max24hr_sn[0]) > 0.0:
        data_dict['cf6_data'][calendar.month_name[idx + 1][:3].upper()]['max_clndr_24hr_snow'].append(
            float(max24hr_sn[0]))
        _find_date(max24hr_sn, sd=False, sn=True)

    # ---------------------------------------------------
    # Greatest Snow Depth values/date(s)

    # for when max snow depth is marked as missing... not sure how much this will come up...
    if max_sd[0] in 'M':
        blank_data = [np.nan, np.nan]  # for missing max 24 hr snow data, and no date
        data_dict['cf6_data'][calendar.month_name[idx + 1][:3].upper()].update({'max_sdepth': blank_data})

    elif max_sd[0] in 'T':
        blank_data = [0, np.nan]  # for weird exceptions where snow depth was marked as trace... should not be accurate...
        data_dict['cf6_data'][calendar.month_name[idx + 1][:3].upper()].update({'max_sdepth': blank_data})

        # for when max snow depth is 0, then no dates either
    elif int(max_sd[0]) == 0:
        blank_data = [0, np.nan]  # for missing max 24 hr snow data, and no date
        data_dict['cf6_data'][calendar.month_name[idx + 1][:3].upper()].update({'max_sdepth': blank_data})

    elif int(max_sd[0]) > 0:
        data_dict['cf6_data'][calendar.month_name[idx + 1][:3].upper()]['max_sdepth'].append(int(max_sd[0]))
        _find_date(max_sd, sd=True, sn=False)


    # ----------------------------------------------------------
    # now lets get the max/min SLP data from the CF6

    # however if the pil is LZK, then pass because SLP data is not automatically added to the CF6
    if pil[3:] != 'LZK':

        # parse out the text from the CF6 that includes the monthly max/min SLP data
        max_slp_text = list(filter(None, (text.split('HIGHEST SLP')[-1].split('\n')[0].split(' '))))
        min_slp_text = list(filter(None, (text.split('LOWEST  SLP')[-1].split('\n')[0].split(' '))))

        # get the max SLP value and date for the month and add it to the dictionary
        try:
            max_slp = float(max_slp_text[0])
            max_slp_date = f'{calendar.month_name[idx + 1].upper()} {_find_numeric_suffix(max_slp_text[-1])}'
        except ValueError:
            max_slp = 'M'
            max_slp_date = 'M'

        data_dict['cf6_data']['pres']['max_pres'].append(max_slp)
        data_dict['cf6_data']['pres']['max_pres_dates'].append(max_slp_date)

        # get the min SLP value and date for the month
        try:
            min_slp = float(min_slp_text[0])
            min_slp_date = f'{calendar.month_name[idx + 1].upper()} {_find_numeric_suffix(min_slp_text[-1])}'
        except ValueError:
            min_slp = 'M'
            min_slp_date = 'M'

        data_dict['cf6_data']['pres']['min_pres'].append(min_slp)
        data_dict['cf6_data']['pres']['min_pres_dates'].append(min_slp_date)

# ----------------------------------------------------------
def _get_misc_temp_data(data_dict, pil, year): #misc_temp_data):

    '''
    This function will parse the misc temp days info from the API url, and will append
    the data to the main working dictionary.

    Parameters
    ----------
    data_dict : dict, the main dictionary that includes all our main CLM product data
    misc_temp_data : str, the parsed monthly misc temp data as a string

    Returns
    ----------
    None

    '''

    # ----------------------------------------------------------
    def _count_degree_days(r, key, t, gt=False):
        '''
        This function will utilize the IEM Daily Climate API, and parse
        for misc temp info such as days above/below n temp.

        Parameters
        ----------
        r : dict, the json dictionary from the IEM API
        key : str, the key value that is being queried in r
        t : int, the temperature that is being queried
        gt : bool, default is False, a boolean that determines the sign operator

        Returns
        ----------
        n : int, the number of days of a threshold that is queried

        '''

        n = 0
        for item in r['results']:

            if str(item[key]) in 'M':
                n += 0 # don't add a day if the value is missing

            else:
                if gt:
                    if int(item[key]) >= t:
                        n += 1
                else:
                    if int(item[key]) <= t:
                        n += 1
        return n
    # ----------------------------------------------------------
    # lets get the misc temp data by using one url, the IEM CLI API
    url = f"https://mesonet.agron.iastate.edu/json/cli.py?station=K{pil[3:]}&year={year}"

    # A GET request to the API
    r = requests.get(url)
    r = r.json()

    # ----------------------------------------------------------
    # this will count all the occurrences of specific degree days with the daily climate IEM API
    data_dict['misc_temp_data'].update(
        {'n days - minT <= 32' : _count_degree_days(r, 'low', 32, gt=False)}) # low temp <= 32

    data_dict['misc_temp_data'].update(
        {'n days - minT <= 20' : _count_degree_days(r, 'low', 20, gt=False)}) # low temp <= 20

    data_dict['misc_temp_data'].update(
        {'n days - minT <= 0' : _count_degree_days(r, 'low', 0, gt=False)}) # low temp <= 0

    data_dict['misc_temp_data'].update(
        {'n days - maxT <= 32' : _count_degree_days(r, 'high', 32, gt=False)}) # high temp <= 32

    data_dict['misc_temp_data'].update(
        {'n days - maxT >= 90' : _count_degree_days(r, 'high', 90, gt=True)}) # high temp >= 90

    data_dict['misc_temp_data'].update(
        {'n days - maxT >= 100' : _count_degree_days(r, 'high', 100, gt=True)}) # high temp >= 100

    data_dict['misc_temp_data'].update(
        {'n days - maxT >= 105': _count_degree_days(r, 'high', 105, gt=True)})  # high temp >= 105

    data_dict['misc_temp_data'].update(
        {'n days - maxT >= 110': _count_degree_days(r, 'high', 110, gt=True)})  # high temp >= 110

    # ----------------------------------------------------------
    # now lets get first/last dates
    # ----------------------------------------------------------
    # First/Last Days of 80, 90, 100
    # ----------------------------------------------------------
    def get_firstlast_date(r, lst, threshold, key):

        '''

        This function will parse the IEM Daily Climate API to find first/last occurrences of
        80,90,100F days for a climate site.

        Parameters
        ----------
        r : dict, the json dictionary for climate station in a given year
        lst : list, a list of daily temperatures
        threshold : int, the threshold temperature value that is being parsed
        key : str, acceptable values include: 'first', 'last', determines the index value and whether it is the first or last date

        Returns
        -------
        date : str, a datetime object that has been formatted as a string, e.g. 'JANUARY 1ST'
        '''

        # we are looking for first 80, 90, 100 degree days
        # run this to filter out any missing data days, helps to keep our indexing straight
        work_r = copy.deepcopy(r)  # make a copy
        for item in work_r['results']:
            if item['high'] == 'M':
                work_r['results'].remove(item)

        idx = [i for i, x in enumerate(lst) if x >= threshold]

        if idx:
            if key in 'first':

                date = datetime.strptime(work_r['results'][idx[0]]['valid'], '%Y-%m-%d')
                month = date.strftime('%B').upper()
                day = _find_numeric_suffix(date.day)

                return f'{month} {day}'

            elif key in 'last':

                date = datetime.strptime(work_r['results'][idx[-1]]['valid'], '%Y-%m-%d')
                month = date.strftime('%B').upper()
                day = _find_numeric_suffix(date.day)

                return f'{month} {day}'

        # if there are no values in idx, then return string 'NONE'
        elif not idx:
            return 'NONE'
    # ----------------------------------------------------------
    # first, use list comprehension to make lists of daily high temps
    daily_highs = [item['high'] for item in r['results'] if item['high'] != 'M']

    # first and last 80F days
    data_dict['misc_temp_data'].update({'FIRST 80F': get_firstlast_date(r, daily_highs, 80, 'first')})  # First 80F high date
    data_dict['misc_temp_data'].update({'LAST 80F': get_firstlast_date(r, daily_highs, 80, 'last')})  # Last 80F high date

    # first and last 90F days
    data_dict['misc_temp_data'].update({'FIRST 90F': get_firstlast_date(r, daily_highs, 90, 'first')})  # First 90F high date
    data_dict['misc_temp_data'].update({'LAST 90F': get_firstlast_date(r, daily_highs, 90, 'last')})  # Last 90F high date

    # first and last 100F days, note, these could be none, or the same date if there is only one occurrence
    data_dict['misc_temp_data'].update({'FIRST 100F': get_firstlast_date(r, daily_highs, 100, 'first')})  # First 100F high date
    data_dict['misc_temp_data'].update({'LAST 100F': get_firstlast_date(r, daily_highs, 100, 'last')})  # Last 100F high date

    # --------------------------------------------
    # First/Last Freeze Dates

    # get the first half of the year for lows
    first_half_lows = [item['low'] for item in r['results'] if item['low'] != 'M' and datetime.strptime(
        item['valid'], '%Y-%m-%d') <= datetime(year, 6, 30)]
    # get the second half of the year for lows
    second_half_lows = [item['low'] for item in r['results'] if item['low'] != 'M' and datetime.strptime(
        item['valid'], '%Y-%m-%d') > datetime(year, 6, 30)]
    # --------------------------------------------
    # last freeze date
    idx = [i for i, x in enumerate(first_half_lows) if x <= 32]

    if idx:
        date = datetime.strptime(r['results'][idx[-1]]['valid'], '%Y-%m-%d')
        month = date.strftime('%B').upper()
        day = _find_numeric_suffix(date.day)

        data_dict['misc_temp_data'].update({'LAST FREEZE': f'{month} {day}'})  # Last freeze date
    else:
        data_dict['misc_temp_data'].update({'LAST FREEZE': 'NONE'})  # Last freeze date


    # --------------------------------------------
    # first freeze date
    idx = [i for i, x in enumerate(second_half_lows) if x <= 32]

    if idx:
        # we need to split r into the second half in order to properly index the first freeze date
        second_half = r['results'][(len(first_half_lows) + 1):]

        date = datetime.strptime(second_half[idx[0]]['valid'], '%Y-%m-%d')
        month = date.strftime('%B').upper()
        day = _find_numeric_suffix(date.day)

        data_dict['misc_temp_data'].update({'FIRST FREEZE': f'{month} {day}'})  # First freeze date
    else:
        data_dict['misc_temp_data'].update({'FIRST FREEZE': 'NONE'})  # First freeze date

# ----------------------------------------------------------
def _get_wind_data(data_dict, pil, year, max_wd):
    '''
    This function will utilize the daily climate page API from IEM, to get max wind values

    Parameters
    ----------
    data_dict : dict, the main dictionary that includes all our main CLM and CF6 product data
    pil : The product ID, this is passed down to the function through main, e.g. 'CLMLZK'
    year : int, the climate year
    max_wd : int, the max wind threshold to make an entry for.

    Returns
    ----------
    None

    '''

    # the IEM API for daily climate data, for a given site and year
    url = f"https://mesonet.agron.iastate.edu/json/cli.py?station=K{pil[3:]}&year={year}"

    # A GET request to the API
    response = requests.get(url)

    # turn the request into a json, and subsequently a dictionary
    response = response.json()

    for item in response['results']:
        # max wind gust, direction, and date
        if str(item['highest_gust_speed']) not in 'M' and item['highest_gust_speed'] >= max_wd:
            data_dict['cf6_data']['wind']['max_gst'].append(item['highest_gust_speed'])  # add the wind gust
            data_dict['cf6_data']['wind']['max_wdr'].append(
                item['highest_gust_direction'])  # add the wind gust direction
            data_dict['cf6_data']['wind']['max_wd_dates'].append(item['valid'])
    return

# -----------------------------------------------------------
# Main function in dict_constructor.py
# -----------------------------------------------------------
def construct_data_dict(pil, months, year):

    '''

    Parameters
    ----------
    pil : str, the six letter climate product pil for a site, e.g. 'CLMLZK'
    months : list, a list of months as integers, for which to find urls for
    year : int, year, formatted as YYYY

    Returns
    -------
    data_dict : dict, the dictionary that includes all of our main climate data from the CLM products
    '''

    # establish our main working dictionary
    data_dict = {}

    # for i in range(1, 13):
    #     _data_dict = {f'{calendar.month_name[i][:3].upper()}': {}}
    #     data_dict.update(_data_dict)

    for i in range(len(months)):
        _data_dict = {f'{calendar.month_name[i + 1][:3].upper()}': {}}
        data_dict.update(_data_dict)

    # add a key for december, because our loop above does not account for it...
    if year != datetime.now().year:
        data_dict['DEC'] = {}

    # now add the months
    for key in data_dict:
        _data_dict = {
            'monthly_max_min': [],
            'high_dates': None,
            'low_dates': None,
            'monthly_avg_temps_dfn': [],  # avg high, low, mean, mean dfn
            'monthly_rain_and_dfn': [],  # monthly total rain and dfn
            'max_clndr_24hr_rain': [],
            # [max 24 hr rainfall (float), 24 hr rainfall dates, max calendar day rainfall (float), dates]
            'monthly_snow_sdepth_dfn': [],
            # monthly total snow, total snow dfn, greatest snow depth, and date(s) of greatest snow depth
            'max_clndr_24hr_snow': []  # max 24 hr total snow and date(s) of 24 hr total snow
        }
        data_dict[key].update(_data_dict)

    # add in a sub-dictionary for the misc temp data
    _data_dict = {
        'n days - minT <= 32': 0,
        'n days - minT <= 20': 0,
        'n days - minT <= 0': 0,
        'n days - maxT <= 32': 0,
        'n days - maxT >= 90': 0,
        'n days - maxT >= 100': 0,
        'n days - maxT >= 105': 0,
        'n days - maxT >= 110': 0,

        'LAST FREEZE' : None,
        'FIRST 80F' : None,
        'FIRST 90F' : None,
        'FIRST 100F' : None,
        'LAST 100F' : None,
        'LAST 90F' : None,
        'LAST 80F' : None,
        'FIRST FREEZE' : None
    }

    data_dict['misc_temp_data'] = _data_dict

    cf6_dict = {}

    for i in range(len(months)):
        _cf6_dict = {f'{calendar.month_name[i + 1][:3].upper()}': {}}
        cf6_dict.update(_cf6_dict)

    # add a key for december, because our loop above does not account for it...
    if year != datetime.now().year:
        cf6_dict['DEC'] = {}

    for key in cf6_dict:
        _cf6_dict = {'max_clndr_24hr_snow': [],  # max 24 hr snow val, and date(s) as taken from the CF6 sheet
                     'max_sdepth': []}  # greatest snow depth, and date(s) as taken from the CF6 sheet
        cf6_dict[key].update(_cf6_dict)

    cf6_dict['pres'] = {'min_pres': [],  # min pres (inHg)
                        'min_pres_dates' : [], # min pres dates
                        'max_pres': [],  # max pres (inHg)
                        'max_pres_dates' : [] # max pres dates
                        }

    cf6_dict['wind'] = {'max_gst': [],  # wind gusts >= 50 mph
                        'max_wdr': [],  # corresponding wind directions for the significant wind speeds
                        'max_wd_dates': [],  # corresponding dates for the max wind gusts
                        }

    data_dict['cf6_data'] = cf6_dict

    # -----------------------------------------------------------
    # first lets get the API links for a station
    clm_api_links = _get_clm_product_links(pil, months, year)  # these variables will be replaced with place holders in the main function

    print('Writing Data to Dictionary...')

    for url in clm_api_links:
        print('...')

        timestamp, station = _parse_timestamp(url)  # get the datetime and station from the url string
        # high_temp_data, low_temp_data = parse_clm_text(url) # this will get the high temp and low temp data from the text within the url
        text_dict = _parse_clm_text(url)  # this will get the high temp and low temp data from the text within the url

        # parse the high/low temp and add it to the main data dictionary
        _get_temp_data(data_dict,
                      text_dict['high_temp_data_text'], text_dict['low_temp_data_text'],
                      text_dict['avg_high_temp_data_text'], text_dict['avg_low_temp_data_text'],
                      text_dict['avg_monthly_data_text'],
                      timestamp.month)

        # parse the precip data and add it to the main data dictionary
        _get_precip_data(data_dict, text_dict['monthly_precip_data_text'], timestamp.month)

        # parse the snow data and add it to the main data dictionary, we will also be getting some data from the CF6 sheet...
        _get_snow_data(pil, data_dict, text_dict['monthly_snow_data_text'], timestamp.month)

    # -----------------------------------------------------------------
    # add in a block here to get snow data through the CF6 sheet api links
    cf6_api_links = _get_cf6_product_links(pil, months, year)

    for idx, url in enumerate(cf6_api_links):
        _get_cf6_snow_pres_data(pil, data_dict, url, idx)

    # ----------------------------------------------------------------
    # MISC DATA
    # parse the misc temp data
    _get_misc_temp_data(data_dict, pil, year)

    # parse significant wind data
    _get_wind_data(data_dict, pil, year, 50)

    # ----------------------------------------------------------------
    # XMACIS DATA
    # this will add the ranking and recency text data from XMACIS to the data dictionary
    # only run if we have 12 months of data, i.e. the climate year is complete
    if len(clm_api_links) == 12:
        data_dict['xmacis_data'] = xmacis_mainfunc(pil, year)

    else:
        data_dict['xmacis_data'] = ma.masked
        print(f'The requested climate year: {year} is not complete, current ranked climate data via xmacis is not available yet. \n')



    print(f'Done Writing Data to Dictionary for {year} {pil[3:]} Annual Summary...\n')

    return data_dict





# ##################################################################
# From record_data.py
# ##################################################################
def _parse_rec_dates(rec_dt, year):
    '''
    This function will convert our record date format from 'mm/dd' to 'mm-dd'.
    We have to add year, to check on leap years, otherwise, without declaring a year,
    datetime defaults the year to 1990, and this can raise errors if we have records
    on a leap day.


    Parameters
    ----------
    rec_dt : list, the list of daily record dates
    year : int, the year, formated as YYYY

    Returns
    ----------
    rec_dt : list, the list with newly formatted dates

    '''

    if rec_dt:
        return [datetime.strptime(f'{dt}/{year}', '%m/%d/%Y').strftime('%m-%d') for dt in rec_dt]
    else:
        return rec_dt

# ----------------------------------------------------------
def _get_iem_records(r, rec_dt, rec_key):
    '''
    This function will take a dictionary of daily climate data, obtained from the IEM API, and
    obtain all the previous daily record data, based on the input new daily record dates.

    Parameters
    ----------
    r : dict, this is the json text we get from the IEM API, which contains daily climate summary info for a given year
    rec_dt : list, a list of new daily record dates that we need previous record info for
    rec_key : str, the keyword for which record we are parsing, args include:
                   'low_record', 'high_record', 'precip_record', 'snow_record'
    Returns
    ----------
    old_records : list, a list of lists with previous record values, and years it was set, e.g. record lows [[1, 2009, 2011], [5, 2005]]

    '''

    old_records = []

    if rec_dt:

        # iterate over the new record dates
        for rec_dt in rec_dt:
            # iterate through each day in cli, which is a dictionary with all days
            for item in r['results']:
                # find each day in the cli json that has a new daily record
                if rec_dt in item['valid']:

                    # get the record value and years based on keyword arguments
                    _old_record = []  # blank list to add record values/years to

                    # for snow, sometimes during the summer months, it comes back as 'M'
                    if item[rec_key] == 'M' and rec_key == 'snow_record':
                        _old_record.append(0.0)

                    else:
                        _old_record.append(item[rec_key])

                    for yr in item[f'{rec_key}_years']:
                        _old_record.append(yr)

                    old_records.append(_old_record)
        return old_records
    else:
        return old_records  # should be an empty list if no record dates

# ----------------------------------------------------------
# Main Function in record_data.py
# ----------------------------------------------------------
def assemble_records(pil, year):

    '''

    This function will take a new daily record text file, that is generated at the office,
    and find all previous daily record info, and assemble both pieces of data into a new
    dictionary, to be used in formatter_main.py

    Parameters
    ----------
    pil : str, the product pil for what climate product we are parsing
    year : int, the year of climate products we are working on, YYYY

    Returns
    -------
    records_dict : dict, a dictionary full of previous record info, and new daily record info

    '''


    # first up, import the record data from our record text file, that is generated in the office
    # this will get all the known record data for the new year...
    with open(f'./records/my{pil[3:].lower()}recs.txt', 'r') as f:
        recs = f.read()

    # check here to see if we have current records data
    #for line in recs.splitlines():
    if str(year) not in recs:
        return None  # return a None value for recs_dict

        # # exit the for loop and continue with the function
        # elif str(year) in :
        #     break


    rec_mx = recs.split('\nRecord High\n')[-1].split('\nRecord Low High\n')[0].splitlines()  # record high
    rec_mx = [f'{rec},HIGH TEMPERATURE' for rec in rec_mx]

    rec_lwmx = recs.split('\nRecord Low High\n')[-1].split('\nRecord High Low\n')[0].splitlines()  # record low high
    rec_lwmx = [f'{rec},COOLEST HIGH TEMPERATURE' for rec in rec_lwmx]

    rec_lw = recs.split('\nRecord Low\n')[-1].split('\nRecord Rain\n')[0].splitlines()  # record low
    rec_lw = [f'{rec},LOW TEMPERATURE' for rec in rec_lw]

    rec_mxlw = recs.split('\nRecord High Low\n')[-1].split('\nRecord Low\n')[0].splitlines()  # record high low
    rec_mxlw = [f'{rec},WARMEST LOW TEMPERATURE' for rec in rec_mxlw]

    rec_pcp = recs.split('\nRecord Rain\n')[-1].split('\nRecord Snow\n')[0].splitlines()  # record rain
    rec_sn = recs.split('\nRecord Snow\n')[-1].splitlines()[:-1]  # record snow, remove the last item from rec_sn because it will always be '\n'

    # set up our records dictionary
    clm_recs = [rec_mx, rec_lwmx, rec_lw, rec_mxlw, rec_pcp, rec_sn]
    keys = ['rec_mx', 'rec_lwmx', 'rec_lw', 'rec_mxlw', 'rec_pcp', 'rec_sn']

    recs_dict = {
        'new_recs': {},
        'old_recs': {}
    }

    recs_dict['new_recs'] = dict(zip(keys, clm_recs))

    # ----------------------------------------------------------
    # gets the dates we need, this converts from 'mm/dd' to 'mm-dd',
    # since that is the format used on the IEM API page
    #year = 2024

    rec_mx_dt = _parse_rec_dates([dt.split(',')[0] for dt in recs_dict['new_recs']['rec_mx']], year)
    rec_lwmx_dt = _parse_rec_dates([dt.split(',')[0] for dt in recs_dict['new_recs']['rec_lwmx']], year)
    rec_lw_dt = _parse_rec_dates([dt.split(',')[0] for dt in recs_dict['new_recs']['rec_lw']], year)
    rec_mxlw_dt = _parse_rec_dates([dt.split(',')[0] for dt in recs_dict['new_recs']['rec_mxlw']], year)
    rec_pcp_dt = _parse_rec_dates([dt.split(',')[0] for dt in recs_dict['new_recs']['rec_pcp']], year)
    rec_sn_dt = _parse_rec_dates([dt.split(',')[0] for dt in recs_dict['new_recs']['rec_sn']], year)

    # ----------------------------------------------------------
    # we still need to go to the IEM API, and fetch all the previous record data/years... how to store it??
    # perhaps same key structure? e.g. 'rec_mx': [[(old record value), yyyy, yyyy, etc.], [(old record value), yyyy, yyyy, etc.]]

    # the IEM API for daily climate data, for a given site and year
    url = f"https://mesonet.agron.iastate.edu/json/cli.py?station=K{pil[3:]}&year={year}"

    # A GET request to the API
    r = requests.get(url)
    r = r.json()

    old_mx_rec = _get_iem_records(r, rec_mx_dt, 'high_record')
    old_lw_rec = _get_iem_records(r, rec_lw_dt, 'low_record')
    old_pcp_rec = _get_iem_records(r, rec_pcp_dt, 'precip_record')
    old_sn_rec = _get_iem_records(r, rec_sn_dt, 'snow_record')

    # lets assumble this into a sub_dictionary and add it to the main dict
    old_recs = [old_mx_rec, old_lw_rec, old_pcp_rec, old_sn_rec]
    keys = ['rec_mx', 'rec_lw', 'rec_pcp', 'rec_sn']

    recs_dict['old_recs'] = dict(zip(keys, old_recs))

    return recs_dict

    # not sure about low-high and high-low data/dates, will have to think on this one...
    # only solution for now seems to be searching for RER's?, but this will be time-consuming and difficult...




# ##################################################################
# From xmacis_parser.py
# ##################################################################
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
# Main function in xmacis_parser.py
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
    with open(f'./json_dict/XMACIS_SortedData_{pil[3:]}.txt', 'w') as f:
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






# ##################################################################
# From formatter_main.py
# ##################################################################
# -----------------------------------------------------------
# Main function in formatter_main.py
# -----------------------------------------------------------
def write_textfile(data_dict, pil, months, year):

    print('\nWriting Data to New Text File...\n...')

    # get the station ID from the pil
    stn_name = _get_station_name(pil)

    # ----------------------------------------------------------
    # here we're actually writing the text file
    with open(f'./output/Supplemental_{year}_CLA{pil[3:]}_Data.txt', 'w') as f:

        f.write('IMPORTANT INFO ABOUT THIS PRODUCT:\n')
        f.write("ALL DATA SHOULD BE MANUALLY QC'D AND CHECKED AGAINST THE CLM/CF6 PRODUCTS\n")
        f.write('\n\n**********  BE SURE TO TURN OFF AUTOWRAP BEFORE COPYING THIS INTO A TEXT EDITOR  **********\n\n')

        # add timestamp to top of product text
        now = datetime.utcnow().strftime('%m/%d/%Y %H:%MZ')
        f.write(f'PRODUCT GENERATED AT: {now}\n\n')

        f.write(header)
        f.write(f'{year} SUPPLEMENTAL ANNUAL CLIMATE DATA FOR {stn_name}\n')
        f.write(f'{header}')

        # ----------------------------------------------------------
        # add in the supplemental text data for temps and precip, obtained from XMACIS and the CF6
        _make_supplemental_data_text(data_dict, f, year, char_lim = 69)

        # ----------------------------------------------------------
        # add in the supplemental tables
        f.write('\n\n(DFN = DEPARTURE FROM NORMAL)\n')

        # first up, the annual temperature summary table
        _make_temp_table(data_dict, f, months, year, stn_name)

        # next, write the annual precip summary table
        _make_precip_table(data_dict, f, months, year, stn_name)

        # next, write the annual snow summary table
        _make_snow_table(data_dict, f, months, year, stn_name)

        # make the misc data summary
        _make_misc_data(data_dict, f)

        # make the daily records info summary
        _make_records_table(f, pil, year)

        print('Done Writing New Text File...\n...')
        print(f'New File Saved As: ./output/Supplemental_{year}_CLA{pil[3:]}_Data.txt\n')


        return f.close()






# ##################################################################
# Lets run the script!
# ##################################################################

# ------------------------------------------------------------------
# here is the function that will be called in the colab notebook
def run_cla_formatter_notebook(pil, year, save_dict):

    # here we define the range of months, based on the entered
    # command line year param
    months, current_year_flag = _month_range(year)

    # this is for when we're going to run the whole function
    data_dict = construct_data_dict(pil, months, year)

    # ------------------------------------------------------------------
    # handle save_dict boolean,
    # whether or not to save the dictionary we make if we are generating one
    if save_dict:
        # for quickly saving a dictionary to a text file, for easier testing later on
        with open(f'./test_files/{year}_{pil[3:]}_Annual_Summary.txt', 'w') as f:
            f.write(json.dumps(data_dict, indent = 4))

    # ------------------------------------------------------------------
    # handle current_year_flag
    if not current_year_flag:
        months.append(13) # this will make sure we get JAN-DEC, otherwise, it is only JAN-NOV

    # ------------------------------------------------------------------
    # now write the text file
    write_textfile(data_dict, pil, months, year)