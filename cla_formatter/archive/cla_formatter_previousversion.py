# Import our modules

# BEAUTIFUL SOUP
from bs4 import BeautifulSoup

# BUILT IN
import calendar
import itertools
from operator import itemgetter
import os, sys
import requests
from urllib.request import urlopen

# MISC
from datetime import datetime, timezone
import json
import numpy.ma as ma
import numpy as np
import pandas as pd
import textwrap

# DICT CONSTRUCTOR
from dict_constructor import construct_data_dict

###########################################################################################

'''
formatter_main.py
Written by: Erik Green, WFO Little Rock, Aug 2024

CLA Formatter utilizes the functions:
    dict_constructor.py
    (api script for xmacis record info)
    (api script for iem record info)
    
The script first utilizes dict_constructor.py, where it fetches the API urls 
for each CLM product for a site, through the IEM API. From there, a main data 
dictionary is constructed.

In formatter_main.py, the main data dictionary is utilized to generate a text file that contains
all annual supplemental data to be included in the CLA product for a climate site.



'''


###########################################################################################
# formatter_main.py - Now that we have the dictionary constructed, lets write the text file...
###########################################################################################
# ----------------------------------------------------------
# Helping Sub-Functions
# ----------------------------------------------------------

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

# -----------------------------------------------------------
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
# Main Working Functions
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

    '''

    # set up current date parameters to check with
    current_date = datetime.now()
    current_year = current_date.year
    current_month = current_date.month

    # check if current year
    # if it is current year, then we need to automatically define a
    # range of months based on the current month
    if year == current_year:
        return [x for x in range(current_month + 1)][2:]  # set up the months to be the last most recently completed month, starting with Feb for Jan product

    # if it is not the current year, then set up a list with all months
    elif year != current_year:
        return [x for x in range(13)][2:]  # start in feb for jan CLM product
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
    #f.write(f'\n{year} TEMPERATURE AVERAGES AND EXTREMES                     {stn_name}, ARKANSAS\n')
    f.write('\n{:60}\n'.format(*arr))
    f.write(table_sep)
    f.write('             AVERAGE TEMPERATURES             |            TEMPERATURE EXTREMES\n')

    arr = ['MONTH', 'HIGH', 'LOW', 'MONTHLY', 'DFN', '|', 'MAX', 'DATE(S)', 'MIN', 'DATE(S)']
    f.write('{:9}{:9}{:9}{:12}{:7}{:5}{:9}{:16}{:9}{:9}\n'.format(*arr))
    #f.write('MONTH     HIGH     LOW     MONTHLY     DFN    |    MAX      DATE(S)         MIN      DATE(S)\n')
    f.write(table_sep)

    # order is as follows: month, avg max, avg min, avg mean, avg dfn, max, max dates, min, min dates
    for m, avgmx, avgmn, avgt, dfn, mx, mxdt, mn, mndt in zip(
                                                            months,
                                                            avg_high_temps, avg_low_temps, avg_temps, avg_temps_dfn,
                                                            high_temps, high_temp_dates,
                                                            low_temps, low_temp_dates
                                                             ):
        # do some formatting here...
        # start with constants for certain spaces in the sequence
        # space1 = space * 4  # the space between the dfn, and the | separator
        # space2 = space * 7  # the space between the monthly high temp, and high temp dates
        # space3 = space * 14  # the space b/w the max temp dates, and the monthly min temp
        # space4 = space * 7  # the space b/w the min temp, and min temp dates

        # format the dfn spacing and positive dfn's
        # if dfn >= 10.0 or dfn <= -10.0:
        #     space1 = space * 3

        if dfn > 0.0:
            dfn = f'+{dfn}'

        # format the max temp spacing if > 100
        # if mx >= 100:
        #     space2 = space * 6
        #
        # # format the min temp spacing if < -10
        # if mn <= -10:
        #     space4 = space * 6

        # format the max temp dates
        if isinstance(mxdt, list):
            # adj = (len(mxdt) * 3) - 3
            # space3 = space * (14 - adj)
            mxdt = [i.split('/')[-1] for i in mxdt]
            mxdt = "/".join(sorted(mxdt, key=int))  # sort the days in ascending order

        else:
            mxdt = mxdt.split('/')[-1]

        # format the min temp dates
        if isinstance(mndt, list):
            mndt = [i.split('/')[-1] for i in mndt]
            mndt = "/".join(sorted(mndt, key=int))  # sort the days in ascending order
        else:
            mndt = mndt.split('/')[-1]

        arr = [f'{m}', f'{avgmx}', f'{avgmn}', f'{avgt}', f'{dfn}', '|', f'{mx}', f'{mxdt}', f'{mn}', f'{mndt}']
        f.write('{:9}{:9}{:9}{:12}{:7}{:5}{:9}{:16}{:9}{:9}\n'.format(*arr))

        #f.write(f'{m}       {avgmx}    {avgmn}     {avgt}       {dfn}{space1}|    {mx}{space2}{mxdt}{space3}{mn}{space4}{mndt}\n')

    # -------------------------------------------------
    # next add in the annual summary data
    f.write(table_sep)  # add a table separator

    # constant space values we will need to adjust
    # space1 = space * 4  # space b/w avg dfn and the | separator
    # space2 = space * 7  # space b/w the yearly max temp and the yearly max temp dates
    # space3 = space * 10  # space b/w the yearly max temp dates and the yearly min
    # space4 = space * 7  # space b/w the yearly min and yearly min dates

    yrly_avgmx = np.round(np.mean(avg_high_temps), 1)
    yrly_avgmn = np.round(np.mean(avg_low_temps), 1)
    yrly_avg = np.round(np.mean(avg_temps), 1)

    yrly_avg_dfn = np.round(np.sum(avg_temps_dfn), 1)
    # if yrly_avg_dfn >= 10.0 or yrly_avg_dfn <= -10.0:
    #     space1 = space * 3

    if yrly_avg_dfn > 0.0:
        yrly_avg_dfn = f'+{yrly_avg_dfn}'

    yrly_mx = np.max(high_temps)
    # if yrly_mx >= 100.0:
    #     space2 = space * 6

    yrly_mn = np.min(low_temps)
    # if yrly_mn <= -10.0:
    #     space4 = space * 6

    # -------------------------------------------------
    def _find_annual_extreme_dates(temp_dates_lst, idx, max_temp=False):

        '''
        This function will parse a temp dates list with a given index and return a string of the extreme dates

        Parameters
        ----------
        temp_dates_lst : list, a list of the extreme temperature dates for a month
        idx : int, the index value of the extreme temperature date for the year
        max_temp : bool, default = False, if max_temp, then edit space3 variable if necessary

        Returns
        ----------
        dt : str, a string of the formatted extreme temp dates
        _space3 : str, a string of formatted spaces for the space 3 variable in the annual values line

        '''

        #_space3 = space3  # we only return this for the max temp dates

        if isinstance(temp_dates_lst[idx], list):
            # if max_temp:
            #     adj = (len(temp_dates_lst[idx]) * 3) - 3
            #     _space3 = space * (10 - adj)

            dt = [i.split('/')[-1] for i in temp_dates_lst[idx]]
            dt = "/".join(sorted(dt, key=int)),  # sort the days in ascending order
        else:
            dt = temp_dates_lst[idx].split('/')[-1]

        return dt #, _space3

    # -------------------------------------------------
    # format the annual max temp date(s)
    yrly_mx = np.max(high_temps)
    idx = high_temps.index(yrly_mx)

    mxdt = _find_annual_extreme_dates(high_temp_dates, idx, max_temp=True)
    yrly_mxdt = f'{calendar.month_name[idx + 1][:3].upper()} {mxdt}'

    # format the annual min temp date(s)
    yrly_mn = np.min(low_temps)
    idx = low_temps.index(yrly_mn)

    mndt = _find_annual_extreme_dates(low_temp_dates, idx, max_temp=False)
    yrly_mndt = f'{calendar.month_name[idx + 1][:3].upper()} {mndt}'

    # now lets write in the data
    arr = ['ANNUAL', f'{yrly_avgmx}', f'{yrly_avgmn}', f'{yrly_avg}', f'{yrly_avg_dfn}', '|', f'{yrly_mx}', f'{yrly_mxdt}', f'{yrly_mn}', f'{yrly_mndt}']
    f.write('{:9}{:9}{:9}{:12}{:7}{:5}{:9}{:16}{:9}{:9}\n'.format(*arr))
    #f.write(f'ANNUAL    {yrly_avgmx}    {yrly_avgmn}     {yrly_avg}       {yrly_avg_dfn}{space1}|    {yrly_mx}{space2}{yrly_mxdt}{space3}{yrly_mn}{space4}{yrly_mndt}\n')

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

    f.write(f'\n\n{year} RAINFALL, DEPARTURES, AND EXTREMES                    {stn_name}, ARKANSAS\n')
    f.write(table_sep)
    f.write('MONTH        RAINFALL       DFN               MAX/CALENDAR DAY            MAX/24 HOURS\n')
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
        # start with constants for certain spaces in the sequence
        space1 = space * 10  # the space b/w the precip and the departure
        space2 = space * 14  # the space b/w the precip dfn and the precip calendar day max
        space3 = space * 19  # space b/w precip calendar day max date and the max precip storm total

        # edit space 1 if monthly precip is greater than 10 inches
        if pcp >= 10.00:
            space1 = space * 9

        # edit space 2 if monthly precip dfn is >= 10.0 or <= -10.0
        if dfn >= 10.0 or dfn <= -10.0:
            space2 = space * 12

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
            space3 = space * 14

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

        # write in the line of monthly data
        f.write(
            f'{m}          {pcp:.2f}{space1}{dfn}{space2}{pcp_mx_clndr:.2f}/{clndr_mxdt}{space3}{pcp_mx_storm_total:.2f}/{stormtotal_mxdt}\n')
        # end of for loop

    # --------------------------------------------
    # constant space values that may be edited based on the data
    space1 = space * 14  # space b/w yearly dfn, and yrly calendar day max precip
    space2 = space * 17  # space b/w yearly calendar day max precip date, and yearly daily max storm total precip value

    # now lets add the annual data
    yrly_rain = np.round(np.sum(precip), 2)
    yrly_dfn = np.round(np.sum(precip_dfn), 2)

    # adjust space1 if yearly precip dfn is >= 10.0 or <= -10.0
    if yrly_dfn >= 10.0 or yrly_dfn <= -10.0:
        space1 = space * 13

    # if the yrly precip dfn is >= 0.0, then assign a positive sign in the string
    if yrly_dfn >= 0.0:
        yrly_dfn = f'+{yrly_dfn}'

    # get the max calendar day and storm total precip values
    # mx_clndr_pcp = np.max(precip_mx_clndr)
    # mx_stormtotal_pcp = np.max(precip_mx_storm_total)

    # need to format the annual precip extreme dates...

    # -------------------------------------------------
    def _find_annual_extreme_dates(pcp_dates_lst, idx, clndr_day_max=False):

        '''
        This function will parse a precip dates list with a given index and return a string of the extreme dates

        pcp_dates_lst : list, a list of the extreme precip dates for a month
        idx : int, the index value of the extreme precip date for the year
        clndr_day_max : bool, default = False, if clndr_day_max, then edit space2 variable if necessary

        returns:
        yrly_mxdt : str, a string of the formatted extreme precip dates
        _space2 : str, a string of formatted spaces for the space 2 variable in the annual values line

        '''
        _space2 = space2  # we only edit this for the calendar day max precip date, if it is two dates

        bdt = list(filter(None, (pcp_dates_lst[idx].split('TO')[0].split(' '))))[0]
        edt = list(filter(None, (pcp_dates_lst[idx].split('TO')[-1].split(' '))))[0]

        if bdt == edt:
            yrly_mxdt = bdt.split('/')[-1]

        elif bdt != edt:
            bdt = bdt.split('/')[-1]
            edt = edt.split('/')[-1]
            yrly_mxdt = f'{bdt}-{edt}'

            if clndr_day_max:
                _space2 = space * 14

        return yrly_mxdt, _space2

    # -------------------------------------------------

    # yearly calendar day max precip date
    clndr_pcp_mx = np.max(precip_mx_clndr)
    idx = precip_mx_clndr.index(clndr_pcp_mx)  # index value of the calendar day max precip

    clndr_max_pcp_dt, space2 = _find_annual_extreme_dates(precip_mx_clndr_dates, idx, clndr_day_max=True)
    yrly_clndr_pcp_mx = f'{calendar.month_name[idx + 1][:3].upper()} {clndr_max_pcp_dt}'

    # -------------------------------------------------

    # yearly daily storm total max precip date
    storm_total_pcp_mx = np.max(precip_mx_storm_total)
    idx = precip_mx_storm_total.index(storm_total_pcp_mx)

    storm_total_pcp_max_dt, _ = _find_annual_extreme_dates(precip_mx_storm_total_dates, idx, clndr_day_max=False)
    yrly_storm_total_pcp_max_dt = f'{calendar.month_name[idx + 1][:3].upper()} {storm_total_pcp_max_dt}'

    # -------------------------------------------------
    # write in the annual precip summary data
    f.write(table_sep)
    f.write(
        f'ANNUAL       {yrly_rain}         {yrly_dfn}{space1}{clndr_pcp_mx}/{yrly_clndr_pcp_mx}{space2}{storm_total_pcp_mx}/{yrly_storm_total_pcp_max_dt}\n')

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
    print('Writing in Annual Snow Table...\n...')

    # --------------------------------------------
    # add table headers and column information
    f.write(f'\n\n{year} SNOWFALL, DEPARTURES, AND EXTREMES                    {stn_name}, ARKANSAS\n')
    f.write(table_sep)
    f.write('MONTH   SNOW      DFN       MAX/CALENDAR DAY       MAX/24 HOUR       GREATEST DEPTH/DATE\n')
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

    mx_sdepth = [data_dict[m]['monthly_snow_sdepth_dfn'][2] for m in snow_months]
    mx_sdepth_dt = [data_dict[m]['monthly_snow_sdepth_dfn'][3] for m in snow_months]

    mx_24hr_snow = [data_dict[m]['max_clndr_24hr_snow'][0] for m in snow_months]
    mx_24hr_snow_dt = [data_dict[m]['max_clndr_24hr_snow'][1] for m in snow_months]

    # for loop begins
    for m, sn, dfn, mx_sd, mx_sd_dt, mx_24hr_sn, mx_24hr_sn_dt in zip(
            snow_months,
            snow, snow_dfn,
            mx_sdepth, mx_sdepth_dt,
            mx_24hr_snow, mx_24hr_snow_dt
    ):
        # constant space values that may need to be edited
        space1 = space * 6  # space b/w the monthly snow, and the dfn
        space2 = space * 7  # space b/w the dfn and the calendar day max snow/date
        space3 = space * 20  # space b/w the calendar day max snow/date and the 24 hr max snow date
        space4 = space * 15  # space b/w 24 hour max, and

        # lets do some formatting here
        # account for when there is trace monthly snow
        if sn == 'T':
            sn = 'T  '

        # edit space1 when snow is >= 10.0
        if isinstance(sn, float) and sn >= 10.0:  # used isinstance to keep value error from popping up when making comparison
            space1 = space * 5

        # adjust dfn parameters
        if dfn > 0.0:
            if dfn >= 10.0:  # snow dfn will never be less than 10 inches for our climate sites
                space2 = space * 6
            dfn = f'+{dfn}'

        elif dfn == 0.0:
            dfn = f' {dfn}'

        # --------------------------------------------
        # lets format the max calendar day and 24 hr snowfall dates here...
        if isinstance(mx_24hr_sn_dt, str):  # if no max snowfall value, then date values are set to nan, so check for this

            bdt = list(filter(None, (mx_24hr_sn_dt.split('TO')[0].split(' '))))[0]
            edt = list(filter(None, (mx_24hr_sn_dt.split('TO')[-1].split(' '))))[0]

            # if the dates are the same, then format for one day
            if bdt == edt:
                mx_24hr_sn_dt = _find_numeric_suffix(bdt.split('/')[-1])

                if isinstance(mx_24hr_sn, float) and mx_24hr_sn >= 10.0:
                    space3 = space * 9
                    space4 = space * 6

                elif isinstance(mx_24hr_sn, float) and mx_24hr_sn <= 10.0:
                    space3 = space * 15
                    space4 = space * 10

                elif mx_24hr_sn == 'T':
                    space3 = space * 17
                    space4 = space * 12

            # if the dates are not the same, then format for multiple dates
            elif bdt != edt:
                try:
                    bdt = _find_numeric_suffix(bdt.split('/')[-1])
                except ValueError:
                    bdt = '|*DDDD*|'
                try:
                    edt = _find_numeric_suffix(edt.split('/')[-1])
                except ValueError:
                    edt = '|*DDDD*|'

                mx_24hr_sn_dt = f'{bdt}-{edt}'

                if isinstance(mx_24hr_sn, float) and mx_24hr_sn >= 10.0:
                    space3 = space * 9
                    space4 = space * 4

                elif isinstance(mx_24hr_sn, float) and mx_24hr_sn <= 10.0:
                    space3 = space * 10
                    space4 = space * 5

                elif mx_24hr_sn == 'T':
                    space3 = space * 11
                    space4 = space * 6

            # format the max calendar day, and 24 hour snow fall values and dates (if applicable)
            mx_24hr_sn = f'{mx_24hr_sn}/{mx_24hr_sn_dt}'

        # handle an exception where an error occurs in the CLM, e.g. 'MM29 TO 02/29', and monthly max snow is not 0.0
        if sn != 0.0 and np.isnan(mx_24hr_sn_dt):
            mx_24hr_sn_dt = '|*Check Date(s)*|'
            # format the max calendar day, and 24 hour snow fall values and dates (if applicable)
            mx_24hr_sn = f'{mx_24hr_sn}/{mx_24hr_sn_dt}'

        # lets format the max snow depth/dates
        if mx_sd > 0.0:
            if isinstance(mx_sd_dt, list):
                dt = "/".join(_find_numeric_suffix(i.split('/')[-1]) for i in mx_sd_dt)
            else:
                dt = _find_numeric_suffix(mx_sd_dt.split('/')[-1])
            mx_sd = f'{int(mx_sd)}/{dt}'
        else:
            mx_sd = '0'

        f.write(f'{m}     {sn}{space1}{dfn}{space2}{mx_24hr_sn}{space3}{mx_24hr_sn}{space4}{mx_sd}\n')
        # end of for loop

    # -----------------------------------------------------------
    # now lets add the annual data max values at the bottom of the table

    # constant space values
    space1 = space * 6  # space b/w the yearly snow and the yearly dfn
    space2 = space * 7  # space b/w the yearly snow dfn and the calendar day max snow value/date
    space3 = space * 13 # space b/w the annual calendar day max snow/date and annual 24 hr snow max/date
    space4 = space * 15 # space b/w the 24 hr snow max/dates and the annual greatest snow depth

    # -------------------------------------------
    # get the annual snowfall total
    sn_filtered = [s for s in snow if s != 'T']  # filter out trace values

    # if the two lists are equal after filtering, then we filtered no T's
    if len(snow) == len(sn_filtered):
        yrly_sn = np.sum(sn_filtered)

    # if the two lists are not equal after filtering, then we filtered T's out...
    elif len(snow) != len(sn_filtered):
        if np.sum(sn_filtered) == 0.0:
            yrly_sn = 'T'

        elif np.sum(sn_filtered) > 0.0:
            yrly_sn = np.sum(sn_filtered)

    if isinstance(yrly_sn, float) and yrly_sn >= 10.0:
        space1 = space * 5

    # -------------------------------------------
    # get the annual snow dfn
    yrly_sn_dfn = np.round(np.sum(snow_dfn), 1)

    if yrly_sn_dfn > 10.0:
        yrly_sn_dfn = f'+{yrly_sn_dfn}'
        space2 = space * 6

    elif yrly_sn_dfn == 0.0:
        yrly_sn_dfn = ' 0.0'

    # -------------------------------------------
    # get the max calendar day snow value and date, which will also be the 24 hr max value...
    # use our find annual extreme function here...
    # -------------------------------------------------
    def _find_annual_extreme_dates(sn_dates_lst, idx, clndr_day_max = False):
        '''

        This function will parse a precip dates list with a given index and return a string of the extreme dates

        Parameters
        ----------
        sn_dates_lst : list, a list of the extreme snow dates for a month
        idx : int, the index value of the extreme precip date for the year
        clndr_day_max : bool, default = False, if clndr_day_max, then edit space2 variable if necessary

        Returns
        ----------
        yrly_mxdt : str, a string of the formatted extreme snow dates
        _space3 : str, a string of formatted spaces for the space 3 variable in the annual values line
        _space4 : str, a string of formatted spaces for the space 4 variable in the annual values line

        '''
        _space3 = space3  # we only edit this for the calendar day max snow date, if it is two dates
        _space4 = space4

        try:
            bdt = list(filter(None, (sn_dates_lst[idx].split('TO')[0].split(' '))))[0]
            edt = list(filter(None, (sn_dates_lst[idx].split('TO')[-1].split(' '))))[0]

            if bdt == edt:
                yrly_mxdt = bdt.split('/')[-1]

            elif bdt != edt:
                bdt = bdt.split('/')[-1]
                edt = edt.split('/')[-1]
                yrly_mxdt = f'{bdt}-{edt}'

                if clndr_day_max:
                    _space3 = space * 10
                    _space4 = space * 5

            return yrly_mxdt, _space3, _space4

        # exception for weird cases where annual max occurs on multiple days...
        except AttributeError:
            yrly_mxdt = '|*Check Date(s)*|'
            return yrly_mxdt, _space3, _space4

    # -------------------------------------------------
    # get the annual max calendar day snow
    sn_filtered = [s for s in mx_24hr_snow if s != 'T']  # filter out trace values

    # if the two lists are equal after filtering, then we filtered no T's
    if len(mx_24hr_snow) == len(sn_filtered):
        sn_clndr_mx = np.max(sn_filtered)

    # if the two lists are not equal after filtering, then we filtered T's out...
    elif len(mx_24hr_snow) != len(sn_filtered):
        if np.sum(sn_filtered) == 0.0:
            sn_clndr_mx = 'T'

        elif np.sum(sn_filtered) > 0.0:
            sn_clndr_mx = np.max(sn_filtered)

    # -------------------------------------------------
    # if the annual calendar day max snow value is > 0.0, and not trace
    if isinstance(sn_clndr_mx, float) and sn_clndr_mx > 0.0:
        idx = mx_24hr_snow.index(sn_clndr_mx)  # index value of the calendar day max snow
        clndr_snow_mx_dt, space3, space4 = _find_annual_extreme_dates(mx_24hr_snow_dt, idx, clndr_day_max=True)
        yrly_clndr_sn_mx = f'{sn_clndr_mx}/{calendar.month_name[idx + 1][:3].upper()} {clndr_snow_mx_dt}'

        if sn_clndr_mx >= 10.0:
            n1 = len(space3)
            n2 = len(space4)
            space3 = space*(n1-1)
            space4 = space*(n2-1)

    elif isinstance(sn_clndr_mx, float) and sn_clndr_mx == 0.0:
        yrly_clndr_sn_mx = '0.0'
        space3 = space*13
        space4 = space*15

    elif isinstance(sn_clndr_mx, str):
        yrly_clndr_sn_mx = 'T/|*Check Dates*|'
        space3 = space*6
        space4 = space*8

    # -------------------------------------------------
    # get the annual max snow depth and the date
    yrly_mx_sd = np.max(mx_sdepth)

    if yrly_mx_sd > 0:
        idx = mx_sdepth.index(yrly_mx_sd)
        yrly_mx_sd_dt, _, _ = _find_annual_extreme_dates(mx_sdepth_dt, idx, clndr_day_max=False)
        yrly_mx_sd = f'{int(yrly_mx_sd)}/{calendar.month_name[idx + 1][:3].upper()} {yrly_mx_sd_dt}'

    else:
        yrly_mx_sd = '0'


    # -------------------------------------------------
    f.write(table_sep)
    f.write(f'ANN.    {yrly_sn}{space1}{yrly_sn_dfn}{space2}{yrly_clndr_sn_mx}{space3}{yrly_clndr_sn_mx}{space4}{yrly_mx_sd}\n')

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
    f.write('LAST FREEZE.......................................................|*Date*|\n')
    f.write('FIRST 80-DEGREE DAY...............................................|*Date*|\n')
    f.write('FIRST 90-DEGREE DAY...............................................|*Date*|\n')
    f.write('FIRST 100-DEGREE DAY..............................................|*Date*|\n')
    f.write('LAST 100-DEGREE DAY...............................................|*Date*|\n')
    f.write('LAST 90-DEGREE DAY................................................|*Date*|\n')
    f.write('LAST 80-DEGREE DAY................................................|*Date*|\n')
    f.write('FIRST FREEZE......................................................|*Date*|\n')


# -----------------------------------------------------------
# Main Script - write_textfile()
# -----------------------------------------------------------
def write_textfile(data_dict, pil, months, year):

    print('\nWriting Data to New Text File...\n...')

    # get the station ID from the pil
    stn_name = _get_station_name(pil)

    # ----------------------------------------------------------
    # here we're actually writing the text file
    with open(f'./output/__Supplemental_{year}_CLA{pil[3:]}_Data.txt', 'w') as f:

        f.write('IMPORTANT INFO ABOUT THIS PRODUCT:\n')
        f.write("ALL DATA SHOULD BE MANUALLY QC'D AND CHECKED AGAINST THE CLM/CF6 PRODUCTS\n\n")
        f.write(header)
        f.write(f'{year} SUPPLEMENTAL ANNUAL CLIMATE DATA FOR {stn_name}\n')
        f.write(f'{header}')
        f.write('\n(DFN = DEPARTURE FROM NORMAL)\n')

        # first up, the annual temperature summary table
        _make_temp_table(data_dict, f, months, year, stn_name)

        # next, write the annual precip summary table
        _make_precip_table(data_dict, f, months, year, stn_name)

        # next, write the annual snow summary table
        #_make_snow_table(data_dict, f, months, year, stn_name)

        # make the misc data summary
        _make_misc_data(data_dict, f)

        print('Done Writing New Text File...\n...')
        print(f'New File Saved As: ./output/Supplemental_{year}_CLA{pil[3:]}_Data.txt\n')


        return f.close()

###########################################################################################
###########################################################################################

# -----------------------------------------------------------
# lets import a text file structured as a dictionary to make our lives easier
# with open('./2021_LZK_Annual_Summary.txt', 'r') as f:
#     json_str = f.read()
#
# f.close()
# data_dict = json.loads(json_str)


# -----------------------------------------------------------
# will need to incorporate these into a function call,
# preferably as sys arg's for command line operations
pil = 'CLMLZK'
year = 2024
load_dict = f'{year}_{pil[3:]}_Annual_Summary.txt'

# ----------------------------------------------------------
# declare some globals as constants that will be used in the
# function all
table_sep = f'-'*93 + f'\n'
header = f'='*93 + f'\n'
space = ' '


def main(pil, year, load_dict = False, save_dict = False):

    # here we define the range of months, based on the entered
    # command line year param
    months = _month_range(year)

    if load_dict:
        with open(f'./test_files/{load_dict}', 'r') as f:
            json_str = f.read()
        f.close()
        data_dict = json.loads(json_str)
    else:
        # this is for when we're going to run the whole function
        data_dict = construct_data_dict(pil, months, year)

    if save_dict:
        # for quickly saving a dictionary to a text file, for easier testing later on
        with open(f'./test_files/{year}_{pil[3:]}_Annual_Summary.txt', 'w') as f:
            f.write(json.dumps(data_dict))

    # now write the text file
    write_textfile(data_dict, pil, months, year)

# ----------------------------------------------------------
# For the function call
if __name__ == '__main__':

    # if > 1, then we are running from the command line, and
    # we need to present args to make the script work
    if len(sys.argv) > 1:

        pil = str(sys.argv[1]) # e.g., 'CLMLZK'
        year = int(sys.argv[2]) # e.g., 2024

        try:
            load_dict = str(sys.argv[3]) # e.g. '2024_LZK_Annual_Summary.txt', default is False
            if os.path.exists(f'./test_files/{load_dict}'):
                pass
            else:
                load_dict = False
        except IndexError:
            load_dict = False

        try:
            save_dict = bool(sys.argv[4]) # e.g. True or False, default is False
        except IndexError:
            save_dict = False

        main(pil, year, load_dict, save_dict)


    # -----------------------------------------------------------------
    # this is for when we run in a text editor with predefined args,
    # not parsed from the command line
    else:

        main(pil, year, load_dict)


