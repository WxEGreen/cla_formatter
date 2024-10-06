# Import our modules

# BUILT IN
#import calendar
#import itertools
#from itertools import groupby
#from operator import itemgetter
#import os, sys
#import requests
#from urllib.request import urlopen

# MISC
#import argparse
from datetime import datetime
#import json
#import numpy.ma as ma
#import numpy as np
#import pandas as pd
#import textwrap

# DICT CONSTRUCTOR
#from .dict_constructor import construct_data_dict

# RECORD DATA
#from record_data import assemble_records

# UTILS
from utils import _get_station_name
from utils import _make_supplemental_data_text
from utils import _make_temp_table
from utils import _make_precip_table
from utils import _make_snow_table
from utils import _make_misc_data
from utils import _make_records_table


###########################################################################################

'''
CLA Supplemental Data Formatter
Written by: Erik Green, WFO Little Rock, Aug 2024

formatter_main.py utilizes the functions:
    dict_constructor.py
    xmacis_parser.py
    record_data.py
    
The script first utilizes dict_constructor.py, where it fetches the API urls 
for each CLM product for a site, through the IEM API. From there, a main data 
dictionary is constructed. Additionally, xmacis_parser.py will utilize the XMACIS
API to find ranking and recency data for climate paramters and add them to a 
dictionary. The dictionaries can be saved as output, depending on the args.

In formatter_main.py, the main data dictionary is utilized to generate a text file 
that contains all pertinent annual supplemental data to be included in the CLA product
for a climate site.


'''


###########################################################################################
# formatter_main.py - Now that we have the dictionary constructed, lets write the text file...
###########################################################################################
# # ----------------------------------------------------------
# # Helping Sub-Functions
# # ----------------------------------------------------------
#
# # ----------------------------------------------------------
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
#
# # ----------------------------------------------------------
# def _get_station_name(pil):
#
#     '''
#     Parameters
#     ----------
#     pil : str, the six letter climate product pil, e.g. 'CLMLZK'
#
#     Returns
#     -------
#     stn_name : str, the three letter station ID, parsed from pil, e.g. 'LZK'
#     '''
#
#     if pil[3:] == 'LZK':
#         stn_name = 'NORTH LITTLE ROCK'
#     elif pil[3:] == 'LIT':
#         stn_name = 'LITTLE ROCK'
#     elif pil[3:] == 'HRO':
#         stn_name = 'HARRISON'
#     elif pil[3:] == 'PBF':
#         stn_name = 'PINE BLUFF'
#
#     return stn_name
#
# # ----------------------------------------------------------
#
# # ----------------------------------------------------------
# # Main Working Functions
# # ----------------------------------------------------------
# def _month_range(year):
#     '''
#     This function will define a range of months, based on the entered param, year.
#
#     Parameters
#     ----------
#     year : int, the year for which a range of months is being defined
#
#     Returns
#     ----------
#     months : list, a list of months as integers
#     current_year_flag : bool, set to True when we are in the current year, False when not the current year
#
#     '''
#
#     # set up current date parameters to check with
#     current_date = datetime.now()
#     current_year = current_date.year
#     current_month = current_date.month
#
#     # check if current year
#     # if it is current year, then we need to automatically define a
#     # range of months based on the current month
#     if year == current_year:
#         return [x for x in range(current_month + 1)][2:], True  # set up the months to be the last most recently completed month, starting with Feb for Jan product
#
#     # if it is not the current year, then set up a list with all months
#     elif year != current_year:
#         return [x for x in range(13)][2:], False  # start in feb for jan CLM product
# # ----------------------------------------------------------
#
# # ----------------------------------------------------------
# def _make_supplemental_data_text(data_dict, f, year, char_lim):
#
#     '''
#
#     Parameters
#     ----------
#     data_dict : dict, the main data dictionary that includes all necessary information
#     f : file, the open text file that is being written to
#     char_lim : int, the character limit for word wrapping in the text file
#
#     Returns
#     -------
#     None
#
#     '''
#
#
#     # -------------------------------------------------
#     print('Writing in Supplemental Summary Text...\n...')
#
#     # ----------------------------------------------------------
#     # instantiate a text wrapper for writing to the textfile
#     wrapper = textwrap.TextWrapper(width = char_lim)  # limits character wrapping
#
#     # ----------------------------------------------------------
#     # lets add the XMACIS Temp Data here
#     if not ma.is_masked(data_dict['xmacis_data']):
#         maxt_text = data_dict['xmacis_data'][0]
#         mint_text = data_dict['xmacis_data'][1]
#         avgt_text = data_dict['xmacis_data'][2]
#
#         f.write('\n.YEARLY TEMPERATURES...\n')
#         f.write(f'{wrapper.fill(maxt_text)}\n\n')
#         f.write(f'{wrapper.fill(mint_text)}\n\n')
#         f.write(f'{wrapper.fill(avgt_text)}\n\n')
#
#         # -------------------------------------------------
#         # add ranking statement
#         rank_statement = '**FOR RANKING PURPOSES SHOWN ABOVE, A YEAR WAS OMITTED IF IT WAS MISSING MORE THAN 7 DAYS WORTH OF DATA**'
#         f.write(f'{wrapper.fill(rank_statement)}\n\n\n')
#
#     else:
#         f.write('\n.YEARLY TEMPERATURES...\n')
#         f.write(f'\nUPDATED RANKED ANNUAL TEMPERATURE DATA FOR {year} IS NOT AVAILABLE YET.\n\n')
#
#     # -------------------------------------------------
#     # write in additional significant phenomena
#     f.write('.ADDITIONAL SIGNIFICANT EVENTS DURING THE YEAR...\n')
#
#     # -------------------------------------------------
#     # get the min/max SLP data
#     def _get_maxmin(val_lst, dt_lst):
#
#         _temp_lst = [val for val in val_lst if val != 'M']
#         idx = val_lst.index(np.max(_temp_lst))
#         dt = dt_lst[idx]
#
#         return np.max(_temp_lst), dt
#
#     # if the data exists and is not an empty list
#     if data_dict['cf6_data']['pres']['min_pres']:
#
#         min_slp, min_slp_dt = _get_maxmin(data_dict['cf6_data']['pres']['min_pres'],
#                                           data_dict['cf6_data']['pres']['min_pres_dates'])
#
#         f.write(f'MINIMUM SEA LEVEL PRESSURE WAS MEASURED AT {min_slp:.2f} INCHES ON {min_slp_dt}.\n\n')
#
#     if data_dict['cf6_data']['pres']['max_pres']:
#         max_slp, max_slp_dt = _get_maxmin(data_dict['cf6_data']['pres']['max_pres'],
#                                           data_dict['cf6_data']['pres']['max_pres_dates'])
#
#         f.write(f'MAXIMUM SEA LEVEL PRESSURE WAS MEASURED AT {max_slp:.2f} INCHES ON {max_slp_dt}.\n\n')
#
#
#     # -------------------------------------------------
#     # now lets add in wind data if it exists...
#
#     wsp = data_dict['cf6_data']['wind']['max_gst']
#     wdr = data_dict['cf6_data']['wind']['max_wdr']
#     dates = data_dict['cf6_data']['wind']['max_wd_dates']
#
#     def _format_windgusts(wsp, wdr, dates):
#
#         # format the date
#         m = [datetime.strptime(d, '%Y-%m-%d').strftime('%B').upper() for d in dates]
#         d = [_find_numeric_suffix(d.split('-')[-1]) for d in dates]
#
#         dts = [f'{m} {d}' for m, d in zip(m, d)]
#
#         for wsp, wdr, dts in zip(wsp, wdr, dts):
#             f.write(f'WINDS GUSTED TO {wsp} MPH/{wdr} DEGREES ON {dts}.\n\n')
#
#     if wsp:
#         _format_windgusts(wsp, wdr, dates)
#
#
#     # -------------------------------------------------
#     # write in the yearly rainfall and snowfall data
#     # get the XMACIS rain and snow data
#     if not ma.is_masked(data_dict['xmacis_data']):
#         pcpn_text = data_dict['xmacis_data'][3]
#         snow_text = data_dict['xmacis_data'][4]
#
#         f.write('\n.YEARLY RAINFALL...\n')
#         f.write(f'{wrapper.fill(pcpn_text)}\n\n')
#
#         f.write('\n.YEARLY SNOWFALL...\n')
#         f.write(f'{wrapper.fill(snow_text)}\n\n')
#
#     else:
#         f.write('\n.YEARLY RAINFALL...\n')
#         f.write(f'\nUPDATED RANKED ANNUAL RAINFALL DATA FOR {year} IS NOT AVAILABLE YET.\n')
#
#         f.write('\n.YEARLY SNOWFALL...\n')
#         f.write(f'\nUPDATED RANKED ANNUAL SNOWFALL DATA FOR {year} IS NOT AVAILABLE YET.\n')
#
# # ----------------------------------------------------------
# def _make_temp_table(data_dict, f, months, year, stn_name):
#     '''
#     This function will write in the annual temperature summary table to the supplemental CLA product text file.
#
#     Parameters
#     ----------
#     data_dict : dict, the main dictionary that includes all our main CLM product data
#     f : file, the text file that is currently open that is being written to
#     months : list, a list of months as integers
#     year : int, the climate year
#     stn_name : str, the three letter climate station ID
#
#     Returns
#     ----------
#     None
#
#     '''
#
#     # -------------------------------------------------
#     def consecutive(lst):
#
#         '''
#
#         This function will take a list of date strings, and organize consecutive or sequential days into tuples,
#         with the first and last day of the sequence in the tuple, or will simply create tuple with matching start
#         and end days for non-consecutive values.
#
#         Parameters
#         ----------
#         lst : list, list of date values as strings
#
#         Returns
#         -------
#         consec_days : list, a list of consecutive days as tuples
#         nonconsec_days : list, a list of non-consecutive days as tuples
#
#         '''
#
#         ranges = []
#         lst = sorted(lst) # sometimes the values are not properly sorted
#
#         for k, g in groupby(enumerate(lst), lambda x: int(x[0]) - int(x[1])):
#             group = (map(itemgetter(1), g))
#             group = list(map(int, group))
#             ranges.append([group[0], group[-1]])
#
#         consec_days = ['-'.join([f'{i[0]:02}', f'{i[1]:02}']) for i in ranges if i[0] != i[-1]]
#         nonconsec_days = [f'{i[0]:02}' for i in ranges if i[0] == i[-1]]
#
#         return consec_days, nonconsec_days
#
#     # -------------------------------------------------
#     def _find_annual_extreme_dates(temp_dates_lst, idx):
#
#         '''
#         This function will parse a temp dates list with a given index and return a string of the extreme dates
#
#         Parameters
#         ----------
#         temp_dates_lst : list, a list of the extreme temperature dates for a month
#         idx : int, the index value of the extreme temperature date for the year
#
#         Returns
#         ----------
#         dt : str, a string of the formatted extreme temp dates
#
#         '''
#
#         if isinstance(temp_dates_lst[idx], list):
#             # sort the days in ascending order
#             dt = sorted([int(i.split('/')[-1]) for i in temp_dates_lst[idx]])
#
#             # check for consecutive days here and format accordingly
#             consec_days, nonconsec_days = consecutive(dt)
#
#             if consec_days:
#                 dt = sorted(consec_days + nonconsec_days)  # add the two lists back together and sort them
#             dt = ','.join(f'{d}' for d in dt)  # join all items in dt into one string
#
#         else:
#             dt = temp_dates_lst[idx].split('/')[-1]
#         return dt
#
#     # -------------------------------------------------
#     print('Writing in Annual Temp Table...\n...')
#
#     # put together the data for iterating quickly
#     months = [calendar.month_name[i-1][:3].upper() for i in months]
#
#     avg_high_temps = [data_dict[m]['monthly_avg_temps_dfn'][0] for m in months]
#     avg_low_temps = [data_dict[m]['monthly_avg_temps_dfn'][1] for m in months]
#     avg_temps = [data_dict[m]['monthly_avg_temps_dfn'][2] for m in months]
#     avg_temps_dfn = [data_dict[m]['monthly_avg_temps_dfn'][3] for m in months]
#
#     high_temps = [data_dict[m]['monthly_max_min'][0] for m in months]
#     high_temp_dates = [data_dict[m]['high_dates'] for m in months]
#
#     low_temps = [data_dict[m]['monthly_max_min'][1] for m in months]
#     low_temp_dates = [data_dict[m]['low_dates'] for m in months]
#
#     # now lets write the table into the text file
#     arr = [f'{year} TEMPERATURE AVERAGES AND EXTREMES', f'{stn_name}, ARKANSAS']
#     f.write('\n{:60}{:60}\n'.format(*arr))
#     f.write(table_sep)
#     f.write('             AVERAGE TEMPERATURES             |            TEMPERATURE EXTREMES\n')
#
#     arr = ['MONTH', 'HIGH', 'LOW', 'MONTHLY', 'DFN', '|', 'MAX', 'DATE(S)', 'MIN', 'DATE(S)']
#     f.write('{:9}{:9}{:9}{:12}{:7}{:5}{:7}{:15}{:7}{:9}\n'.format(*arr))
#
#     f.write(table_sep)
#
#     # for loop begins
#     # order is as follows: month, avg max, avg min, avg mean, avg dfn, max, max dates, min, min dates
#     for m, avgmx, avgmn, avgt, dfn, mx, mxdt, mn, mndt in zip(
#                                                             months,
#                                                             avg_high_temps, avg_low_temps, avg_temps, avg_temps_dfn,
#                                                             high_temps, high_temp_dates,
#                                                             low_temps, low_temp_dates
#                                                              ):
#
#         if dfn > 0.0:
#             dfn = f'+{dfn}'
#
#         # format the max temp dates
#         if isinstance(mxdt, list):
#             mxdt = [i.split('/')[-1] for i in mxdt]
#
#             # check for consecutive days here and format accordingly
#             consec_days, nonconsec_days = consecutive(mxdt)
#             mxdt = sorted(consec_days + nonconsec_days)  # add the two lists back together and sort them
#             mxdt = ','.join(f'{d}' for d in mxdt)  # join all items in dt into one string
#
#             #mxdt = ",".join(sorted(mxdt, key=int))  # sort the days in ascending order
#         else:
#             mxdt = mxdt.split('/')[-1]
#
#         # format the min temp dates
#         if isinstance(mndt, list):
#             mndt = [i.split('/')[-1] for i in mndt]
#
#             # check for consecutive days here and format accordingly
#             consec_days, nonconsec_days = consecutive(mndt)
#             mndt = sorted(consec_days + nonconsec_days)  # add the two lists back together and sort them
#             mndt = ','.join(f'{d}' for d in mndt)  # join all items in dt into one string
#
#             #mndt = ",".join(sorted(mndt, key=int))  # sort the days in ascending order
#         else:
#             mndt = mndt.split('/')[-1]
#
#         # now write in the data to the text file
#         arr = [f'{m}', f'{avgmx}', f'{avgmn}', f'{avgt}', f'{dfn}', '|', f'{mx}', f'{mxdt}', f'{mn}', f'{mndt}']
#         f.write('{:9}{:9}{:9}{:11}{:8}{:5}{:7}{:15}{:7}{:9}\n'.format(*arr))
#         # for loop ends
#
#     # -------------------------------------------------
#     # next add in the annual summary data
#     f.write(table_sep)  # add a table separator
#
#     # compile the yearly avg max,min,avg temps and avg temp dfn
#     yrly_avgmx = np.round(np.mean(avg_high_temps), 1)
#     yrly_avgmn = np.round(np.mean(avg_low_temps), 1)
#     yrly_avg = np.round(np.mean(avg_temps), 1)
#     yrly_avg_dfn = np.round(np.mean(avg_temps_dfn), 1)
#
#     # if the dfn is > 0, then add '+' to the string
#     if yrly_avg_dfn > 0.0:
#         yrly_avg_dfn = f'+{yrly_avg_dfn}'
#
#
#     # -------------------------------------------------
#     # format the annual max temp date(s)
#     yrly_mx = np.max(high_temps)       # yearly max temp
#     idx = high_temps.index(yrly_mx)    # index value of the max temp
#
#     # if there are multiple values in idx
#     if high_temps.count(yrly_mx) > 1:
#         # index values of all occurrences
#         idx = [i for i, x in enumerate(high_temps) if x == yrly_mx]
#
#         # temp list
#         _dt_lst = []
#         for i in idx:
#             _dt_lst.append(_find_annual_extreme_dates(high_temp_dates, i))
#
#         yrly_mxdt = [f'{calendar.month_name[i + 1][:3].upper()} {dt}' for i, dt in zip(idx, _dt_lst)]
#         #yrly_mxdt = '/'.join([i for i in yrly_mxdt])
#
#     # for when there is only one idx
#     else:
#         mxdt = _find_annual_extreme_dates(high_temp_dates, idx) # return value with the extreme value date(s)
#         yrly_mxdt = f'{calendar.month_name[idx + 1][:3].upper()} {mxdt}' # formatted string of the extreme value date(s)
#
#
#     # -------------------------------------------------
#     # format the annual min temp date(s)
#     yrly_mn = np.min(low_temps)
#     idx = low_temps.index(yrly_mn)
#
#     # if there are multiple values in idx
#     if low_temps.count(yrly_mn) > 1:
#         # index values of all occurrences
#         idx = [i for i, x in enumerate(low_temps) if x == yrly_mn]
#
#         # temp list
#         _dt_lst = []
#         for i in idx:
#             _dt_lst.append(_find_annual_extreme_dates(low_temp_dates, i))
#
#         yrly_mndt = [f'{calendar.month_name[i + 1][:3].upper()} {dt}' for i, dt in zip(idx, _dt_lst)]
#         #yrly_mndt = '/'.join([i for i in yrly_mndt])
#
#     # for when there is only one idx
#     else:
#         mndt = _find_annual_extreme_dates(low_temp_dates, idx)
#         yrly_mndt = f'{calendar.month_name[idx + 1][:3].upper()} {mndt}'
#
#
#     # -------------------------------------------------
#     # add in handling for excessive numbers of same matching high/low days
#     # boolean flags
#     mx_flag = False
#     mn_flag = False
#
#     # if these values arrive as lists, then there are multiple months, otherwise, they'll be one string
#     if isinstance(yrly_mxdt, list):
#         mx_flag = True
#
#     if isinstance(yrly_mndt, list):
#         mn_flag = True
#
#     # -------------------------------------------------
#     # more than one date after splitting max and min values...
#     if mx_flag and mn_flag:
#
#         # if more values in yrly_mndt, then add extra to yrly_mxdt
#         if len(yrly_mxdt) < len(yrly_mndt):
#             if isinstance(yrly_mxdt, list): # we can only append if it is a list
#                 for i in range(np.abs(len(yrly_mxdt) - len(yrly_mndt))):
#                     yrly_mxdt.append('')
#             else:
#                 yrly_mxdt = [yrly_mxdt] # convert to a list if it is not one
#                 for i in range(np.abs(len(yrly_mxdt) - len(yrly_mndt))):
#                     yrly_mxdt.append('')
#
#         # if more values in yrly_mxdt, then add extra to yrly_mndt
#         elif len(yrly_mxdt) > len(yrly_mndt):
#             if isinstance(yrly_mndt, list): # we can only append if it is a list
#                 for i in range(np.abs(len(yrly_mxdt) - len(yrly_mndt))):
#                     yrly_mndt.append('')
#             else:
#                 yrly_mndt = [yrly_mndt] # convert to a list if it is not one
#                 for i in range(np.abs(len(yrly_mxdt) - len(yrly_mndt))):
#                     yrly_mndt.append('')
#
#         # lets try to conglomerate the two lists into one here with list comprehension...
#         mxmn = [[mx, mn] for mx, mn in zip(yrly_mxdt[1:], yrly_mndt[1:])]
#
#         arr = ['ANNUAL', f'{yrly_avgmx}', f'{yrly_avgmn}', f'{yrly_avg}', f'{yrly_avg_dfn}',
#                '|', f'{yrly_mx}', f'{yrly_mxdt[0]}', f'{yrly_mn}', f'{yrly_mndt[0]}']
#         f.write('{:9}{:9}{:9}{:11}{:8}{:5}{:7}{:15}{:7}{:9}\n'.format(*arr))
#
#         for i in mxmn:
#             arr = ['', f'{i[0]}', f'{i[1]}']
#             f.write('{:58}{:22}{:22}\n'.format(*arr))
#
#     # -------------------------------------------------
#     # only the max temps flagged for multiple values
#     elif mx_flag:
#         arr = ['ANNUAL', f'{yrly_avgmx}', f'{yrly_avgmn}', f'{yrly_avg}', f'{yrly_avg_dfn}',
#                '|', f'{yrly_mx}', f'{yrly_mxdt[0]}', f'{yrly_mn}', f'{yrly_mndt}']
#         f.write('{:9}{:9}{:9}{:11}{:8}{:5}{:7}{:15}{:7}{:9}\n'.format(*arr))
#
#         for mx in yrly_mxdt[1:]:
#             arr = ['', f'{mx}']
#             f.write('{:58}{:58}\n'.format(*arr))
#
#     # -------------------------------------------------
#     # only the min temps flagged for multiple values
#     elif mn_flag:
#         arr = ['ANNUAL', f'{yrly_avgmx}', f'{yrly_avgmn}', f'{yrly_avg}', f'{yrly_avg_dfn}',
#                '|', f'{yrly_mx}', f'{yrly_mxdt}', f'{yrly_mn}', f'{yrly_mndt[0]}']
#         f.write('{:9}{:9}{:9}{:11}{:8}{:5}{:7}{:15}{:7}{:9}\n'.format(*arr))
#
#         for mn in yrly_mndt[1:]:
#             arr = ['', f'{mn}']
#             f.write('{:80}{:80}\n'.format(*arr))
#
#     # -------------------------------------------------
#     # 'normal cases', no extra dates
#     else:
#         arr = ['ANNUAL', f'{yrly_avgmx}', f'{yrly_avgmn}', f'{yrly_avg}', f'{yrly_avg_dfn}',
#                '|', f'{yrly_mx}', f'{yrly_mxdt}', f'{yrly_mn}', f'{yrly_mndt}']
#         f.write('{:9}{:9}{:9}{:11}{:8}{:5}{:7}{:15}{:7}{:9}\n'.format(*arr))
#
# # ----------------------------------------------------------
# def _make_precip_table(data_dict, f, months, year, stn_name):
#     '''
#     This function will write in the annual rain summary table to the supplemental CLA product text file.
#
#     Parameters
#     ----------
#     data_dict : dict, the main dictionary that includes all our main CLM product data
#     f : file, the text file that is currently open that is being written to
#     months : list, a list of months as integers
#     year : int, the climate year
#     stn_name : str, the three letter climate station ID
#
#     Returns
#     ----------
#     None
#
#     '''
#
#     print('Writing in Annual Precip Table...\n...')
#
#     # # --------------------------------------------
#     # add table headers and column information
#
#     arr = [f'{year} RAINFALL, DEPARTURES, AND EXTREMES', f'{stn_name}, ARKANSAS']
#     f.write('\n\n{:60}{:60}\n'.format(*arr))
#     f.write(table_sep)
#
#     arr = ['MONTH', 'RAINFALL', 'DFN', 'MAX/CALENDAR DAY', 'MAX/24 HOURS']
#     f.write('{:13}{:15}{:18}{:28}{:28}\n'.format(*arr))
#     f.write(table_sep)
#
#     # --------------------------------------------
#     # now lets compile and organize the data
#     # put together the data for iterating quickly
#     months = [calendar.month_name[i-1][:3].upper() for i in months]
#
#     precip = [data_dict[m]['monthly_rain_and_dfn'][0] for m in months]
#     precip_dfn = [data_dict[m]['monthly_rain_and_dfn'][1] for m in months]
#
#     # the calendar day max is actually index 3 and 4
#     precip_mx_clndr = [data_dict[m]['max_clndr_24hr_rain'][2] for m in months] # previously idx 0
#     precip_mx_clndr_dates = [data_dict[m]['max_clndr_24hr_rain'][3] for m in months] # previously idx 1
#
#     # these are the true 24 hr totals, can overlap days
#     precip_mx_storm_total = [data_dict[m]['max_clndr_24hr_rain'][0] for m in months] # previously idx 2
#     precip_mx_storm_total_dates = [data_dict[m]['max_clndr_24hr_rain'][1] for m in months] # previously idx 3
#
#     # for loop begins
#     for m, pcp, dfn, pcp_mx_clndr, pcp_mx_clndr_dates, pcp_mx_storm_total, pcp_mx_storm_total_dates in zip(
#             months,
#             precip, precip_dfn,
#             precip_mx_clndr, precip_mx_clndr_dates,
#             precip_mx_storm_total, precip_mx_storm_total_dates
#     ):
#         # --------------------------------------------
#         # lets do some formatting here
#         # assign a sign to dfn if it is positive
#         if dfn >= 0.0:
#             dfn = f'+{dfn:.2f}'
#
#         # --------------------------------------------
#         # lets format the max calendar day rainfall dates here...
#         # for date in pcp_mx_clndr_dates:
#         bdt = list(filter(None, (pcp_mx_clndr_dates.split('TO')[0].split(' '))))[0]
#         edt = list(filter(None, (pcp_mx_clndr_dates.split('TO')[-1].split(' '))))[0]
#
#         # if the dates are the same, then format for one day
#         if bdt == edt:
#             clndr_mxdt = _find_numeric_suffix(bdt.split('/')[-1])
#
#         # if the dates are not the same, then format for multiple dates
#         elif bdt != edt:
#             bdt = _find_numeric_suffix(bdt.split('/')[-1])
#             edt = _find_numeric_suffix(edt.split('/')[-1])
#             clndr_mxdt = f'{bdt}-{edt}'
#
#         # clean up the final string to be entered
#         pcp_mx_clndr = f'{pcp_mx_clndr:.2f}/{clndr_mxdt}'
#
#
#         # --------------------------------------------
#         # lets format the max storm total rainfall dates here...
#         bdt = list(filter(None, (pcp_mx_storm_total_dates.split('TO')[0].split(' '))))[0]
#         edt = list(filter(None, (pcp_mx_storm_total_dates.split('TO')[-1].split(' '))))[0]
#
#         # if the dates are the same, then format for one day
#         if bdt == edt:
#             stormtotal_mxdt = _find_numeric_suffix(bdt.split('/')[-1])
#
#         # if the dates are not the same, then format for multiple dates
#         elif bdt != edt:
#             bdt = _find_numeric_suffix(bdt.split('/')[-1])
#             edt = _find_numeric_suffix(edt.split('/')[-1])
#             stormtotal_mxdt = f'{bdt}-{edt}'
#
#         # clean up the final string to be entered
#         stormtotal_mx = f'{pcp_mx_storm_total:.2f}/{stormtotal_mxdt}'
#
#         # write in the line of monthly data
#         #arr = [f'{m}', f'{pcp:.2f}', f'{dfn}', f'{pcp_mx_clndr}', f'{clndr_mxdt}', f'{pcp_mx_storm_total:.2f}', f'{stormtotal_mxdt}']
#         arr = [f'{m}', f'{pcp:.2f}', f'{dfn}', f'{pcp_mx_clndr}', f'{stormtotal_mx}']
#         f.write('{:13}{:14}{:19}{:28}{:28}\n'.format(*arr))
#         # end of for loop
#
#     # --------------------------------------------
#     # now lets add the annual data
#     yrly_rain = np.round(np.sum(precip), 2)
#     yrly_dfn = np.round(np.sum(precip_dfn), 2)
#
#     # if the yrly precip dfn is >= 0.0, then assign a positive sign in the string
#     if yrly_dfn >= 0.0:
#         yrly_dfn = f'+{yrly_dfn}'
#
#     # -------------------------------------------------
#     def _find_annual_extreme_dates(pcp_dates_lst, idx):
#
#         '''
#         This function will parse a precip dates list with a given index and return a string of the extreme dates
#
#         pcp_dates_lst : list, a list of the extreme precip dates for a month
#         idx : int, the index value of the extreme precip date for the year
#
#         returns:
#         yrly_mxdt : str, a string of the formatted extreme precip dates
#
#         '''
#
#         bdt = list(filter(None, (pcp_dates_lst[idx].split('TO')[0].split(' '))))[0]
#         edt = list(filter(None, (pcp_dates_lst[idx].split('TO')[-1].split(' '))))[0]
#
#         if bdt == edt:
#             yrly_mxdt = bdt.split('/')[-1]
#
#         elif bdt != edt:
#             bdt = bdt.split('/')[-1]
#             edt = edt.split('/')[-1]
#             yrly_mxdt = f'{bdt}-{edt}'
#
#         return yrly_mxdt
#
#     # -------------------------------------------------
#
#     # yearly calendar day max precip date
#     clndr_pcp_mx = np.max(precip_mx_clndr)
#     idx = precip_mx_clndr.index(clndr_pcp_mx)  # index value of the calendar day max precip
#
#     clndr_max_pcp_dt = _find_annual_extreme_dates(precip_mx_clndr_dates, idx)
#     yrly_clndr_pcp_mx = f'{calendar.month_name[idx + 1][:3].upper()} {clndr_max_pcp_dt}'
#
#     # -------------------------------------------------
#
#     # yearly daily storm total max precip date
#     storm_total_pcp_mx = np.max(precip_mx_storm_total)
#     idx = precip_mx_storm_total.index(storm_total_pcp_mx)
#
#     storm_total_pcp_max_dt = _find_annual_extreme_dates(precip_mx_storm_total_dates, idx)
#     yrly_storm_total_pcp_max_dt = f'{calendar.month_name[idx + 1][:3].upper()} {storm_total_pcp_max_dt}'
#
#     # -------------------------------------------------
#     # write in the annual precip summary data
#     f.write(table_sep)
#     arr = ['ANNUAL', f'{yrly_rain:.2f}', f'{yrly_dfn}', f'{clndr_pcp_mx:.2f}/{yrly_clndr_pcp_mx}', f'{storm_total_pcp_mx:.2f}/{yrly_storm_total_pcp_max_dt}']
#     f.write('{:13}{:14}{:19}{:28}{:28}\n'.format(*arr))
#
# # ----------------------------------------------------------
# def _make_snow_table(data_dict, f, months, year, stn_name):
#     '''
#     This function will write in the annual snow summary table to the supplemental CLA product text file.
#
#     Parameters
#     ----------
#     data_dict : dict, the main dictionary that includes all our main CLM product data
#     f : file, the text file that is currently open that is being written to
#     months : list, a list of months as integers
#     year : int, the climate year
#     stn_name : str, the three letter climate station ID
#
#     Returns
#     ----------
#     None
#
#     '''
#     # --------------------------------------------
#     def consecutive(lst):
#
#         '''
#
#         This function will take a list of date strings, and organize consecutive or sequential days into tuples,
#         with the first and last day of the sequence in the tuple, or will simply create tuple with matching start
#         and end days for non-consecutive values.
#
#         Parameters
#         ----------
#         lst : list, list of date values as strings
#
#         Returns
#         -------
#         consec_days : list, a list of consecutive days as tuples
#         nonconsec_days : list, a list of non-consecutive days as tuples
#
#         '''
#
#         # format the incoming dates to integers and sort them
#         # sometimes our list will need this, sometimes it will not, so add error handling
#         try:
#             lst = sorted([int(x.split('/')[-1]) for x in lst])
#         except AttributeError:
#             pass
#
#         ranges = []
#         for k, g in groupby(enumerate(lst), lambda x: x[0] - x[1]):
#             group = (map(itemgetter(1), g))
#             group = list(map(int, group))
#             ranges.append([group[0], group[-1]])
#
#         consec_days = ['-'.join([f'{i[0]:02}', f'{i[1]:02}']) for i in ranges if i[0] != i[-1]]
#         nonconsec_days = [f'{i[0]:02}' for i in ranges if i[0] == i[-1]]
#
#         return consec_days, nonconsec_days
#     # --------------------------------------------
#     print('Writing in Annual Snow Table...\n...')
#
#     # --------------------------------------------
#     # add table headers and column information
#     arr = [f'{year} SNOWFALL, DEPARTURES, AND EXTREMES', f'{stn_name}, ARKANSAS']
#     f.write('\n\n{:60}{:60}\n'.format(*arr))
#     f.write(table_sep)
#
#     arr = ['MONTH', 'SNOW', 'DFN', 'MAX/CALENDAR DAY', 'MAX/24 HOUR', 'GREATEST DEPTH/DATE(S)']
#     f.write('{:8}{:10}{:10}{:23}{:18}{:18}\n'.format(*arr))
#     f.write(table_sep)
#
#     # --------------------------------------------
#     # now lets compile and organize the data
#     # put together the data for iterating quickly
#     snow_months = ['JAN', 'FEB', 'MAR', 'APR', 'OCT', 'NOV', 'DEC']  # these are the months we need for the snow table summary
#
#     # filter out snow_months based on our input of months
#     snow_months = [calendar.month_name[m - 1][:3].upper() for m in months if
#                        calendar.month_name[m - 1][:3].upper() in snow_months]
#
#     snow = [data_dict[m]['monthly_snow_sdepth_dfn'][0] for m in snow_months]
#     snow_dfn = [data_dict[m]['monthly_snow_sdepth_dfn'][1] for m in snow_months]
#
#     # data from the CLM, this tends to be flaky sometimes....
#     # clm_mx_sdepth = [data_dict[m]['monthly_snow_sdepth_dfn'][2] for m in snow_months]
#     # clm_mx_sdepth_dt = [data_dict[m]['monthly_snow_sdepth_dfn'][3] for m in snow_months]
#     #
#     # clm_mx_24hr_snow = [data_dict[m]['max_clndr_24hr_snow'][0] for m in snow_months]
#     # clm_mx_24hr_snow_dt = [data_dict[m]['max_clndr_24hr_snow'][1] for m in snow_months]
#
#     # data from the CF6 sheet
#     cf6_mx_sdepth = [data_dict['cf6_data'][m]['max_sdepth'][0] for m in snow_months]
#     cf6_mx_sdepth_dt = [data_dict['cf6_data'][m]['max_sdepth'][1] for m in snow_months]
#
#     cf6_mx_24hr_snow = [data_dict['cf6_data'][m]['max_clndr_24hr_snow'][0] for m in snow_months]
#     cf6_mx_24hr_snow_dt = [data_dict['cf6_data'][m]['max_clndr_24hr_snow'][1] for m in snow_months]
#
#     # for loop begins
#     for m, sn, dfn, mx_sd, mx_sd_dt, mx_24hr_sn, mx_24hr_sn_dt in zip(
#             snow_months,
#             snow, snow_dfn,
#             # clm_mx_sdepth, clm_mx_sdepth_dt,
#             # clm_mx_24hr_snow, clm_mx_24hr_snow_dt
#
#             # lets try using the snow data from the CF6
#             cf6_mx_sdepth, cf6_mx_sdepth_dt,
#             cf6_mx_24hr_snow, cf6_mx_24hr_snow_dt
#     ):
#
#         # adjust dfn parameters
#         if dfn > 0.0:
#             dfn = f'+{dfn}'
#
#         elif dfn == 0.0:
#             dfn = f' {dfn}'
#
#         # --------------------------------------------
#         # lets format the max calendar day and 24 hr snowfall dates here...
#         # first check if max 24 hr snow is > 0.0
#         if not isinstance(mx_24hr_sn, str) and mx_24hr_sn != 0.0: # we have to include exception for string 'T'
#             dt = _find_numeric_suffix(mx_24hr_sn_dt.split('/')[-1])
#             mx_24hr_sn = f'{mx_24hr_sn:.1f}/{dt}'
#
#         # for when max 24 hr snow is 0.0
#         elif not isinstance(mx_24hr_sn, str) and mx_24hr_sn == 0.0: # we have to include exception for string 'T'
#             mx_24hr_sn = '0.0'
#
#         # for when max 24 hr snow is 'T'
#         elif isinstance(mx_24hr_sn, str): # this should handle Trace 'T' values
#             dt = _find_numeric_suffix(mx_24hr_sn_dt.split('/')[-1])
#             mx_24hr_sn = f'{mx_24hr_sn}/{dt}'
#
#         # --------------------------------------------
#         # lets format the max snow depth/dates
#         if mx_sd > 0.0:
#             if isinstance(mx_sd_dt, list):
#
#                 consec_days, nonconsec_days = consecutive(mx_sd_dt) # find sequences of consecutive days if they exist
#                 if consec_days:
#                     _consec_days = [i.split('-') for i in consec_days] # split the consec day strings
#
#                     # use list comprehension to add the numeric suffices to the consec day strings
#                     _consec_days = ['-'.join([_find_numeric_suffix(i[0]), _find_numeric_suffix(i[1])]) for i in _consec_days]
#
#                     # use list comprehension to add numeric suffices to the non consec day strings
#                     nonconsec_days = [_find_numeric_suffix(i) for i in nonconsec_days]
#
#                     # now lets add them together and join to form one string
#                     dt = _consec_days + nonconsec_days
#                     dt = ",".join(d for d in dt)
#
#                 # consec_days is an empty list, then all values will be in nonconsec_days
#                 else:
#                     dt = [_find_numeric_suffix(i) for i in nonconsec_days]
#                     dt = ",".join(d for d in dt)
#
#             # if there is only one date to handle, not a list of values
#             else:
#                 dt = _find_numeric_suffix(mx_sd_dt.split('/')[-1])
#
#             # set up the max snow depth and the date(s) string
#             mx_sd = f'{int(mx_sd)}/{dt}'
#
#         # if max snow depth is just 0, then set to '0' string
#         else:
#             mx_sd = '0'
#
#         # now let's write in the monthly total/max data to the table
#         arr = [f'{m}', f'{sn}', f'{dfn}', f'{mx_24hr_sn}', f'{mx_24hr_sn}', f'{mx_sd}']
#         f.write('{:8}{:9}{:11}{:23}{:18}{:18}\n'.format(*arr))
#         # end of for loop
#
#     # -----------------------------------------------------------
#     # now lets add the annual data max values at the bottom of the table
#     # -----------------------------------------------------------
#     # Function for finding sum or max values of snow parameters
#     def _snow_total(snow, key):
#         '''
#
#         This function will perform three operations, including finding annual snow total, finding max
#         calendar day snow total, and will find the annual max snow depth value, for values in the arg snow.
#
#         Parameters
#         ----------
#         snow : list, list of snow fall data, including monthly totals, daily max values, and monthly snow depth
#         key : str, key value that determines which operation to perform, key values include:
#                     -- 'annual_snow'
#                     -- 'daily_max_snow'
#                     -- 'snow_depth'
#
#         Returns
#         -------
#         total or max value : float or int, depending on key, float (snow total or max), int (snow depth)
#
#         '''
#
#         # ----------------------------------------------------
#         # if we are handling annual snow fall totals
#         if key in 'annual_snow':
#             # get the annual snowfall total
#             sn_filtered = [s for s in snow if s != 'T']  # filter out trace values
#
#             # if the two lists are equal after filtering, then we filtered no T's
#             if len(snow) == len(sn_filtered):
#                 return np.round(np.sum(sn_filtered), 1)
#
#             # if the two lists are not equal after filtering, then we filtered T's out...
#             elif len(snow) != len(sn_filtered):
#                 # if the sum is still 0.0, then annual snowfall was only trace 'T'
#                 if np.sum(sn_filtered) == 0.0:
#                     return 'T'
#
#                 # if the sum is not 0.0 after filtering out trace values, find the sum
#                 elif np.sum(sn_filtered) > 0.0:
#                     return np.round(np.sum(sn_filtered), 1)
#
#         # ----------------------------------------------------
#         # this will find the single calendar day max
#         if key in 'daily_max_snow':
#             sn_filtered = [s for s in snow if s != 'T']  # filter out trace values
#
#             # if the two lists are equal after filtering, then we filtered no T's
#             if len(snow) == len(sn_filtered):
#                 return np.max(sn_filtered)
#
#             # if the two lists are not equal after filtering, then we filtered T's out...
#             elif len(snow) != len(sn_filtered):
#                 # if the max is still 0.0 after filtering out trace values, then max was trace 'T'
#                 if np.max(sn_filtered) == 0.0:
#                     return 'T'
#
#                 # if the max is greater than 0.0,
#                 elif np.max(sn_filtered) > 0.0:
#                     return np.max(sn_filtered)
#
#         # ----------------------------------------------------
#         # this will find the max snow depth
#         if key in 'snow_depth':
#             # should not be a problem with snow depth, since it is in whole inches...
#             sn_filtered = [s for s in snow if s != 'T']  # filter out trace values
#             return np.max(sn_filtered)
#
#     # -------------------------------------------
#     # get the annual snowfall total
#     yrly_sn = _snow_total(snow, key = 'annual_snow')
#
#     # -------------------------------------------
#     # get the annual snow dfn, and format accordingly
#     yrly_sn_dfn = np.round(np.sum(snow_dfn), 1)
#     if yrly_sn_dfn > 0.0:
#         yrly_sn_dfn = f'+{yrly_sn_dfn}'
#
#     elif yrly_sn_dfn == 0.0:
#         yrly_sn_dfn = ' 0.0'
#
#     # -------------------------------------------------
#     # get the annual max calendar day snow
#     sn_clndr_mx = _snow_total(cf6_mx_24hr_snow, key = 'daily_max_snow')
#
#
#     # -------------------------------------------
#     # get the max calendar day snow value dates, which will also be the 24 hr max value...
#     def _find_annual_extreme_dates(snow_dates_lst, idx):
#
#         '''
#         This function will parse a snow dates list with a given index and return a string of the extreme dates
#
#         Parameters
#         ----------
#         temp_dates_lst : list, a list of the extreme snow dates for a month
#         idx : int, the index value of the extreme snow date for the year
#
#         Returns
#         ----------
#         dt : str, a string of the formatted extreme snow dates
#
#         '''
#
#         if isinstance(snow_dates_lst[idx], list):
#             # sort the days in ascending order
#             dt = sorted([int(i.split('/')[-1]) for i in snow_dates_lst[idx]])
#             dt = '/'.join(f'{d:02}' for d in dt)
#         else:
#             dt = snow_dates_lst[idx].split('/')[-1]
#         return dt
#     # -------------------------------------------
#     def _find_annual_extreme_sd_dates(temp_dates_lst, idx):
#
#         '''
#         This function will parse a temp dates list with a given index and return a string of the extreme dates
#
#         Parameters
#         ----------
#         temp_dates_lst : list, a list of the extreme temperature dates for a month
#         idx : int, the index value of the extreme temperature date for the year
#
#         Returns
#         ----------
#         dt : str, a string of the formatted extreme temp dates
#
#         '''
#
#         if isinstance(temp_dates_lst[idx], list):
#             # sort the days in ascending order
#             dt = sorted([int(i.split('/')[-1]) for i in temp_dates_lst[idx]])
#             # check for consecutive days here and format accordingly
#             consec_days, nonconsec_days = consecutive(dt)
#
#             if consec_days:
#                 dt = sorted(consec_days + nonconsec_days)  # add the two lists back together and sort them
#             dt = ','.join(f'{d}' for d in dt)  # join all items in dt into one string
#
#         else:
#             dt = temp_dates_lst[idx].split('/')[-1]
#         return dt
#     # -------------------------------------------------
#     # First check if multiple instances...
#     if cf6_mx_24hr_snow.count(sn_clndr_mx) > 1:
#         # index values of all occurrences
#         idx = [i for i, x in enumerate(cf6_mx_24hr_snow) if x == sn_clndr_mx]
#
#         # temporary list
#         _dt_lst = []
#         for i in idx:
#             _dt_lst.append(_find_annual_extreme_dates(_dt_lst, i))
#
#         sn_clndr_mx = [f'{calendar.month_name[i + 1][:3].upper()} {dt}' for i, dt in zip(idx, _dt_lst)]
#         sn_clndr_mx = '/'.join([i for i in sn_clndr_mx])
#
#     # -------------------------------------------------
#     # if there is only one instance of the annual max calendar day snow total
#     else:
#         idx = cf6_mx_24hr_snow.index(sn_clndr_mx) # index value of the annual max calendar day snowfall
#
#         # if the annual calendar day max snow value is > 0.0, and not trace
#         if not isinstance(sn_clndr_mx, str) and sn_clndr_mx != 0.0: # we have to include exception for string 'T'
#             dt = cf6_mx_24hr_snow_dt[idx].split('/')[-1]
#             sn_clndr_mx = f'{sn_clndr_mx:.1f}/{calendar.month_name[idx + 1][:3].upper()} {dt}'
#
#         # for when max calendar day snow is 0.0
#         elif not isinstance(sn_clndr_mx, str) and sn_clndr_mx == 0.0: # we have to include exception for string 'T'
#             sn_clndr_mx = '0.0'
#
#         # for when max calendar day snow is 'T'
#         elif isinstance(sn_clndr_mx, str): # this should handle Trace 'T' values
#             dt = cf6_mx_24hr_snow_dt[idx].split('/')[-1]
#             sn_clndr_mx = f'{sn_clndr_mx:.1f}/{calendar.month_name[idx + 1][:3].upper()} {dt}'
#
#
#     # -------------------------------------------------
#     # get the annual max snow depth and the date
#     yrly_mx_sd = np.max(cf6_mx_sdepth)
#
#     if yrly_mx_sd > 0:
#         idx = cf6_mx_sdepth.index(yrly_mx_sd)
#
#         yrly_mx_sd_dt = _find_annual_extreme_sd_dates(cf6_mx_sdepth_dt, idx)
#         yrly_mx_sd = f'{int(yrly_mx_sd)}/{calendar.month_name[idx + 1][:3].upper()} {yrly_mx_sd_dt}'
#
#     # if yearly max snow depth is just 0, then set to string '0'
#     else:
#         yrly_mx_sd = '0'
#
#
#     # -------------------------------------------------
#     # Now lets write the annual summary data to the table
#     f.write(table_sep)
#     arr = ['ANN', f'{yrly_sn}', f'{yrly_sn_dfn}', f'{sn_clndr_mx}', f'{sn_clndr_mx}', f'{yrly_mx_sd}']
#     f.write('{:8}{:9}{:11}{:23}{:18}{:18}\n'.format(*arr))
#
# # ----------------------------------------------------------
# def _make_misc_data(data_dict, f):
#
#     '''
#
#     Parameters
#     ----------
#     data_dict : dict, the main dictionary that includes all our main CLM product data
#     f : file, the text file that is currently open that is being written to
#
#     Returns
#     -------
#     None
#
#     '''
#
#     print('Writing in Annual Miscellaneous Data...\n...')
#
#     # lets assemble some data
#     n1 = data_dict['misc_temp_data']['n days - minT <= 32']
#     n2 = data_dict['misc_temp_data']['n days - minT <= 20']
#     n3 = data_dict['misc_temp_data']['n days - minT <= 0']
#     n4 = data_dict['misc_temp_data']['n days - maxT <= 32']
#     n5 = data_dict['misc_temp_data']['n days - maxT >= 90']
#     n6 = data_dict['misc_temp_data']['n days - maxT >= 100']
#     n7 = data_dict['misc_temp_data']['n days - maxT >= 105']
#     n8 = data_dict['misc_temp_data']['n days - maxT >= 110']
#
#     # first/last dates
#     first_freeze = data_dict['misc_temp_data']['FIRST FREEZE']
#     last_freeze = data_dict['misc_temp_data']['LAST FREEZE']
#
#     first_80F = data_dict['misc_temp_data']['FIRST 80F']
#     last_80F = data_dict['misc_temp_data']['LAST 80F']
#
#     first_90F = data_dict['misc_temp_data']['FIRST 90F']
#     last_90F = data_dict['misc_temp_data']['LAST 90F']
#
#     first_100F = data_dict['misc_temp_data']['FIRST 100F']
#     last_100F = data_dict['misc_temp_data']['LAST 100F']
#
#     # Now lets write in the data
#     f.write(f'\n\n{table_sep}')
#     f.write('MISCELLANEOUS DATA (FIRST/LAST DATES, ETC.)\n')
#     f.write(table_sep)
#
#     # add in the misc data
#     f.write(f'DAYS WITH MINIMUMS AT OR BELOW 32 DEGREES.........................{n1:02}\n')
#     f.write(f'DAYS WITH MINIMUMS AT OR BELOW 20 DEGREES.........................{n2:02}\n')
#     f.write(f'DAYS WITH MINIMUMS AT OR BELOW 0 DEGREES..........................{n3:02}\n')
#     f.write(f'DAYS WITH MAXIMUMS AT OR BELOW 32 DEGREES.........................{n4:02}\n')
#     f.write(f'DAYS WITH MAXIMUMS AT OR ABOVE 90 DEGREES.........................{n5:02}\n')
#     f.write(f'DAYS WITH MAXIMUMS AT OR ABOVE 100 DEGREES........................{n6:02}\n')
#     f.write(f'DAYS WITH MAXIMUMS AT OR ABOVE 105 DEGREES........................{n7:02}\n')
#     f.write(f'DAYS WITH MAXIMUMS AT OR ABOVE 110 DEGREES........................{n8:02}\n')
#     f.write(f'LAST FREEZE.......................................................{last_freeze}\n')
#     f.write(f'FIRST 80-DEGREE DAY...............................................{first_80F}\n')
#     f.write(f'FIRST 90-DEGREE DAY...............................................{first_90F}\n')
#     f.write(f'FIRST 100-DEGREE DAY..............................................{first_100F}\n')
#     f.write(f'LAST 100-DEGREE DAY...............................................{last_100F}\n')
#     f.write(f'LAST 90-DEGREE DAY................................................{last_90F}\n')
#     f.write(f'LAST 80-DEGREE DAY................................................{last_80F}\n')
#     f.write(f'FIRST FREEZE......................................................{first_freeze}\n')
#
# # ----------------------------------------------------------
# def _make_records_table(f, pil, year):
#
#     '''
#
#     This function will compile all the new daily records for a climate site, which are stored in
#     './records' and format them into a new table on the text file.
#
#     Parameters
#     ----------
#     f : file, the text file that is currently open that is being written to
#     pil : str, the climate site that is being parsed, e.g. 'CLMLZK'
#     year : int, the year of climate data that is being parsed, YYYY
#
#     Returns
#     -------
#     None
#
#     '''
#
#     print('Writing in New Daily Records Data...\n...')
#
#     # first, create the record data dictionary
#     records_dict = assemble_records(pil, year)
#
#     # if records_dict is None, then we are running for a not current year, or don't have the correct record data saved in ./records
#     # write into f that no record data is available
#     if records_dict is None:
#         f.write(f'\n\n{table_sep}NEW DAILY RECORDS\n{table_sep}')
#         f.write(f'\nDAILY RECORD DATA {year} IS NOT AVAILABLE, OR THERE IS NOT MATCHING DAILY RECORD DATA IN ./records FOR {year}.\n')
#
#     # if records_dict was successfully created, then process and write records data to f
#     elif records_dict is not None:
#         new_records = records_dict['new_recs'] # new record info
#         prev_records = records_dict['old_recs'] # previous record info
#
#         # declare the constant header for temperature records
#         temp_rec_header = ['DATE', 'RECORD TYPE', 'NEW RECORD', 'OLD RECORD']
#
#
#         # -----------------------------------------------------------
#         def _write_temp_records(key1, key2, title):
#
#             '''
#
#             Parameters
#             ----------
#             key1 : str, a key value for the new_records and prev_records dictionaries,
#                         will fall under 'rec_mx' or 'rec_lw'
#             key2 : str, a key value for the new_records and prev_records dictionaries,
#                         will fall under 'rec_lwmx' or 'rec_mxlw'
#             title : str, the title header for what record category is being generated,
#                         i.e., 'RECORD HIGHS' or 'RECORD LOWS'
#
#             Returns
#             -------
#             None
#             '''
#
#             # the new record highs/lows
#             new_recs = [int(item.split(',')[1].split(' ')[0]) for item in new_records[key1]]
#             new_recs = [f'{rec} IN {year}' for rec in new_recs]
#             # the date of the new daily record
#             new_rec_dt = [item.split(',')[0] for item in new_records[key1]]
#             # the record type
#             rec_type1 = [item.split(',')[-1] for item in new_records[key1]]
#
#             # the new record low-highs or high-lows
#             new_rec_opp = [int(item.split(',')[1].split(' ')[0]) for item in new_records[key2]]
#             new_rec_opp = [f'{rec} IN {year}' for rec in new_rec_opp]
#             # the date of the new daily record
#             new_rec_opp_dt = [item.split(',')[0] for item in new_records[key2]]
#             # the record type
#             rec_type2 = [item.split(',')[-1] for item in new_records[key2]]
#
#             # previous record data
#             # the previous record data from the dictionary, includes value and year(s) as list of lists
#             prev_rec_data = prev_records[key1]
#
#             # the previous record values
#             prev_rec = [rec[0] for rec in prev_records[key1]]
#
#             # now get the previous record year(s) sorted
#             prev_yrs = []
#             for item in prev_rec_data:
#                 if len(item) > 2:
#                     item = item[1:]
#                     item = [str(item) for item in item]
#                     prev_yrs.append(', '.join(item))
#
#                 elif len(item) == 2:
#                     prev_yrs.append(str(item[1]))
#
#             # our final list of formatted strings for previous value and year(s)
#             prev_recs1 = []
#
#             for rec, dt in zip(prev_rec, prev_yrs):
#                 if len(dt) > 1:
#                     dt = ''.join(dt)
#                     prev_recs1.append(f'{rec} IN {dt}')
#
#                 else:
#                     prev_recs1.append(f'{rec} IN {dt}')
#
#             # record low highs
#             # because there is no easy way to get previous low-high or high-low data,
#             # simply append a Note statement for every extra low-high/high-low record
#             prev_recs2 = [f'|*CHECK {pil[3:]} CLIMATE BOOK OR RER FOR PREVIOUS RECORD/YEAR(S)*|' for i in new_rec_opp]
#
#             # combine the high and low-high data
#             new_recs = new_recs + new_rec_opp
#             new_rec_dt = new_rec_dt + new_rec_opp_dt
#             rec_type = rec_type1 + rec_type2
#             prev_recs = prev_recs1 + prev_recs2
#
#             # now sort the data
#             new_rec_dt, rec_type, new_recs, prev_recs = zip(*sorted(zip(new_rec_dt, rec_type, new_recs, prev_recs)))
#
#             if new_recs:
#                 f.write(f'\n{title}\n')
#                 f.write('{:8}{:32}{:18}{:18}\n'.format(*temp_rec_header))
#
#                 # for loop begins
#                 for dt, type, r, prev_r in zip(new_rec_dt, rec_type, new_recs, prev_recs):
#                     arr = [f'{dt}', f'{type}', f'{r}', f'{prev_r}']
#                     f.write('{:8}{:32}{:18}{:18}\n'.format(*arr))
#         # -----------------------------------------------------------
#
#         f.write(f'\n\n{table_sep}NEW DAILY RECORDS\n{table_sep}')
#
#         # write in record highs and low-highs
#         _write_temp_records('rec_mx', 'rec_lwmx', '.RECORD HIGHS...')
#
#         # write in record lows and high-lows
#         _write_temp_records('rec_lw', 'rec_mxlw', '.RECORD LOWS...')
#
#         # -----------------------------------------------------------
#         # now lets write in precip records
#         # declare the constant header for temperature records
#         pcp_rec_header = ['DATE', 'NEW RECORD', 'OLD RECORD']
#
#         # -----------------------------------------------------------
#         def _write_precip_records(key1, title):
#
#             '''
#
#             Parameters
#             ----------
#             key1 : str, a key value for the new_records and prev_records dictionaries,
#                         will fall under 'rec_pcp' or 'rec_sn'
#             title : str, the title header for what record category is being generated,
#                         i.e., 'RECORD RAINFALL' or 'RECORD SNOWFALL'
#
#             Returns
#             -------
#             None
#             '''
#
#             # the new record rainfall or snowfall
#
#             if key1 in 'rec_pcp':
#                 new_recs = [float(item.split(',')[-1].split(' ')[0]) for item in new_records[key1]]
#                 new_recs = [f'{rec:.2f} IN {year}' for rec in new_recs]
#
#             elif key1 in 'rec_sn':
#                 new_recs = [item.split(',')[-1].split(' ')[0] for item in new_records[key1]]
#                 new_recs = [f'{float(item):.1f}' if item != 'T' else item for item in new_recs]  # handle trace values 'T'
#                 new_recs = [f'{rec} IN {year}' for rec in new_recs]
#
#             # the date of the new daily record
#             new_rec_dt = [item.split(',')[0] for item in new_records[key1]]
#
#             # previous record data
#             # the previous record data from the dictionary, includes value and year(s) as list of lists
#             prev_rec_data = prev_records[key1]
#
#             # the previous record values
#             prev_rec = [rec[0] for rec in prev_records[key1]]
#
#             # now get the previous record year(s) sorted
#             prev_yrs = []
#             for item in prev_rec_data:
#                 # means there are multiple previous years that share the previous daily record
#                 if len(item) > 2:
#                     item = item[1:]
#                     item = [str(item) for item in item]
#                     prev_yrs.append(', '.join(item))
#
#                 # only one year for the previous daily record
#                 elif len(item) == 2:
#                     prev_yrs.append(str(item[1]))
#
#                 # happens for daily snow in the summer sometimes, where new daily record is 'T' for hail
#                 # simply add in note about no previous daily record
#                 elif item[0] == 0.0:
#                     prev_yrs.append('NO PREV DAILY RECORD')
#
#             # our final list of formatted strings for previous value and year(s)
#             prev_recs = []
#
#             for rec, dt in zip(prev_rec, prev_yrs):
#
#                 # multiple previous years, e.g. '2012, 2014, ...'
#                 if ',' in dt:
#                     prev_recs.append(f'{rec} IN {dt}')
#
#                 # no previous daily record, mainly for snow in summer (hail)
#                 elif dt == 'NO PREV DAILY RECORD':
#                     prev_recs.append(dt)
#
#                 else:
#                     prev_recs.append(f'{rec} IN {dt}')
#
#
#             # now lets write in the new record data
#             if new_recs:
#                 f.write(f'\n{title}\n')
#                 f.write('{:8}{:18}{:18}\n'.format(*pcp_rec_header))
#
#                 # for loop begins
#                 for dt, r, prev_r in zip(new_rec_dt, new_recs, prev_recs):
#                     arr = [f'{dt}', f'{r}', f'{prev_r}']
#                     f.write('{:8}{:18}{:18}\n'.format(*arr))
#                     # for loop ends
#         # -----------------------------------------------------------
#
#         # write in the record rainfall
#         _write_precip_records('rec_pcp', '.RECORD RAINFALL...')
#
#         # write in the record snow
#         _write_precip_records('rec_sn', '.RECORD SNOWFALL...')

# -----------------------------------------------------------
# Main Script - write_textfile()
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
# pil = 'CLMLZK'
# year = 2021
#
# load_dict = f'{year}_{pil[3:]}_Annual_Summary.txt'

# ----------------------------------------------------------
# declare some globals as constants that will be used in the
# functions above quite frequently...
table_sep = f'-'*93 + f'\n'
header = f'='*93 + f'\n'
space = ' '


# # ------------------------------------------------------------------
# # here is a module that can be imported to run in a colab notebook
# def run_cla_formatter_notebook(pil, year, save_dict):
#
#     # here we define the range of months, based on the entered
#     # command line year param
#     months, current_year_flag = _month_range(year)
#
#     # this is for when we're going to run the whole function
#     data_dict = construct_data_dict(pil, months, year)
#
#     # ------------------------------------------------------------------
#     # handle save_dict boolean,
#     # whether or not to save the dictionary we make if we are generating one
#     if save_dict:
#         # for quickly saving a dictionary to a text file, for easier testing later on
#         with open(f'./test_files/{year}_{pil[3:]}_Annual_Summary.txt', 'w') as f:
#             f.write(json.dumps(data_dict, indent = 4))
#
#     # ------------------------------------------------------------------
#     # handle current_year_flag
#     if not current_year_flag:
#         months.append(13) # this will make sure we get JAN-DEC, otherwise, it is only JAN-NOV
#
#     # ------------------------------------------------------------------
#     # now write the text file
#     write_textfile(data_dict, pil, months, year)

# ----------------------------------------------------------
# For the function call
# if __name__ == '__main__':
#
#     #main(pil, year, load_dict, save_dict)
#     cli_main(sys.argv) # for running in a command line




