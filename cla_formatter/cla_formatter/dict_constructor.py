# Import our modules

# BEAUTIFUL SOUP
from bs4 import BeautifulSoup

# BUILT IN
import calendar
import copy
import itertools
from operator import itemgetter
import os, sys
import requests
from urllib.request import urlopen

# MISC
from datetime import datetime #timezone
import json
import numpy.ma as ma
import numpy as np
import pandas as pd
import textwrap

# XMACIS PARSER
from xmacis_parser import xmacis_mainfunc

# UTILS
#from .utils import _month_range

###########################################################################################

'''
dict_constructor.py
Written by: Erik Green, WFO Little Rock, Aug 2024

dict_constructor.py fetches the API urls for each CLM product for a site, 
through the IEM API. From there, a main data dictionary is constructed.

Notes, per Thomas:
On the CLM, the greatest 24 hr total is the true 24 hr total (can overlap days)
            the greatest storm total is the actual calendar day max
'''

###########################################################################################
# dict_constructor.py- Get the url's for each CLM and CF6 sheet,
#                      and construct a dictionary with that data
###########################################################################################

# ----------------------------------------------------------
# Helping Sub-Functions
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

# ----------------------------------------------------------
# Main Working Functions
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
# Main Script - construct_data_dict()
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


###########################################################################################
# Below is for testing, otherwise, construct_data_dict will simply be imported
###########################################################################################

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
#
#     '''
#
#     # set up current date parameters to check with
#     current_date = dt.now()
#     current_year = current_date.year
#     current_month = current_date.month
#
#     # check if current year
#     # if it is current year, then we need to automatically define a
#     # range of months based on the current month
#     if year == current_year:
#         return [x for x in range(current_month + 1)][2:]  # set up the months to be the last most recently completed month, starting with Feb for Jan product
#
#     # if it is not the current year, then set up a list with all months
#     elif year != current_year:
#         return [x for x in range(13)][2:]  # start in feb for jan CLM product



# if __name__ == '__main__':
#
#     pil = 'CLMHRO'
#     year = 2023
#     months = _month_range(year)
#     data_dict = construct_data_dict(pil, months, year)
#
#     # for quickly saving a dictionary to a text file, for easier testing
#     with open(f'./test_files/{year}_{pil[3:]}_Annual_Summary.txt', 'w') as f:
#         f.write(json.dumps(data_dict, indent = 4))
#
#     f.close()