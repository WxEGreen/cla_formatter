# Import our modules

# MISC
from datetime import datetime
import requests


'''
record_data.py
Written by: Erik Green, WFO Little Rock, Aug 2024

Notes, working here last on 8/4, we had just finished
pulling in the new record data, and getting the old
record data (high/low/rain/snow) from the IEM daily CLI API...

Need to add that data to the dictionary, and investigate
avenues for getting high-low and low-high, if possible...

Otherwise, we're onto incorporating the record data into the text file
'''


# ----------------------------------------------------------
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
# Main Function
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



if __name__ == '__main__':

    records_dict = assemble_records('CLMLZK', 2024)
    #print(records_dict)

















