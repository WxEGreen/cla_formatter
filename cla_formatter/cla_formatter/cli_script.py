# IMPORT OUR MODULES

# BUILT IN
import argparse
import sys

# MISC
import json

# DICT CONSTRUCTOR
from dict_constructor import construct_data_dict

# FORMATTER MAIN
from formatter_main import write_textfile

# UTILS
from utils import _month_range

'''
This script will be what we call when we want to run CLA FORMATTER from the command line interface

'''


# ----------------------------------------------------------
class CustomFormatter(argparse.ArgumentDefaultsHelpFormatter,
                      argparse.RawDescriptionHelpFormatter):
    pass

# ----------------------------------------------------------
def cli(argv):

    '''

    Command Line Interface

    Parameters
    ----------
    argv

    Returns
    -------
    args

    '''

    # -----------------------------------------------------------------
    # if > 1, then we are running from the command line, and
    # we need to present args to make the script work
    description = '''
    CLA FORMATTER
    -------------
    '''


    #if len(argv) > 1:
    # lets parse the arguments from the command line...
    parser = argparse.ArgumentParser(
        prog = argv[0],
        description = description,
        formatter_class = CustomFormatter)

    # establish the args
    # note, setting an arg name with '--' or '-' in it denotes it as optional, otherwise, names will be denoted as positional (required)
    parser.add_argument('pil',
                        type = str,
                        help = 'The 6 character PIL for a monthly climate product, e.g., CLMLZK')

    parser.add_argument('year',
                        type = int,
                        help = 'The year of climate products that are being generated, formatted as YYYY')

    # boolean args for loading a local file data dictionary
    parser.add_argument('--load_dict',
                        action = 'store_true',
                        help = f'A boolean variable that determines whether we are loading a previously saved data dictionary of climate product information\n' +
                               f'If False, does not load a file from the local disk; If True, will format a file name from pil and year, and load a file from the local disk;\n')

    # boolean args for loading a local file data dictionary
    parser.add_argument('--no_load_dict',
                        action = 'store_false',
                        help = f'A boolean variable that determines whether we are loading a previously saved data dictionary of climate product information\n' +
                               f'If False, does not load a file from the local disk; If True, will format a file name from pil and year, and load a file from the local disk;\n')

    # boolean args for saving a created data dictionary in main()
    parser.add_argument('--save_dict',
                        action = 'store_true',
                        help = f'A boolean variable that determines whether we are saving the data dictionary that gets generated, or loaded from a local directory\n' +
                               f'If False, does not save the generated data dictionary to the local disk; If True, will format a file name from pil and year, and save file to the local disk;\n')

    # boolean args for saving a created data dictionary in main()
    parser.add_argument('--no_save_dict',
                        action = 'store_false',
                        help = f'A boolean variable that determines whether we are saving the data dictionary that gets generated, or loaded from a local directory\n' +
                               f'If False, does not save the generated data dictionary to the local disk; If True, will format a file name from pil and year, and save file to the local disk;\n')


    args = parser.parse_args(argv[1:])

    return args
    #
    # else:
    #     return None # if we are not running through the command line, then no args to parse, set args to None
    #cc

# ----------------------------------------------------------
#def main(pil, year, load_dict, save_dict):
def cli_main(argv):

    # lets instantiate sys args...
    args = cli(argv)
    pil = args.pil
    year = args.year
    load_dict = args.load_dict
    save_dict = args.save_dict

    # here we define the range of months, based on the entered
    # command line year param
    months, current_year_flag = _month_range(year)

    # ------------------------------------------------------------------
    # handle load_dict boolean, whether we load a saved dictionary or not
    if load_dict:
        file = f'{year}_{pil[3:]}_Annual_Summary.txt'
        with open(f'./test_files/{file}', 'r') as f:
            json_str = f.read()
        f.close()
        data_dict = json.loads(json_str)
    else:
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




# ----------------------------------------------------------
# For the function call
if __name__ == '__main__':

    #main(pil, year, load_dict, save_dict)
    cli_main(sys.argv) # for running in a command line