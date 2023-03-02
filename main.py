import logging as log
import os
import re
import warnings

import pandas as pd

log.basicConfig(format='%(levelname)s - %(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=log.INFO)
warnings.simplefilter(action='ignore', category=UserWarning)

PATH = 'Pruzkum_data'

RESOURCE = 'Zdroj'
ADDRESS = 'Kraj'
SUBPROCESS_ABBR = 'Podproces zkratka'
PROCESS_NAME = 'Proces nazev'
PROCESS = 'Proces'
SUBPROCESS = 'Podproces'
ACTION_ABBR = 'Aktivita zkratka'
ACTION = 'Aktivity'
ROLE = 'Role'
TYPE = 'Typ podanání'
PHYSICALLY = 'Fyzicky podaná žádost'
DATA_BOX = 'Datová schránka'
ROBOT = 'Robot'
TIME = 'Čas'
TIME_UNENTERED_REQUEST = 'nezadaná žádost'
TIME_PARTIALLY_ENTERED_REQUEST = 'částečně zadaná žádost'
TIME_FULL_ENTERED_REQUEST = 'úplně zadaná žádost'
TIME_INCOMPLETE_REQUEST = 'nekompletní žádost'
TIME_COMPLETE_REQUEST = 'kompletní žádost'
FREQUENCY = 'ČETNOST'
NOTE = 'POZNÁMKA'
TABLE_END_ROW = 'Celkový čas (minuty)'

REGEX_ABBR_SUBPROCESS = re.compile('^[A-Z]+[0-9]+$')
REGEX_ABBR_ACTION_1 = re.compile('^[A-Z]+[\s,.][\d+].[\d+]$')
REGEX_ABBR_ACTION_2 = re.compile('^[A-Z]+[\d+].[\d+]$')

# get all the file names
file_names = []
for root, dirs, files in os.walk(PATH):
    for file in files:
        file_names.append(os.path.join(root, file))
log.info('Found %s files.', len(file_names))

table_data = []
for file_name in file_names:

    log.info('Reading exel file: %s', file_name)
    df = pd.read_excel(file_name)

    sheets_names = pd.ExcelFile(file_name).sheet_names
    log.info('Found sheets: %s', sheets_names)

    sheet_name = sheets_names[0]
    log.info("Get sheet: %s", sheet_name)

    # NaN change to ''
    excel_data_df = pd.read_excel(file_name, sheet_name=sheet_name).fillna('')

    init_headers = excel_data_df.columns.values.tolist()
    init_process = init_headers[0].strip()
    log.info('Header table contains: %s', init_process)

    number_column_time = None
    number_column_frequency = None
    number_column_physically = None
    number_column_data_box = None
    number_column_robot = None
    number_column_physically_incomplete = None
    number_column_physically_complete = None
    number_column_physically_frequency = None
    number_column_physically_note = None
    number_column_data_box_incomplete = None
    number_column_data_box_complete = None
    number_column_data_box_note = None
    number_column_robot_unentered = None
    number_column_robot_partially_entered = None
    number_column_robot_entered = None
    number_column_robot_note = None

    process = None
    for row in excel_data_df.itertuples():
        row_vals = list(row)

        values = {RESOURCE: file_name,
                  ADDRESS: file_name.split("\\")[1],
                  PROCESS_NAME: None,
                  SUBPROCESS: None,
                  ACTION: None,
                  ROLE: None,
                  TIME: None,
                  TYPE: None,
                  TIME_COMPLETE_REQUEST: None,
                  TIME_INCOMPLETE_REQUEST: None,
                  TIME_FULL_ENTERED_REQUEST: None,
                  TIME_PARTIALLY_ENTERED_REQUEST: None,
                  TIME_UNENTERED_REQUEST: None,
                  FREQUENCY: None,
                  NOTE: None,
                  SUBPROCESS_ABBR: None,
                  ACTION_ABBR: None}

        if init_process is not None:
            values[PROCESS_NAME] = init_process
            process = init_process
            init_process = None

        if process is None:
            prom_process = row_vals[2].strip()
            if prom_process == '':
                process = None
            else:
                process = prom_process

        if TABLE_END_ROW in row_vals[2]:
            values[PROCESS_NAME] = None
            process = None
            number_column_time = None
            number_column_frequency = None
            number_column_physically = None
            number_column_data_box = None
            number_column_robot = None
            number_column_physically_incomplete = None
            number_column_physically_complete = None
            number_column_physically_frequency = None
            number_column_physically_note = None
            number_column_data_box_incomplete = None
            number_column_data_box_complete = None
            number_column_data_box_note = None
            number_column_robot_unentered = None
            number_column_robot_partially_entered = None
            number_column_robot_entered = None
            number_column_robot_note = None
        else:
            if process is not None:
                abbreviation = row_vals[1].replace(" ", "")
                if REGEX_ABBR_SUBPROCESS.search(abbreviation):
                    values[SUBPROCESS_ABBR] = abbreviation
                    values[SUBPROCESS] = row_vals[2]
                if (REGEX_ABBR_ACTION_1.search(abbreviation) is not None) | (
                        REGEX_ABBR_ACTION_2.search(abbreviation) is not None):
                    values[ACTION_ABBR] = abbreviation
                    values[SUBPROCESS] = table_data[-1].get(SUBPROCESS)
                    values[ACTION] = row_vals[2]

            if len(row_vals) > 3:
                values[ROLE] = row_vals[3]
            values[PROCESS_NAME] = process

            if number_column_time is not None:
                values[TIME] = row_vals[number_column_time]

            if number_column_frequency is not None:
                values[FREQUENCY] = row_vals[number_column_frequency]

        if (values[SUBPROCESS_ABBR] is not None) | (values[ACTION_ABBR] is not None):
            already_added = False

            if number_column_physically is not None:
                val_physically = values.copy()
                val_physically[TYPE] = PHYSICALLY
                if number_column_physically_incomplete is not None:
                    val_physically[TIME_INCOMPLETE_REQUEST] = row_vals[number_column_physically_incomplete]
                if number_column_physically_complete is not None:
                    val_physically[TIME_COMPLETE_REQUEST] = row_vals[number_column_physically_complete]
                if number_column_physically_frequency is not None:
                    val_physically[FREQUENCY] = row_vals[number_column_physically_frequency]
                if number_column_physically_note is not None:
                    val_physically[NOTE] = row_vals[number_column_physically_note]
                table_data.append(val_physically)
                already_added = True

            if number_column_data_box is not None:
                val_data_box = values.copy()
                val_data_box[TYPE] = DATA_BOX
                if number_column_data_box_incomplete is not None:
                    val_data_box[TIME_INCOMPLETE_REQUEST] = row_vals[number_column_data_box_incomplete]
                if number_column_data_box_complete is not None:
                    val_data_box[TIME_COMPLETE_REQUEST] = row_vals[number_column_data_box_complete]
                if number_column_data_box_note is not None:
                    val_data_box[NOTE] = row_vals[number_column_data_box_note]
                table_data.append(val_data_box)
                already_added = True

            if number_column_robot is not None:
                val_robot = values.copy()
                val_robot[TYPE] = ROBOT
                if number_column_robot_unentered is not None:
                    val_robot[TIME_UNENTERED_REQUEST] = row_vals[number_column_robot_unentered]
                if number_column_robot_partially_entered is not None:
                    val_robot[TIME_PARTIALLY_ENTERED_REQUEST] = row_vals[number_column_robot_partially_entered]
                if number_column_robot_entered is not None:
                    val_robot[TIME_FULL_ENTERED_REQUEST] = row_vals[number_column_robot_entered]
                if number_column_robot_note is not None:
                    val_robot[NOTE] = row_vals[number_column_robot_note]
                table_data.append(val_robot)
                already_added = True

            if not already_added:
                table_data.append(values)

        index = 0
        for val in row:
            if isinstance(val, str):

                if (number_column_physically is not None) & (number_column_data_box is not None):
                    if (int(index) >= int(number_column_physically)) & (int(index) < int(number_column_data_box)):
                        if TIME_INCOMPLETE_REQUEST in val:
                            number_column_physically_incomplete = index
                        elif TIME_COMPLETE_REQUEST in val:
                            number_column_physically_complete = index
                        elif NOTE in val:
                            number_column_physically_note = index
                        elif FREQUENCY in val:
                            number_column_physically_frequency = index

                if number_column_data_box is not None:
                    if int(index) >= int(number_column_data_box):
                        if TIME_INCOMPLETE_REQUEST in val:
                            number_column_data_box_incomplete = index
                        elif TIME_COMPLETE_REQUEST in val:
                            number_column_data_box_complete = index
                        elif NOTE in val:
                            number_column_data_box_note = index

                if number_column_robot is not None:
                    if int(index) >= int(number_column_robot):
                        if TIME_UNENTERED_REQUEST in val:
                            number_column_robot_unentered = index
                        elif TIME_PARTIALLY_ENTERED_REQUEST in val:
                            number_column_robot_partially_entered = index
                        elif TIME_FULL_ENTERED_REQUEST in val:
                            number_column_robot_entered = index
                        elif NOTE in val:
                            number_column_robot_note = index

            if row_vals[1] == PROCESS:
                if val == PHYSICALLY:
                    number_column_physically = index
                elif val == DATA_BOX:
                    number_column_data_box = index
                elif val == ROBOT:
                    number_column_robot = index

                if val == TIME:
                    number_column_time = index

                if val == FREQUENCY:
                    number_column_frequency = index

            index = index + 1

OUTPUT_FILE_NAME = 'output.xlsx'

# write data to Excel file
df = pd.DataFrame.from_dict(table_data)
log.info("Data for Excel file: %s", df)
df.to_excel(OUTPUT_FILE_NAME, index=False)
