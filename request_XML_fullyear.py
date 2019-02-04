# File that requests file from OASIS API, returns XML
# Adapted from PYISO Repository
# ROBERT SPRAGG: FEBRUARY 1, 2019

from bs4 import BeautifulSoup
import csv
import lxml
import pandas as pd
import pytz
import requests
import xml.etree.ElementTree as ET
import zipfile

from dateutil.parser import parse as dateutil_parse
from io import BytesIO, StringIO

# unzip method is from PYISO github
def unzip(content): 
    """
    Unzip encoded data.
    Returns the unzipped content as an array of strings, each representing one file's content
    or returns None if an error was encountered.
    ***Previous behavior: Only returned the content from the first file***
    """
    # create zip file
    try:
        filecontent = BytesIO(content)
    except TypeError:
        filecontent = StringIO(content)

    try:
        # have zipfile
        z = zipfile.ZipFile(filecontent)
    except zipfile.BadZipfile:
        LOGGER.error('%s: unzip failure for content beginning:\n%s' % (self.NAME, str(content)[0:100]))
        LOGGER.debug('%s: Faulty unzip content:\n%s' % (self.NAME, content))
        return None

    # have unzipped content
    unzipped = [z.read(thisfile) for thisfile in z.namelist()]
    z.close()

    # return
    return unzipped

def utcify(local_ts_str, tz_name=None, is_dst=None):
    """
    Convert a datetime or datetime string to UTC.

    Uses the default behavior of dateutil.parser.parse to convert the string to a datetime object.

    :param string local_ts: The local datetime to be converted.
    :param string tz_name: If local_ts is naive, it is assumed to be in timezone tz. If tz is not provided, the client's default timezone is used.
    :param bool is_dst: If provided, explicitly set daylight savings time as True or False.
    :return: Datetime in UTC.
    :rtype: datetime
    """
    # set up tz
    TZ_NAME = 'America/Los_Angeles'
    # TZ_NAME = 'America/Los_Angeles'
    if tz_name is None:
        tz = pytz.timezone(TZ_NAME)
    else:
        tz = pytz.timezone(tz_name)

    # parse
    try:
        local_ts = dateutil_parse(local_ts_str)
    except (AttributeError, TypeError):  # already parsed
        local_ts = local_ts_str

    # localize
    if local_ts.tzinfo is None:  # unaware
        if is_dst is None:
            aware_local_ts = tz.localize(local_ts)
        else:
            aware_local_ts = tz.localize(local_ts, is_dst=is_dst)
    else:  # already aware
        aware_local_ts = local_ts

    # convert to utc
    aware_utc_ts = aware_local_ts.astimezone(pytz.utc)

    # return
    return aware_utc_ts

def parse_xml(raw_data, market_run_id, freq):
    # extract values from xml

    data_items = ['LMP_PRC']
    data_label = 'LMP'
    #market_run_id = market_run_id

    # set up storage
    extracted_data = {}
    parsed_data = []
    prices = []
    # find prices
    raw_data = raw_data[0]
    for raw_soup_dp in raw_data:
        data_item = raw_soup_dp.find(['DATA_ITEM', 'data_item']).string
        if data_item in data_items:

            # NOT CONVERTED TO UTC
            ts = raw_soup_dp.find(['INTERVAL_START_GMT', 'interval_start_gmt']).string
            # CONVERTED TO UTC
            ts = utcify(raw_soup_dp.find(['INTERVAL_START_GMT', 'interval_start_gmt']).string)
            val = float(raw_soup_dp.find(['VALUE', 'value']).string)
            HE  = float(raw_soup_dp.find(['INTERVAL_NUM']).string)
            try:
                extracted_data[ts] += [val, HE]
            except KeyError:
                extracted_data[ts] = [val, HE]
    
    # assemble data
    for ts in sorted(extracted_data.keys()):
        parsed_dp = {data_label: extracted_data[ts][0]}
        parsed_dp.update({'Interval Index' : extracted_data[ts][1]})
        parsed_dp.update({'UTC timestamp': ts, 'freq': freq, 'market': market_run_id, 'ba_name': 'CAISO'})
        #if self.options['data'] == 'gen':
        #    parsed_dp.update({'fuel_name': 'other'})

        # add to storage
        parsed_data.append(parsed_dp)

    return parsed_data


# URL to pull one hour's 15-minute data from OASIS
def request_file():
    # CAISO Payload
    #dateformat = yyyymmddT00:00-0000
    url_base = 'http://oasis.caiso.com/oasisapi/SingleZip?queryname='

    #############################
    ## USER-DEFINED PARAMETERS ##
    #############################
    # LMP node of interest
    node = 'MUSTANGS_2_B1'
    # choose market of interest (queryname) from the following set
    # {'PRC_RTPD_LMP' (15-minute LMP for all PNodes and APNodes in $/MWh)
    #  'PRC_LMP'      (hourly LMP for all PNodes and APNodes - for DAM and RUC)
    #  'PRC_INTVL_LMP' (five-minute LMP for all PNodes and APNodes)
    #  'PRC_AS'        (Ancillary Services Regional Shadow Price for all AS types - hourly)}
    queryname = 'PRC_LMP' 
    # start and end date: format = 'yyyymmddT00:00-0000'
    # INTERVAL UNIT = GMT 
    startdatetime = '20180101T08:00-0000'
    enddatetime   = '20180201T08:00-0000'
    #############################
    #############################

    # correct market_run_id depends on queryname
    if queryname == 'PRC_LMP':
        market_run_id = 'DAM'
        freq          = 'hourly'
    elif queryname == 'PRC_RTPD_LMP':
        market_run_id = 'RTPD'
        freq          = '15-minute'
    elif queryname == 'PRC_INTVL_LMP':
        market_run_id = 'RTM'
        freq          = '5-minute'
    elif queryname == 'PRC_AS':
        market_run_id = 'DAM'
        freq          = 'hourly'
    else:
        print('Error, wrong queryname')

    # Set up final data frame for data storage 
    all_months_df = pd.DataFrame(columns =['placeholder'])

    # iterate through each month of the year
    for month in range(1,3):

        startdatetime = '2018' + "{:02d}".format(month) + '01T08:00-0000'
        enddatetime   = '2018' + "{:02d}".format(month + 1) + '01T08:00-0000'
        # Assemble URL
        if queryname == 'PRC_AS':
            url = url_base + queryname + '&market_run_id=' + market_run_id + '&startdatetime' + startdatetime + '&enddatetime' + enddatetime + '&version=1&anc_type=ALL&anc_region=ALL'
        else:
            url = url_base + queryname + '&startdatetime=' + startdatetime + '&enddatetime=' + enddatetime + '&version=1&market_run_id=' + market_run_id + '&node=' + node

        #print(url)
        #return
        # PRC_LMP single node:       http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_LMP&startdatetime=20130919T07:00-0000&enddatetime=20130920T07:00-0000&version=1&market_run_id=DAM&node=LAPLMG1_7_B2
        # PRC_INTVL_LMP single node: http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_INTVL_LMP&startdatetime=20130919T07:00-0000&enddatetime=20130919T08:00-0000&version=1&market_run_id=RTM&node=LAPLMG1_7_B2
        # PRC_AS single node:        http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_AS&market_run_id=DAM&startdatetime=20130919T07:00-0000&enddatetime=20130920T07:00-0000&version=1&anc_type=ALL&anc_region=ALL
        #url = 'http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_RTPD_LMP&startdatetime=20180101T00:00-0000&enddatetime=20180103T00:00-0000&version=1&market_run_id=RTPD&node=MUSTANGS_2_B1'

        # HTTP Response
        resp = requests.get(url)

        # Unzip response file, return XML
        content = unzip(resp.content)

        # Clean up XML using BeautifulSoup package
        raw_data = [BeautifulSoup(thisfile, 'xml').find_all(['REPORT_DATA', 'report_data']) for thisfile in content]

        # returns a list of dictionaries, one for each timestep, with LMP as one of the key value pairs (i.e. {'LMP' : 23.4})
        parsed_data = parse_xml(raw_data, market_run_id, freq)
        #print('Parsed Data:\n')

        # Convert list of dictionaries to pandas dataframe
        df = pd.DataFrame(parsed_data)

        # concatenate data frames
        if all_months_df.empty:
            all_months_df = df
        else:
            all_months_df = [all_months_df, df]
            all_months_df = pd.concat(all_months_df)


    # Write to CSV
    month = 'twomonthtest'
    all_months_df.to_csv('OASIS_Scrape_Output/' + node + month + market_run_id + '_' + queryname + '.csv', columns = list(df))  


request_file()