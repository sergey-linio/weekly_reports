import numpy as np
import pandas as pd
from pandas.tseries.offsets import *
from sklearn import linear_model

from openpyxl import load_workbook

from auth_to_gbq2 import get_biqquery_service
from auth_to_gbq2 import get_gbq_query_results
from auth_to_gbq2 import extract

from sql_helper_for_weeks_report import get_weekly_information
from sql2 import term_report_2, search_report

from pydblite import Base

from configobj import ConfigObj

pd.set_option('precision', 2)

WEEK_COLUMNS = ['last week', 'last 4 weeks', 'last 12 weeks', 'last 24 weeks', 'last 32 weeks']

SESSIONS = [
    '# search sessions last week',
    '# search of sessions last 4 weeks',
    '# search sessions last 12 weeks',
    '# search sessions last 24 weeks',
]

CACHE_PATH = ''
DB_CONNECTION = None

CONVERSION = [
    '% last week conversion',
    '% last 4 weeks conversion',
    '% last 12 weeks conversion',
    '% last 24 weeks conversion',
]

CURRENCY = [9, 10, 11, 12]

CURRENCY_DICT = {
    'PE': 'PEN',
    'MX': 'MXP',
    'AR': 'ARS',
    'CO': 'COP',
    'EC': 'USD',
    'PA': 'USD',
    'CL': 'CLP',
    'VE': 'VEF',
}

COUNTRY_LIST = ['PE', 'MX', 'AR', 'CL', 'PA', 'EC', 'VE', 'CO']


def get_time_period(weeks):
    """
    generate automatic time period for report in weeks
    """
    start = pd.to_datetime('today') - Week(weeks + 1, weekday=6)

    if pd.to_datetime('today') - Week(weekday=5) >= pd.to_datetime('today') - Day(2):
        end = pd.to_datetime('today') - Day(2)
    else:
        end = pd.to_datetime('today') - Week(weekday=5)

    return start, end

WEEK_START, WEEK_END = get_time_period(1)
MONTH_START, MONTH_END = get_time_period(4)
THREE_MONTH_START, THREE_MONTH_END = get_time_period(12)
SIX_MONTH_START, SIX_MONTH_END = get_time_period(24)
YEAR_START, YEAR_END = get_time_period(52)


# --------------------------------------------------------------------
# Common functions


def create_report(**kwargs):
    """
    creates report


    Parameters
    ----------
    config : bool, default False

    config_path : str, default 'config.cfg'
        define path to config file if config = True

    country_list : list, default COUNTRY_LIST

    start_1 : pd.Timestamp, str, default YEAR_START

    end_1 : pd.Timestamp, str, default YEAR_END    

    start_2 : pd.Timestamp, str, default SIX_MONTH_START

    end_2 : pd.Timestamp, str, default SIX_MONTH_END

    master_name : str, default 'search_report_international_master_file.xlsx'

    file_name : str, default 'search_report_international.xlsx'

    folder : str, default './search_report_excel_files/'

    cache_path : str, default './query_cache/report_queries3.pdl'

    """

    config = kwargs['config'] if 'config' in kwargs.keys() else False
        
    global CACHE_PATH

    if config:

        def get_config_value(config_file, field, default):
            if field not in config_file['search_report']:
                return default
            else:
                return config_file['search_report'][field]

        config_file = ConfigObj(kwargs['config_path'] if 'config_path' in kwargs.keys() else 'config.ini')

        country_list = get_config_value(config_file, 'country_list', COUNTRY_LIST)
        
        start_1 = pd.Timestamp(get_config_value(config_file, 'start_1', YEAR_START))
        end_1 = pd.Timestamp(get_config_value(config_file, 'end_1', YEAR_END))
        
        start_2 = pd.Timestamp(get_config_value(config_file, 'start_2', SIX_MONTH_START))
        end_2 = pd.Timestamp(get_config_value(config_file, 'end_2', SIX_MONTH_END))

        master_name = get_config_value(
            config_file,
            'master_name',
            'search_report_international_master_file.xlsx',
        )
        file_name = str(end_2.date()) + '_' + get_config_value(
            config_file,
            'file_name',
            'search_report_international.xlsx',
        )
        folder = get_config_value(
            config_file,
            'folder',
            './search_report_excel_files/',
        )

        CACHE_PATH = get_config_value(
            config_file,
            'cache_path',
            './query_cache/report_queries3.pdl'
        )

    else:
        country_list = kwargs['country_list'] if 'country_list' in kwargs.keys() else COUNTRY_LIST

        start_1 = pd.Timestamp(kwargs['start_1'] if 'start_1' in kwargs.keys() else YEAR_START)
        end_1 = pd.Timestamp(kwargs['end_1'] if 'end_1' in kwargs.keys() else YEAR_END)

        start_2 = pd.Timestamp(kwargs['start_2'] if 'start_2' in kwargs.keys() else SIX_MONTH_START)
        end_2 = pd.Timestamp(kwargs['end_2'] if 'end_2' in kwargs.keys() else SIX_MONTH_END)

        master_name = kwargs['master_name'] if 'master_name' in kwargs.keys() \
            else 'search_report_international_master_file.xlsx'

        file_name = str(end_2.date()) + '_' + kwargs['file_name'] if 'file_name' in kwargs.keys() \
            else str(end_2.date()) + '_' + 'search_report_international.xlsx'

        folder = kwargs['folder'] if 'folder' in kwargs.keys() \
            else './search_report_excel_files/'

        CACHE_PATH = kwargs['cache_path'] if 'cache_path' in kwargs.keys() \
            else './query_cache/report_queries3.pdl'

    global DB_CONNECTION
    DB_CONNECTION = Base(CACHE_PATH).open()

    first_table = get_country_dict(country_list, start_1, end_1)
    second_table = get_country_dict2(country_list, start_2, end_2)
    get_excel_file(first_table, second_table, master_name, file_name, folder)


def country_picker(Country, beginning, end):
    """
    method country_picker is placed here to quickly make changes in prj_id and dataset_num
    """
    if Country == 'PE':
        prj_id = 'ace-amplifier-455'
        # 'virtual-door-615'
        dataset_num = '80948718'
        # '59608596'
        fill_mobile = True
        nodata = [
            pd.Timestamp('2015-12-05'),
            pd.Timestamp('2015-12-06'),
            pd.Timestamp('2015-10-03'),
            pd.Timestamp('2015-10-04'),
            pd.Timestamp('2015-10-05'),
            pd.Timestamp('2015-10-06'),
            pd.Timestamp('2016-02-08'),
        ]
    elif Country == 'MX':
        prj_id = 'ace-amplifier-455'
        # dataset_num = '58090804'
        dataset_num = '79705518'
        fill_mobile = True
        nodata = [
            pd.Timestamp('2015-12-05'),
            pd.Timestamp('2015-12-06'),
            pd.Timestamp('2016-02-08'),
        ]
    elif Country == 'AR':
        prj_id = 'ace-amplifier-455'
        dataset_num = '93341905'
        fill_mobile = False
        nodata = [
            pd.Timestamp('2015-12-05'),
            pd.Timestamp('2015-12-06'),
        ]
    elif Country == 'CL':
        prj_id = 'ace-amplifier-455'
        dataset_num = '89511434'
        fill_mobile = False
        nodata = [
            pd.Timestamp('2015-12-05'),
            pd.Timestamp('2015-12-06'),
        ]
    elif Country == 'PA':
        prj_id = 'ace-amplifier-455'
        dataset_num = '89512432'
        fill_mobile = False
        nodata = [
            pd.Timestamp('2015-12-05'),
            pd.Timestamp('2015-12-06'),
        ]
    elif Country == 'EC':
        prj_id = 'ace-amplifier-455'
        dataset_num = '96998454'
        fill_mobile = False
        nodata = [
            pd.Timestamp('2015-12-05'),
            pd.Timestamp('2015-12-06'),
        ]
    elif Country == 'VE':
        prj_id = 'ace-amplifier-455'
        dataset_num = '79706701'
        fill_mobile = False
        nodata = [
            pd.Timestamp('2015-12-05'),
            pd.Timestamp('2015-12-06'),
            pd.Timestamp('2016-02-08'),
        ]
    elif Country == 'CO':
        prj_id = 'ace-amplifier-455'
        dataset_num = '80392523'
        fill_mobile = False
        nodata = [
            pd.Timestamp('2015-12-05'),
            pd.Timestamp('2015-12-06'),
            pd.Timestamp('2016-02-08'),
        ]
    else:
        prj_id = 'ace-amplifier-455'
        dataset_num = '58090804'
        fill_mobile = True
        nodata = [
            pd.Timestamp('2015-12-05'),
            pd.Timestamp('2015-12-06'),
        ]
    return prj_id, dataset_num, Country, beginning, end, nodata


def get_excel_file(first_table, second_table, master_name, file_name, folder):

    # filename and path setup
    full_file_name = folder + file_name
    full_file_name_master = folder + master_name
    # or customize it as you want
    # full_file_name = 'search_report_international.xlsx'

    book = load_workbook(full_file_name_master)
    # writer = pandas.ExcelWriter('Masterfile.xlsx', engine='openpyxl')
    writer = pd.ExcelWriter(full_file_name, engine='openpyxl')
    writer.book = book
    writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
    # df.to_excel(writer,'Sheet1',startcol=15)
    # writer.save()

    def write_element(table, startrow, startcol, element):
        [value.ix[:, element].to_excel(
            writer,
            key,
            na_rep='NaN',
            startrow=startrow,
            startcol=startcol,
            header=False,
            index=False) for key, value in table.items()]

    # to excel
    # writing the first table
    write_element(first_table, 1, 1, WEEK_COLUMNS)

    # writing the second table
    write_element(second_table, 16, 0, ['Term'])
    write_element(second_table, 16, 1, ['LM meaning'])
    write_element(second_table, 16, 3, SESSIONS)
    write_element(second_table, 16, 8, CONVERSION)
    write_element(second_table, 16, 13, CURRENCY)

    writer.save()


# ----------------------------------------------------------------------
# Functions for first table


def get_country_dict(country_list, beginning, end):
    country_tables_dict = {}
    for c in country_list:
        try:
            country_tables_dict[c] = get_table(*country_picker(c, beginning, end))
            print c + ' succeed'
        except Exception, e:
            country_tables_dict[c] = pd.DataFrame([c, 'to many rows'])
            print c + ' failed' + str(e)
    return country_tables_dict


def get_table(prj_id, dataset_num, country, beginning, end, nodata):

    # set connection to GBQ
    BQ = get_biqquery_service()

    # send queries in date range and union the results in one df
    search_df = pd.concat(
        [
            add_week_to_df(get_gbq_query_results(
                    BQ,
                    search_report.format(
                        date_begin=i[0].strftime('%Y-%m-%d'),
                        date_end=i[1].strftime('%Y-%m-%d'),
                        dataset_num=dataset_num
                    ),
                    prj_id,
                    db=DB_CONNECTION
                ),
                i[1],
            ) for i in generate_weekly_intervals(beginning, end) if i[0] not in nodata and i[1] not in nodata
        ],
        ignore_index=True
    )

    # drop NaN
    df = search_df.fillna(0)

    # country
    currency = CURRENCY_DICT[country]

    # transform the dataframe with helper functions
    testdf2 = multi_index_aggregate(prepare_big_picture(df))
    big_picture = testdf2.loc[[2, 3, 4, 5, 6], :].transpose()
    big_picture.columns = [
        'last week',
        'last 4 weeks',
        'last 12 weeks',
        'last 24 weeks',
        'last 32 weeks',
    ]

    # resultin dataframe
    big_picture_pretty = big_picture.loc[[
        'search_sessions',
        'search_sessions_share',
        'conversion_rate',
        'source_gross_revenue',
        'source_gross_revenue_share',
        'orders',
        'orders_share',
        'revenue_per_session'
        ], :]

    pretty_index = pd.Index([
        '# of search sessions',
        '% of search sessions',
        '% conversion rate',
        '{} gross revenue'.format(currency),
        '% of gross revenue',
        '# orders',
        '% of orders',
        '{} revenue per session'.format(currency)
    ])

    big_picture_pretty.index = pretty_index
    # big_picture_pretty.loc[['% of search sessions','% of gross revenue','% of orders']] = \
    # big_picture_pretty.loc[['% of search sessions','% of gross revenue','% of orders']] * 100
    # big_picture_pretty.loc['% conversion rate'] = big_picture_pretty.loc['% conversion rate'] / 100

    return big_picture_pretty


def prepare_big_picture(df):

    # splitting data
    df1_t = df.ix[df.GROUP == 'total sessions', :]
    df1_t.index = pd.Index(range(len(df1_t)))
    df1_s = df.ix[df.GROUP == 'search sessions', :]
    df1_s.index = pd.Index(range(len(df1_s)))

    # adding columns share:
    # FIXME: add space after ,
    df1_s.loc[:, 'search_sessions_share'] = df1_s['search_sessions'] / df1_t['search_sessions']
    df1_s.loc[:, 'source_gross_revenue_share'] = df1_s['source_gross_revenue'] / df1_t['source_gross_revenue']
    df1_s.loc[:, 'orders_share'] = df1_s['orders'] / df1_t['orders']
    df1_s.loc[:, 'revenue_per_session_share'] = df1_s['revenue_per_session'] / df1_t['revenue_per_session']
    # df1_s.loc[:,'week'] = df1_s.date.apply(lambda x: x.week)
    # df1_s.loc[:,'month'] = df1_s.date.apply(lambda x: x.month)

    return df1_s


def multi_index_aggregate(df):
    i = len(df)
    df1 = df
    df1.index = pd.MultiIndex.from_arrays([
        yesterday_index(np.zeros(i)),
        week_index(np.zeros(i)),
        month_index(np.zeros(i)),
        three_month_index(np.zeros(i)),
        six_month_index(np.zeros(i)),
        year_index(np.zeros(i))],
        names=[
            'yesterday',
            'last week',
            'last 4 weeks',
            'last 12 weeks',
            'last 24 weeks',
            'last 32 weeks',
        ]
    )

    return pd.concat([
        df1.mean(level=['last week']),
        df1.mean(level=['last 4 weeks']),
        df1.mean(level=['last 12 weeks']),
        df1.mean(level=['last 24 weeks']),
        df1.mean(level=['last 32 weeks']),
    ])


def yesterday_index(array1):
    array = array1
    array[-1:] = 1
    array[:-1] = 0
    return array


def week_index(array1):
    array = array1
    if len(array) > 1:
        array[-1:] = 2
        array[:-1] = 0
    else:
        array[:] = 2
    return array


def month_index(array1):
    array = array1
    if len(array) > 4:
        array[-4:] = 3
        array[:-4] = 0
    else:
        array[:] = 4
    return array


def three_month_index(array1):
    array = array1
    if len(array) > 12:
        array[-12:] = 4
        array[:-12] = 0
    else:
        array[:] = 4
    return array


def six_month_index(array1):
    array = array1
    if len(array) > 24:
        array[-24:] = 5
        array[:-24] = 0
    else:
        array[:] = 5
    return array


def year_index(array1):
    array = array1
    if len(array) > 32:
        array[-32:] = 6
        array[:-32] = 0
    else:
        array[:] = 6
    return array

# ----------------------------------------------------------------------
# Functions for second table


def get_country_dict2(country_list, beginning, end):
    country_tables_dict = {}
    for c in country_list:
        try:
            country_tables_dict[c] = clear_df_column_names(
                create_common_df_for_given_period(
                    beginning,
                    end,
                    *country_picker(c, 1, 2)[:2]
                ),
                CURRENCY_DICT[c]
            )
            print c + ' succeed'
        except Exception, e:
            country_tables_dict[c] = pd.DataFrame([c, 'to many rows'])
            raise
            print c + ' failed: ' + str(e)

    for value in country_tables_dict.values():

        value['LM search sessions'] = value.apply(lambda row: get_lm_coefs(row, SESSIONS), axis=1)
        value['LM conversion'] = value.apply(lambda row: get_lm_coefs(row, CONVERSION), axis=1)
        value['LM currency'] = value.apply(lambda row: get_lm_coefs(row, CURRENCY), axis=1)
        quantile_dict = dict(value.quantile([0.33, 0.8, 1.0]).loc[
            :,
            ['LM search sessions', 'LM conversion', 'LM currency']
        ])
        value['LM meaning'] = value.apply(lambda row: get_meaning(row, quantile_dict), axis=1)
        value['LM sort'] = value.apply(lambda row: get_sorting(row, quantile_dict), axis=1)
        value.sort_values(['LM sort'], axis=0, ascending=False, inplace=True)

    return country_tables_dict


def get_lm_coefs(row, group):
    y = np.array(row[group]).reshape(-1, 1)
    x = np.array([4, 3, 2, 1]).reshape(-1, 1)
    regr = linear_model.LinearRegression()
    try:
        regr.fit(x, y)
    except:
        return np.NaN
    return regr.coef_[0][0]


def get_number(value, quantiles):
    try:
        if value <= quantiles[0.33]:
            return -1.0
        elif value <= quantiles[0.8]:
            return 0.0
        elif value <= quantiles[1.0]:
            return 1.0
        else:
            return -1.0
    except:
        return -1.0


def get_meaning(row, quantiles):
    ss = get_number(row['LM search sessions'], quantiles['LM search sessions'])
    conv = get_number(row['LM search sessions'], quantiles['LM search sessions'])
    curr = get_number(row['LM currency'], quantiles['LM currency'])

    result = get_sorting(row, quantiles)

    if result == 3.0:
        return '****'
    elif result == 2.0:
        return '***'
    elif result == 1.0:
        return '**'
    elif result == 0.0:
        return '*'
    elif result == -1.0:
        return '-'
    elif result == -2.0:
        return '-'
    elif result == -3.0:
        return '-'
    else:
        return 0.0
    return 0.0


def get_sorting(row, quantiles):
    ss = get_number(row['LM search sessions'], quantiles['LM search sessions'])
    conv = get_number(row['LM search sessions'], quantiles['LM search sessions'])
    curr = get_number(row['LM currency'], quantiles['LM currency'])
    return ss + conv + curr


def clear_df_column_names(df, currency):
    """
    delete useless columns then order and 'pretty-name' columns
    """
    local_df = df.drop('week', axis=1)
    local_df = local_df.drop('last_3_month', axis=1)
    local_df = local_df.drop('last_3_month_conversion', axis=1)
    local_df = local_df.drop('last_3_month_revenue', axis=1)
    local_df.columns = list(range(1, 13))
    normal_order = list(range(1, 11, 3)) + list(range(2, 12, 3)) + list(range(3, 13, 3))
    orderd_df = local_df[normal_order]
    orderd_df.columns = [
        '# search sessions last week', '# search of sessions last 4 weeks',
        '# search sessions last 12 weeks', '# search sessions last 24 weeks',
        '% last week conversion', '% last 4 weeks conversion',
        '% last 12 weeks conversion', '% last 24 weeks conversion',
        '{} revenue last week'.format(currency), '{} revenue last 4 weeks'.format(currency),
        '{} revenue last 12 weeks'.format(currency), '{} revenue last 24 weeks'.format(currency)
    ]
    result_df = orderd_df.reset_index(level=['Term'])
    return result_df


def create_common_df_for_given_period(first_date, last_date, prj_id, dataset):
    """
    creates dataframe with calculated data for 1,4,12,24 weeks
    """
    weekly_intervals = generate_weekly_intervals(first_date, last_date)
    queue_to_be_executed = list()
    new_df = create_avg_df(generate_df_for_n_weeks(
        [weekly_intervals[-1]],
        dataset,
        prj_id,
        first_date,
        last_date
    ))
    if len(weekly_intervals) >= 4:
        queue_to_be_executed.append(weekly_intervals[-5:-1])
    if len(weekly_intervals) >= 12:
        queue_to_be_executed.append(weekly_intervals[-13:-1])
    if len(weekly_intervals) >= 24:
        queue_to_be_executed.append(weekly_intervals[-25:-1])
    for i in queue_to_be_executed:
        part_df = create_avg_df(generate_df_for_n_weeks(i, dataset, prj_id, first_date, last_date))
        new_df = pd.concat([new_df, part_df], axis=1, join_axes=[new_df.index])
    return new_df


def create_avg_df(df):
    """
    count mean for each column
    """
    local_df = df.fillna(0)
    week_index = pd.MultiIndex.from_arrays([
            np.array(local_df.index),
            np.array(local_df.Term),
            np.array(local_df.week)
        ],
        names=[u'basic', u'Term', u'week']
    )
    multi_df = local_df
    multi_df.index = week_index
    week_avg_df = multi_df.mean(level=['Term']).sort_values('last_3_month_revenue', ascending=False)
    return week_avg_df


def generate_df_for_n_weeks(weeks, dataset, prj_id, beginning, end):
    """
    this function generates dataframe with calculated data within given weeks
    """
    BQ = get_biqquery_service()
    daily_pdt_reco_df = pd.concat([
        add_week_to_df(get_gbq_query_results(
                BQ,
                get_weekly_information.format(
                    date_start=beginning,
                    date_end=end,
                    week_start=weeks[i][0],
                    week_end=weeks[i][1],
                    dataset_num=dataset,
                ),
                prj_id,
                db=DB_CONNECTION
            ),
            i + 1,
        ) for i in range(len(weeks))], ignore_index=True)

    return daily_pdt_reco_df


def add_week_to_df(df, week_number):
    """
    add new column to existing df with information about number of week
    """
    df['week'] = week_number
    return df


def generate_weekly_intervals(beginning_date, end_date):
    """
    generate weeks periods for a given dates
    """
    times = list()
    check_correct_order(beginning_date, end_date)
    for i in pd.date_range(beginning_date, end_date, freq='7d'):
        times.append([i])
    for i in range(0, len(times)):
        if i != len(times)-1:
            times[i].append((pd.Timestamp(times[i][0])+Day(6)))
        else:
            times[i].append(pd.Timestamp(end_date))
    return times


def check_correct_order(beginning_date, end_date):
    """
    check that end_date is older then beginning_date
    """
    if pd.Timestamp(beginning_date) > pd.Timestamp(end_date):
        beginning_date, end_date = end_date, beginning_date
