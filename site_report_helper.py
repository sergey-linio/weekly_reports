import pandas as pd
import numpy as np
from numpy import NaN
from pandas.tseries.offsets import *

from auth_to_gbq2 import get_biqquery_service
from auth_to_gbq2 import get_gbq_query_results
from auth_to_gbq2 import extract

from sql2 import *
from pandas.tseries.offsets import *

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.pyplot import subplots_adjust

from pydblite import Base

from configobj import ConfigObj

pd.set_option('precision', 1)


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

COUNTRY_LIST = [
    'PE',
    'MX',
    'AR',
    'CL',
    'PA',
    'EC',
    'VE',
    'CO',
]

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


COLUMN_NAMES = [
    u'device_category',
    u'device_os',
    u'cr_pp',
    u'cr_add_to_cart',
    u'cr_success',
    u'cr_total',
    u'orders',
    u'avg_order_value',
    u'basket_size',
    u'visits',
    u'bounce_rate',
    u'revenue_per_session',
]

CACHE_PATH = ''
DB_CONNECTION = None


# --------------------------------------------------------------------
# Common functions


def check_correct_order(beginning_date, end_date):
    """
    check that end_date is older then beginning_date
    """
    if pd.Timestamp(beginning_date) > pd.Timestamp(end_date):
        beginning_date, end_date = end_date, beginning_date


def country_picker(Country):
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
    return prj_id, dataset_num, Country, fill_mobile, nodata


def create_site_report(**kwargs):
    """
    creates site_report


    Parameters
    ----------
    config : bool, default False

    config_path : str, default 'config.cfg'
        define path to config file if config = True

    country_list : list, default COUNTRY_LIST

    start : pd.Timestamp, str, default SIX_MONTH_START

    end : pd.Timestamp, str, default SIX_MONTH_END

    file_name: str, default 'site_report_international.xlsx'

    folder : str, default './site_report_excel_files/'

    cache_path : str, default './query_cache/report_queries.pdl'

    """

    config = kwargs['config'] if 'config' in kwargs.keys() else False

    global CACHE_PATH

    if config:

        def get_config_value(config_file, field, default):
            if field not in config_file['site_report']:
                return default
            else:
                return config_file['site_report'][field]

        config_file = ConfigObj(kwargs['config_path'] if 'config_path' in kwargs.keys() else 'config.ini')

        country_list = get_config_value(config_file, 'country_list', COUNTRY_LIST)
        if type(country_list) != list:
            country_list = [country_list]

        start = pd.Timestamp(get_config_value(config_file, 'start', SIX_MONTH_START))
        end = pd.Timestamp(get_config_value(config_file, 'end', SIX_MONTH_END))

        file_name = str(end.date()) + '_' + get_config_value(
            config_file,
            'file_name',
            'site_report_international.xlsx',
        )

        folder = get_config_value(config_file, 'folder', './site_report_excel_files/')

        CACHE_PATH = get_config_value(config_file, 'cache_path', './query_cache/report_queries.pdl')

    else:
        country_list = kwargs['country_list'] if 'country_list' in kwargs.keys() else COUNTRY_LIST

        start = pd.Timestamp(kwargs['start'] if 'start' in kwargs.keys() else SIX_MONTH_START)
        end = pd.Timestamp(kwargs['end'] if 'end' in kwargs.keys() else SIX_MONTH_END)

        file_name = str(end.date()) + '_' + kwargs['file_name'] if 'file_name' in kwargs.keys() \
            else str(end.date()) + '_' + 'site_report_international.xlsx'

        folder = kwargs['folder'] if 'folder' in kwargs.keys() \
            else './site_report_excel_files/'

        CACHE_PATH = kwargs['cache_path'] if 'cache_path' in kwargs.keys() \
            else './query_cache/report_queries.pdl'

    global DB_CONNECTION
    DB_CONNECTION = Base(CACHE_PATH).open()

    get_plots_for_countries(country_list, start, end)
    write_to_excel(get_country_dict(country_list), file_name, folder)


# --------------------------------------------------------------------
# Functions for excel


def write_to_excel(country_tables_dict, file_name, folder):

    full_file_name = folder + file_name

    # or customize it as you want
    # full_file_name = 'site_report_international.xlsx'

    # to excel
    writer = pd.ExcelWriter(full_file_name)
    [proper_order(value).to_excel(writer, key, na_rep='NaN') for key, value in country_tables_dict.items()]
    writer.save()


def proper_order(df):
    all_all = np.array([0, 5, 10, 15, 20])
    desktop_all = all_all + 1
    mobile_ios = all_all + 2
    mobile_android = all_all + 3
    tablet_all = all_all + 4
    df2 = pd.concat(
        [
            df.ix[:, all_all],
            df.ix[:, desktop_all],
            df.ix[:, mobile_ios],
            df.ix[:, mobile_android],
            df.ix[:, tablet_all]
        ],
        axis=1,
    )

    df2.ix[[2, 3, 4, 5, 10], :] = df2.ix[[2, 3, 4, 5, 10], :] / float(100)

    try:
        return df2
    except:
        return df


def get_country_dict(country_list):
    country_tables_dict = {}
    for c in country_list:
        try:
            country_tables_dict[c] = get_table(*country_picker(c))
            print c + ' succeed'
        except:
            country_tables_dict[c] = pd.DataFrame([c, 'to many rows'])
            print c + ' failed'
            raise
    return country_tables_dict


def get_table(prj_id='ace-amplifier-455', dataset_num='58090804', country='MX',
              fill_mobile=True, nodata=[pd.Timestamp('2015-12-05')], *args):

    # country
    currency = CURRENCY_DICT[country]

    # column names
    column_names_week = ['last week'] * 5
    column_names_4_week = ['last 4 weeks'] * 5
    column_names_12_week = ['last 12 weeks'] * 5
    column_names_24_week = ['last 24 weeks'] * 5
    column_names_52_week = ['last 52 weeks'] * 5

    pretty_names = [
        'device category',
        'device os',
        '% cr to product page from all visits',
        '% cr to add-to-cart from product page',
        '% cr to success',
        '% cr total',
        '# orders',
        '{currency}, avg order value'.format(currency=currency),
        'avg basket size',
        '# visits',
        '% bounce rate',
        '{currency}, revenue per session'.format(currency=currency)
    ]

    # get dataframes
    lw_df = get_dataframe(
        WEEK_START,
        WEEK_END,
        pretty_names,
        column_names_week,
        prj_id,
        dataset_num,
        nodata,
        fill_mobile,
    )

    l4w_df = get_dataframe(
        MONTH_START,
        MONTH_END,
        pretty_names,
        column_names_4_week,
        prj_id,
        dataset_num,
        nodata,
        fill_mobile
    )

    l12w_df = get_dataframe(
        THREE_MONTH_START,
        THREE_MONTH_END,
        pretty_names,
        column_names_12_week,
        prj_id,
        dataset_num,
        nodata,
        fill_mobile,
    )

    l24w_df = get_dataframe(
        SIX_MONTH_START,
        SIX_MONTH_END,
        pretty_names,
        column_names_24_week,
        prj_id,
        dataset_num,
        nodata,
        fill_mobile,
    )

    l52w_df = get_dataframe(
        YEAR_START,
        YEAR_END,
        pretty_names,
        column_names_52_week,
        prj_id,
        dataset_num,
        nodata,
        fill_mobile,
    )

    return pd.concat([lw_df, l4w_df, l12w_df, l24w_df, l52w_df], axis=1)


def get_dataframe(begining, end, pretty_names, column_names, prj_id, ds_num, nodata, fill_mobile=True):

    # set connection to GBQ
    BQ = get_biqquery_service()

    # send queries in date range and union the results in one df
    # site_df = get_gbq_query_results(BQ, site_report.format(dataset_num = ds_num,date_begin = begining,date_end =  end),prj_id)

    site_df_raw = pd.concat(
        [
            get_gbq_query_results(
                BQ,
                site_report.format(
                    date_begin=i[0].strftime('%Y-%m-%d'),
                    date_end=i[1].strftime('%Y-%m-%d'),
                    dataset_num=ds_num
                ),
                prj_id,
                db=DB_CONNECTION,
            )
            for i in generate_weekly_intervals(begining, end) if i[0] not in nodata and i[1] not in nodata
        ],
        ignore_index=True,
    )

    site_df = create_avg_df(site_df_raw)

    # drop NaN
    site_df = site_df.fillna(0)

    # set NaN for some platforms
    if fill_mobile:
        flag1 = site_df.device_category == 'tablet'
        flag2 = site_df.device_category == 'mobile'

        if prj_id == 'virtual-door-615':
            site_df.ix[(flag1 | flag2), ['cr_pp']] = NaN
        else:
            site_df.ix[(flag1 | flag2), ['cr_pp', 'cr_add_to_cart', 'cr_success']] = NaN

    # sort values
    site_df = site_df.sort_values(by=['device_category', 'device_os'])

    # set row names
    site_df.columns = pretty_names
    site_df = site_df.transpose()

    # set column names
    site_df.columns = column_names

    return site_df


def create_avg_df(df):
    """
    count mean for each column
    """
    local_df = df.fillna(0)
    week_index = pd.MultiIndex.from_arrays(
        [
            np.array(local_df.index),
            np.array(local_df.device_category),
            np.array(local_df.device_os),
        ],
        names=[u'basic', u'device_category', u'device_os']
    )
    multi_df = local_df
    multi_df.index = week_index
    week_avg_df = multi_df.mean(level=['device_category', 'device_os'])
    week_avg_df['device_category'] = [i[0] for i in week_avg_df.index.values]
    week_avg_df['device_os'] = [i[1] for i in week_avg_df.index.values]
    week_avg_df.index = pd.Index(range(len(week_avg_df)))

    final_df = week_avg_df.ix[:, COLUMN_NAMES]

    return final_df


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
            times[i].append((pd.Timestamp(times[i][0]) + Day(6)))
        else:
            times[i].append(pd.Timestamp(end_date))
    return times


# --------------------------------------------------------------------
# Functions for pdf


def get_plots_for_countries(country_list, beginning, end):
    for c in country_list:
        try:
            get_plots_outer(*get_data_for_plots(beginning, end, *country_picker(c)))
            print c + ' succeed'
        except:
            print c + ' failed'
            raise
    return country_list


def get_plots_outer(site_df, beginning, end, Country):
    # create writer
    pp = PdfPages('./site_report_plots/' + 'site_report_' + Country + '_' +
                  beginning.strftime('%Y-%m-%d') + '_' + end.strftime('%Y-%m-%d') + '.pdf')

    # save plots
    pp.savefig(
        get_plots(
            site_df.ix[site_df.device_category == 'all', :],
            'all',
            'all',
            Country,
        )
    )
    pp.savefig(
        get_plots(
            site_df.ix[site_df.device_category == 'desktop', :],
            'desktop',
            'all',
            Country,
        )
    )
    pp.savefig(
        get_plots(
            site_df.ix[((site_df.device_category == 'mobile') & (site_df.device_os == 'Android')), :],
            'mobile',
            'Android',
            Country,
        )
    )
    pp.savefig(
        get_plots(
            site_df.ix[((site_df.device_category == 'mobile') & (site_df.device_os == 'iOS')), :],
            'mobile',
            'iOS',
            Country,
        )
    )

    # save writer
    pp.savefig()

    # close writer
    pp.close()


def get_plots(plot_df, device, os, country):
    # country
    currency = CURRENCY_DICT[country]

    # values for plots
    orders_w = np.array(pd.rolling_mean(plot_df.orders, 7))
    orders_4w = np.array(pd.rolling_mean(plot_df.orders, 28))
    orders_12w = np.array(pd.rolling_mean(plot_df.orders, 84))
    orders_24w = np.array(pd.rolling_mean(plot_df.orders, 168))

    avg_order_value_w = np.array(pd.rolling_mean(plot_df.avg_order_value, 7))
    avg_order_value_4w = np.array(pd.rolling_mean(plot_df.avg_order_value, 28))
    avg_order_value_12w = np.array(pd.rolling_mean(plot_df.avg_order_value, 84))
    avg_order_value_24w = np.array(pd.rolling_mean(plot_df.avg_order_value, 168))

    visits_w = np.array(pd.rolling_mean(plot_df.visits, 7))
    visits_4w = np.array(pd.rolling_mean(plot_df.visits, 28))
    visits_12w = np.array(pd.rolling_mean(plot_df.visits, 84))
    visits_24w = np.array(pd.rolling_mean(plot_df.visits, 168))

    bounce_rate_w = np.array(pd.rolling_mean(plot_df.bounce_rate, 7))
    bounce_rate_4w = np.array(pd.rolling_mean(plot_df.bounce_rate, 28))
    bounce_rate_12w = np.array(pd.rolling_mean(plot_df.bounce_rate, 84))
    bounce_rate_24w = np.array(pd.rolling_mean(plot_df.bounce_rate, 168))

    date = np.array(plot_df.date)

    # setting size of the plots
    fig_size = plt.rcParams["figure.figsize"]
    fig_size[0] = 12
    fig_size[1] = 12
    plt.rcParams["figure.figsize"] = fig_size

    # new style method 1; unpack the axes
    fig, ((ax1, ax2,), (ax3, ax4)) = plt.subplots(2, 2)

    ax1.plot(date, orders_w, lw=3, label='1 week average', color='green', ls=':')
    ax1.plot(date, orders_4w, lw=3, label='4 week average', color='blue', ls='--')
    ax1.plot(date, orders_12w, lw=4, label='12 week average', color='black', ls='-')
    ax1.plot(date, orders_24w, lw=4, label='24 week average', color='black', ls='-.')
    ax1.legend(loc='upper left')
    ax1.set_title('Number of orders')
    ax1.set_ylabel('#')

    ax2.plot(date, avg_order_value_w, lw=3, label='1 week average', color='green', ls=':')
    ax2.plot(date, avg_order_value_4w, lw=3, label='4 week average', color='blue', ls='--')
    ax2.plot(date, avg_order_value_12w, lw=4, label='12 week average', color='black', ls='-')
    ax2.plot(date, avg_order_value_24w, lw=4, label='24 week average', color='black', ls='-.')
    ax2.set_title('Average order value')
    ax2.set_ylabel('{currency}'.format(currency=currency))

    ax3.plot(date, visits_w, lw=3, label='1 week average', color='green', ls=':')
    ax3.plot(date, visits_4w, lw=3, label='4 week average', color='blue', ls='--')
    ax3.plot(date, visits_12w, lw=4, label='12 week average', color='black', ls='-')
    ax3.plot(date, visits_24w, lw=4, label='24 week average', color='black', ls='-.')
    ax3.set_title('Number of sessions')
    ax3.set_ylabel('#')

    ax4.plot(date, bounce_rate_w, lw=3, label='1 week average', color='green', ls=':')
    ax4.plot(date, bounce_rate_4w, lw=3, label='4 week average', color='blue', ls='--')
    ax4.plot(date, bounce_rate_12w, lw=4, label='12 week average', color='black', ls='-')
    ax4.plot(date, bounce_rate_24w, lw=4, label='24 week average', color='black', ls='-.')
    ax4.set_title('% Bounce rate')
    ax4.set_ylabel('%')

    for ax in ax1, ax2, ax3, ax4:
        ax.grid(True)
        for tick in ax.get_xticklabels():
            tick.set_rotation(45)

    subplots_adjust(hspace=0.3, wspace=0.3)
    fig.suptitle(
        '{country} site report charts for {device} device, and {os} os'.format(
            country=country,
            device=device,
            os=os
        ),
        fontsize=14,
        fontweight='bold',
    )

    return fig


def get_data_for_plots(beginning, end, prj_id, dataset_num, Country, fill_mobile, nodata):
    # set connection to GBQ
    BQ = get_biqquery_service()

    # temp nodata
    nodata = [
        pd.Timestamp('2015-12-05'),
        pd.Timestamp('2015-12-06'),
        pd.Timestamp('2015-10-03'),
        pd.Timestamp('2015-10-04'),
        pd.Timestamp('2015-10-05'),
        pd.Timestamp('2015-10-06'),
        pd.Timestamp('2016-02-08'),
    ]

    # send queries in date range and union the results in one df
    site_df = pd.concat(
        [
            add_date_to_df(
                get_gbq_query_results(
                    BQ,
                    site_report.format(
                        dataset_num=dataset_num,
                        date_begin=i.strftime('%Y-%m-%d'),
                        date_end=i.strftime('%Y-%m-%d')
                    ),
                    prj_id,
                    db=DB_CONNECTION
                ),
                i,
            )
            for i in pd.date_range(beginning, end) if i not in nodata
        ],
        ignore_index=True,
    )

    # drop NaN
    site_df = site_df.fillna(0)

    return site_df, beginning, end, Country


# helper function
def add_date_to_df(df, date):
    df['date'] = date
    return df
