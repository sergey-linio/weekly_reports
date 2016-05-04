import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pandas.tseries.offsets import *
from matplotlib.pyplot import subplots_adjust
from matplotlib.backends.backend_pdf import PdfPages

from auth_to_gbq2 import get_biqquery_service
from auth_to_gbq2 import get_gbq_query_results
from auth_to_gbq2 import extract

from sql2 import pdt_reco

from pydblite import Base

from configobj import ConfigObj

pd.set_option('precision', 2)

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

CACHE_PATH = ''

DB_CONNECTION = None


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


# helper function
def add_date_to_df(df, date):
    df['date'] = date
    return df


def get_plots(daily_pdt_reco_df, daily_pdt_reco_df3, country, traffic):

    currency = CURRENCY_DICT[country]

    # subset the data
    df_traffic_type = daily_pdt_reco_df.ix[daily_pdt_reco_df.medium == traffic, :].sort_values('date')
    df_traffic_type3 = daily_pdt_reco_df3.ix[daily_pdt_reco_df3.medium == traffic, :].sort_values('date')

    # exact values
    num_of_visitors = np.array(df_traffic_type.num_of_visitors)
    share_of_visitors = np.array(df_traffic_type.share_of_visitors)
    num_of_sessions = np.array(df_traffic_type.num_of_sessions)
    share_of_sessions = np.array(df_traffic_type.share_of_sessions)
    session_duration_min = np.array(df_traffic_type.session_duration_min)
    loose_revenue = np.array(df_traffic_type.loose_revenue)
    share_of_loose_revenue = np.array(df_traffic_type.share_of_loose_revenue)
    loose_conversion_rate = np.array(df_traffic_type.loose_conversion_rate)
    loose_bakset_size = np.array(df_traffic_type.loose_bakset_size)
    loose_revenue_per_session = np.array(df_traffic_type.loose_revenue_per_session)

    # mean values
    num_of_visitors_mean = np.array(df_traffic_type3.num_of_visitors)
    share_of_visitors_mean = np.array(df_traffic_type3.share_of_visitors)
    num_of_sessions_mean = np.array(df_traffic_type3.num_of_sessions)
    share_of_sessions_mean = np.array(df_traffic_type3.share_of_sessions)
    session_duration_min_mean = np.array(df_traffic_type3.session_duration_min)
    loose_revenue_mean = np.array(df_traffic_type3.loose_revenue)
    share_of_loose_revenue_mean = np.array(df_traffic_type3.share_of_loose_revenue)
    loose_conversion_rate_mean = np.array(df_traffic_type3.loose_conversion_rate)
    loose_bakset_size_mean = np.array(df_traffic_type3.loose_bakset_size)
    loose_revenue_per_session_mean = np.array(df_traffic_type3.loose_revenue_per_session)

    # both dataframes are order by date
    date = np.array(df_traffic_type.date)

    # setting size of the plots
    fig_size = plt.rcParams["figure.figsize"]
    fig_size[0] = 15
    fig_size[1] = 15
    plt.rcParams["figure.figsize"] = fig_size

    # new style method 1; unpack the axes
    fig, ((ax1, ax2), (ax3, ax4), (ax5, ax6), (ax7, ax8), (ax9, ax10)) = plt.subplots(5, 2)

    ax1.plot(date, num_of_visitors, 'bo-')
    ax1.plot(date, num_of_visitors_mean, lw=1, label='weekly average', color='black', ls='--')
    ax1.legend(loc='upper left')
    ax1.set_title('N of visitors in traffic segment who at least clicked to one reco')

    ax2.plot(date, share_of_visitors, 'co-')
    ax2.plot(date, share_of_visitors_mean, lw=1, label='weekly average', color='black', ls='--')
    ax2.set_title('% of visitors in traffic segment who at least clicked to one reco')
    ax2.set_ylabel('% of visitors')

    ax3.plot(date, num_of_sessions, 'bo-')
    ax3.plot(date, num_of_sessions_mean, lw=1, label='weekly average', color='black', ls='--')
    ax3.set_title('N of sessions in traffic segment with at least one click to reco')

    ax4.plot(date, share_of_sessions,  'co-')
    ax4.plot(date, share_of_sessions_mean, lw=1, label='weekly average', color='black', ls='--')
    ax4.set_title('% of sessions in traffic segment with at least one click to reco')
    ax4.set_ylabel('% of sessions')

    ax5.plot(date, loose_revenue, 'go-')
    ax5.plot(date, loose_revenue_mean, lw=1, label='weekly average', color='black', ls='--')
    ax5.set_title('Loose revenue in traffic segment from pdt reco')
    ax5.set_ylabel('{}'.format(currency))

    ax6.plot(date, share_of_loose_revenue, 'co-')
    ax6.plot(date, share_of_loose_revenue_mean, lw=1, label='weekly average', color='black', ls='--')
    ax6.set_title('Share of loose revenue in traffic segment from pdt reco')
    ax6.set_ylabel('% revenue')

    ax7.plot(date, session_duration_min, 'yo-')
    ax7.plot(date, session_duration_min_mean, lw=1, label='weekly average', color='black', ls='--')
    ax7.set_title('Average session duration (loose) in traffic segment from pdt reco')
    ax7.set_ylabel('MIN')

    ax8.plot(date, loose_conversion_rate, 'ro-')
    ax8.plot(date, loose_conversion_rate_mean, lw=1, label='weekly average', color='black', ls='--')
    ax8.set_title('Loose conversion rate in traffic segment from pdt reco')
    ax8.set_ylabel('% CR')

    ax9.plot(date, loose_bakset_size, 'ko-')
    ax9.plot(date, loose_bakset_size_mean, lw=1, label='weekly average', color='black', ls='--')
    ax9.set_title('Average basket size (loose) in traffic segment from pdt reco')
    ax9.set_ylabel('Units')

    ax10.plot(date, loose_revenue_per_session, 'mo-')
    ax10.plot(date, loose_revenue_per_session_mean, lw=1, label='weekly average', color='black', ls='--')
    ax10.set_title('Loose revenue per session in traffic segment from pdt reco')
    ax10.set_ylabel('{}/Session'.format(currency))

    for ax in ax1, ax2, ax3, ax4, ax5, ax6, ax7, ax8, ax9, ax10:
        ax.grid(True)
        for tick in ax.get_xticklabels():
            tick.set_rotation(45)

    subplots_adjust(hspace=0.7)
    fig.suptitle(
        '{c} daily product recomendation statistics for {t} traffic'.format(c=country, t=traffic),
        fontsize=14,
        fontweight='bold',
    )

    return fig


def get_table(week_avg_df, country, traffic):
    currency = CURRENCY_DICT[country]
    df = week_avg_df.ix[week_avg_df.medium == traffic, :]
    df.columns = [
        u'num_of_visitors',
        u'share_of_visitors',
        u'num_of_sessions',
        u'share_of_sessions',
        u'session_duration_min',
        u'loose_revenue_{}'.format(currency),
        u'share_of_loose_revenue',
        u'loose_conversion_rate',
        u'loose_bakset_size',
        u'loose_revenue_per_session_{}'.format(currency),
        u'week',
        u'medium'
    ]

    return df


def data_for_plots(prj_id, dataset_num, Country, beginning, end, nodata):
    # set connection to GBQ
    BQ = get_biqquery_service()
    
    # send queries in date range and union the results in one df
    daily_pdt_reco_df = pd.concat(
        [
            add_date_to_df(get_gbq_query_results(
                BQ,
                pdt_reco.format(
                    date_begin=i.strftime('%Y-%m-%d'),
                    date_end=i.strftime('%Y-%m-%d'),
                    dataset_num=dataset_num
                ),
                prj_id,
                db=DB_CONNECTION
            ), i) for i in pd.date_range(beginning, end) if i not in nodata
        ],
        ignore_index=True,
    )

    # drop NaN
    daily_pdt_reco_df = daily_pdt_reco_df.fillna(0)

    # Calculate weekly average values

    # deep copy for safe
    daily_pdt_reco_df2 = daily_pdt_reco_df.copy(deep=True)

    # obtain week number
    daily_pdt_reco_df2['week'] = daily_pdt_reco_df2.date.apply(lambda x: x.week)

    # building multi-index
    week_index = pd.MultiIndex.from_arrays(
        [
            np.array(daily_pdt_reco_df2.index),
            np.array(daily_pdt_reco_df2.week),
            np.array(daily_pdt_reco_df2.medium)
        ],
        names=[
            u'basic',
            u'week',
            u'medium'
        ],
    )

    daily_pdt_reco_df2.index = week_index

    # mean by multi-index levels
    week_avg_df = daily_pdt_reco_df2.mean(level=['week', 'medium'])

    # add column to make join
    week_avg_df['medium'] = [i[1] for i in week_avg_df.index.values]

    # dataframe with date
    week_day_df = daily_pdt_reco_df2[['week', 'date', 'medium']]

    daily_pdt_reco_df3 = week_avg_df.merge(week_day_df, on=['week', 'medium'])

    return daily_pdt_reco_df, daily_pdt_reco_df3, week_avg_df, Country, beginning, end


def get_plots_outer(daily_pdt_reco_df, daily_pdt_reco_df3, week_avg_df, Country, beginning, end):
    # create writer
    pp = PdfPages('./product_recommendation_report_plots/' + Country + '_' + beginning.strftime('%Y-%m-%d') + '_' + end.strftime('%Y-%m-%d') + '.pdf')

    # save plots
    pp.savefig(get_plots(daily_pdt_reco_df, daily_pdt_reco_df3, Country, 'direct'))
    pp.savefig(get_plots(daily_pdt_reco_df, daily_pdt_reco_df3, Country, 'organic'))
    pp.savefig(get_plots(daily_pdt_reco_df, daily_pdt_reco_df3, Country, 'except affiliates'))

    # save writer
    pp.savefig()

    # close writer
    pp.close()


def create_plots(**kwargs):
    """
    creates plots


    Parameters
    ----------
    config : bool, default False

    config_path : str, default 'config.cfg'
        define path to config file if config = True

    country_list : list, default COUNTRY_LIST

    start : pd.Timestamp, str, default SIX_MONTH_START

    end : pd.Timestamp, str, default SIX_MONTH_END

    folder : str, default './product_recommendation_report_plots/'

    cache_path : str, default './query_cache/report_queries2.pdl'

    """

    config = kwargs['config'] if 'config' in kwargs.keys() else False
        
    global CACHE_PATH

    if config:

        def get_config_value(config_file, field, default):
            if field not in config_file['product_recommendation_report']:
                return default
            else:
                return config_file['product_recommendation_report'][field]

        config_file = ConfigObj(kwargs['config_path'] if 'config_path' in kwargs.keys() else 'config.ini')

        country_list = get_config_value(config_file, 'country_list', COUNTRY_LIST)
        if type(country_list) != list:
            country_list = [country_list]

        start = pd.Timestamp(get_config_value(config_file, 'start', SIX_MONTH_START))
        end = pd.Timestamp(get_config_value(config_file, 'end', SIX_MONTH_END))

        folder = get_config_value(config_file, 'folder', './product_recommendation_report_plots/')

        CACHE_PATH = get_config_value(config_file, 'cache_path', './query_cache/report_queries2.pdl')

    else:
        country_list = kwargs['country_list'] if 'country_list' in kwargs.keys() else COUNTRY_LIST

        start = pd.Timestamp(kwargs['start'] if 'start' in kwargs.keys() else SIX_MONTH_START)
        end = pd.Timestamp(kwargs['end'] if 'end' in kwargs.keys() else SIX_MONTH_END)

        folder = kwargs['folder'] if 'folder' in kwargs.keys() \
            else './product_recommendation_report_plots/'

        CACHE_PATH = kwargs['cache_path'] if 'cache_path' in kwargs.keys() \
            else './query_cache/report_queries2.pdl'

    global DB_CONNECTION
    DB_CONNECTION = Base(CACHE_PATH).open()

    get_plots_for_countries(country_list, start, end)


def get_plots_for_countries(country_list, beginning, end):
    for c in country_list:
        try:
            get_plots_outer(*data_for_plots(*country_picker(c, beginning, end)))
            print c + ' succeed'
        except:
            print c + ' failed'
            raise
            pass
    return country_list
