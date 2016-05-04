import httplib2
import pprint
import sys
import numpy as np
import pandas as pd
from pandas.io import gbq

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from oauth2client.client import OAuth2WebServerFlow
from oauth2client.client import AccessTokenRefreshError
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run_flow
from oauth2client import tools

from pandas.core.api import DataFrame
from pandas.compat import lzip, bytes_to_str
from datetime import datetime
from pandas.tools.merge import concat

from pydblite import Base


#  Put set_trace() anywhere in your code that you want to stop and take a look around
def set_trace():
    from sys import _getframe
    from IPython.core.debugger import Pdb
    Pdb(color_scheme='Linux').set_trace(_getframe().f_back)


def extract(db, query):
    try:
        return db(query=query)[0]['result_df']
    except (KeyError, IndexError) as e:
        return pd.DataFrame()


def get_biqquery_service(CLIENT_SECRET='client_secret_big_query.json'):

    FLOW = flow_from_clientsecrets(CLIENT_SECRET, scope='https://www.googleapis.com/auth/bigquery')
    flags = tools.argparser.parse_args(args=['--noauth_local_webserver'])

    storage = Storage('bigquery_credentials.dat')
    credentials = storage.get()

    if credentials is None or credentials.invalid:
        credentials = run_flow(FLOW, storage, flags)

    http = httplib2.Http()
    http = credentials.authorize(http)

    bigquery_service = build('bigquery', 'v2', http=http)

    try:
        # try retrieve some data
        datasets = bigquery_service.datasets()
        # listReply = datasets.list(projectId=PROJECT_NUMBER).execute()
        # if it runs OK, return the service
        return bigquery_service

    except HttpError as err:
        print 'Error in listDatasets:', pprint.pprint(err.content)

    except AccessTokenRefreshError:
        print ("Credentials have been revoked or expired, please re-run \
                the application to re-authorize")


def get_gbq_query_results(gbq_service, query, prj_id='ace-amplifier-455', index_col=None,
                          col_order=None, cache=True, cache_path='', db=None):
    
    # set_trace()
    if cache:
        if db is None:
            db = Base(cache_path).create('query', 'result_df', mode='open')
            # db.create_index('query')
        final_df = extract(db, query)
        if len(final_df) > 0:
            # print 'extracted from cache ' + db.path
            return final_df

    df = gbq.read_gbq(query, prj_id)

    if cache:
        db.insert(query, df)
        db.commit()
        print 'saved to cache ' + db.path

    return df
