import pandas as pd
from datetime import datetime
import getData
from pandas import DataFrame
import logging

logger = logging.getLogger(f'Recomservicelog.{__name__}')
n = 10


class PaceByAlgo:
    def __init__(self, pid, db_confg):
        self.pid = pid
        self.db_confg = db_confg

    def select_algo(self, start_date, end_date):
        logger.info('--Getting algo----')
        # query = ('select rh.date, rh.pace_point, rh.algo from rate_horizon as rh inner join '
        #          '(select client_id, date, max(id) as id from rate_horizon group by client_id, date) as rh2 on '
        #          '(rh.client_id = rh2.client_id and rh.id = rh2.id) where rh.client_id = {} and rh.date '
        #          'between "{}" and "{}" order by rh.date'.format(self.pid, start_date, end_date))
        #
        query = f"SELECT rh.date, rh.pace_point, rh.algo from revseed.rate_horizon as rh " \
                f"where rh.client_id = {self.pid} and rh.date and rh.status = 'active' between" \
                f" {start_date} and {end_date} order by rh.date, rh.pace_point;"
        try:
            pacepontalog_df = getData.getData(myquery=query, pid=self.pid, db_confg=self.db_confg)
        except Exception as E:
            pacepontalog_df = pd.DataFrame(columns=['date', 'pace_point', 'algo'])
            logger.info('error to get pace point data: {}'.format(E))

        # # pacepontalog_df = pd.read_excel(r'C:\Users\RDM2\Desktop\pacebydate.xlsx')
        # ppalgo_df = pd.DataFrame(pacepontalog_df)
        # pace_collist = ppalgo_df.columns.to_list()
        # ppalgo_df =pd.melt(ppalgo_df, id_vars=pace_collist[0], var_name='pace',
        #               value_name='pacebyalgo')
        # start_date = datetime.strptime(start_date, '%Y-%m-%d')
        #
        # pace_df = pd.DataFrame(pacepontalog_df, columns=['Date'])
        # pace_df['pace'] = (pace_df['Date'] - start_date).dt.days
        #
        # algobypp = pace_df.merge(ppalgo_df, how='left', on=['Date', 'pace'])
        #
        # pacealgo = pd.DataFrame(algobypp, columns=['Date', 'pacebyalgo'])

        return pacepontalog_df

    def getparameters(self):
        logger.info("--Getting parameter values---")

        myquery = "select * from property_parameters where client_id = {}".format(self.pid)
        try:
            parameter_df = getData.getData(myquery=myquery, pid=self.pid, db_confg=self.db_confg)
        except Exception as E:
            parameter_df = pd.DataFrame()
            logger.info("Error @ fetch parameter data :{}".format(E))
        # parameter_df = pd.read_excel(r'C:\Users\RDM2\Desktop\parameters.xlsx', index_col=False)
        parameter_df = pd.DataFrame(parameter_df, columns=['param_name', 'param_value'])
        parameter_dict = dict(zip(parameter_df['param_name'], parameter_df['param_value']))
        logger.debug(parameter_dict)
        return parameter_dict






if __name__ == '__main__':
    db = {'rhost': 'rhost', 'ruser': 'ruser', 'rpwd' :'rpwd', 'rdb' : 'rdb', 'dbtype':'dbtype'}

    pace = PaceByAlgo(pid=1, db_confg=db)
    start_date = '2020-04-20'
    end_date = '2020-06-02'
    pacealgo_df: DataFrame= pace.select_algo(start_date, end_date)
    print(pacealgo_df)
    parameter = pace.getparameters()
    print(parameter)
