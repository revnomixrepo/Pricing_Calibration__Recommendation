import pandas as pd
import numpy as np
from scipy.stats import poisson
import datetime
import configparser
import os
import getData
import logging

# logger = logging.getLogger(__name__)
logger = logging.getLogger(f'Recomservicelog.{__name__}')
logger.info(f'Starting Rate Analysis environment for Recommendation')


def run_inf_factor(df_,cid,config):
    logger.info(f"----Running Single Rate Inflation Recommendation----")

    rate_inflation_q = "SELECT * FROM revseed.rate_inflation_factor where client_id =:prid"
    rate_inflation_df = getData.getData(rate_inflation_q,pid=cid,db_confg=config)
    logger.info(f"Getting inflation factor from database for {cid}")

    rate_pace_q = "SELECT * FROM revseed.rate_pace_calibration where client_id =:prid"
    rate_pace_df = getData.getData(rate_pace_q,pid=cid,db_confg=config)
    logger.info(f"Getting calibrated pace data from database for {cid}")

    rate_mu_q = "SELECT * FROM revseed.rate_mu_calibration where client_id =:prid"
    rate_mu_df = getData.getData(rate_mu_q,pid=cid,db_confg=config)
    logger.info(f"Getting calibrated mu_sigma data from database for {cid}")

    inf_factor = rate_inflation_df["Inflation_Factor"][0]
    column_week_names = {"dow1":"Monday",
                         "dow2":"Tuesday",
                         "dow3":"Wednesday",
                         "dow4":"Thursday",
                         "dow5":"Friday",
                         "dow6":"Saturday",
                         "dow7":"Sunday",
                         "week_days":"WD",
                         "week_ends":"WE",
                         "all_days":"Total"}
    rate_pace_df = rate_pace_df.rename(columns=column_week_names)
    rate_mu_df = rate_mu_df.rename(columns=column_week_names)

    df_ = df_.sort_values(by='checkin_date').reset_index()
    df_["Pace"] = (df_["checkin_date"] - df_["checkin_date"][0]).dt.days
    df_["Day Of Week"] = df_["checkin_date"].dt.day_name()

    df_['d1'] = 8
    df_['d2'] = 6
    df_['m1'] = 1
    df_['dow'] = df_['checkin_date'].dt.day_name()
    df_['dtyr'] = df_['checkin_date'].dt.year
    df_['dt1'] = pd.to_datetime(df_['dtyr'].astype(str) + '-' +
                                df_['m1'].astype(str) + '-' +
                                df_['d1'].astype(str))
    df_['dt2'] = pd.to_datetime(df_['dtyr'].astype(str) + '-' +
                                df_['m1'].astype(str) + '-' +
                                df_['d2'].astype(str))
    df_['dtwk'] = df_['dt2'].dt.dayofweek + 2
    df_['dt3'] = df_['dt1'] - pd.TimedeltaIndex(df_['dtwk'], unit='D')
    df_['dtdf'] = (df_['dt3'] - df_['checkin_date']).apply(lambda x: x / np.timedelta64(1, 'D'))
    df_.loc[(df_['dtdf'] > 0.0), 'WkNum'] = 52
    df_.loc[(df_['dtdf'] <= 0.0), 'WkNum'] = df_['checkin_date'].apply(lambda x: x.strftime('%W'))
    df_['WkNum'] = pd.to_numeric(df_['WkNum'])
    df_['Week_Num'] = np.where((df_['WkNum'] == 53), 0, df_['WkNum'])
    df_['Week_Num'] = np.where((df_['Week_Num'] == 0), 52, df_['WkNum'])

    df_ = df_.drop(["d1", "d2", "m1", "dow", "dtyr", "dt1", "dt2", "dtwk", "dt3", "dtdf", "WkNum"], axis=1)

    def season_number(week_num,season_df):
        season_df = season_df.loc[1:]
        try:
            count = season_df.loc[(week_num >= season_df["start_week"]) & (week_num <= season_df["end_week"])].index.values.astype(int)[0]
        except:
            count = season_df.loc[(week_num >= season_df["start_week"]) | (week_num <= season_df["end_week"])].index.values.astype(int)[0]

        return count

    season_query = "select hotel_id, start_week, end_week, max_capacity FROM seasonality_definitions WHERE client_id =:prid"
    logger.info("--- Fetching the Day of Season Definition ---")
    df_season = getData.getData(myquery=season_query, pid=cid, db_confg=config)

    df_["Season"] = df_['Week_Num'].apply(lambda x:season_number(x,df_season))

    def pace_mu(pace, dow, df):
        try:
            adr_ = df[dow][df["pace_points"] == pace]
            adr = adr_[pace]
            return adr
        except:
            pass
            return None

    def season_mu(season, dow, df):
        try:
            adr_ = df[dow][df["season_number"] == season]
            adr = adr_[season-1]
            return adr
        except:
            pass
            return None

    mu_pace = df_.apply(lambda x: pace_mu(x["Pace"],x["Day Of Week"],rate_pace_df),axis=1)
    mu_season = df_.apply(lambda x: season_mu(x["Season"],x["Day Of Week"],rate_mu_df),axis=1)
    logger.info(f"Merging Pace information with current data in diagonal pattern")
    logger.info(f"Merging Mu_sigma information with current data in diagonal pattern")
    df_["Pace_MU"] = mu_pace
    df_["Season_MU"] = mu_season

    logger.info(f"Introducing column 'MU' where first 90 days pace comes from calibrated pace information"
                f" and others from calibrated mu_sigma information")
    df_["MU"] = np.where(df_["Pace"]>90,df_["Season_MU"],df_["Pace_MU"])
    logger.info(f"Multiplying MU with inflation factor to achieve inflated rates")
    df_["Inflaction_Mu"] = df_["MU"] * inf_factor

    logger.info(f"Applying possion distribution for inflated_mu adr with all four rates")
    df_["prcp_Max"] = poisson._pmf(df_["rate_rcp"],df_["Inflaction_Mu"])
    df_["pmpi_Max"] = poisson._pmf(df_["rate_mpi"],df_["Inflaction_Mu"])
    df_["ppqm_Max"] = poisson._pmf(df_["rate_pqm"],df_["Inflaction_Mu"])
    df_["pari_Max"] = poisson._pmf(df_["rate_ari"],df_["Inflaction_Mu"])

    dict = {"prcp_Max":"rate_rcp",
            "pmpi_Max":"rate_mpi",
            "ppqm_Max":"rate_pqm",
            "pari_Max":"rate_ari"}


    logger.info(f"Identifying Algo having minimum rates and Possion_Distributed Algo with maximum distribution")
    df_["possion_max"] = df_[["prcp_Max", "pmpi_Max", "ppqm_Max", "pari_Max"]].idxmax(axis=1)
    df_["algo_min"] =  df_[["rate_rcp","rate_mpi","rate_pqm","rate_ari"]].idxmin(axis=1)

    df_["sum_possion"] = df_[["prcp_Max","pmpi_Max","ppqm_Max","pari_Max"]].sum(axis=1)
    df_["min_rate"] = df_[["rate_rcp","rate_mpi","rate_pqm","rate_ari"]].min(axis=1)
    df_["possion_max"] = df_["possion_max"].apply(lambda x: dict.get(x))

    # max_rate = []
    # for i in range(len(df_.index)):
    #     max_of_possion = df_["possion_max"][i]
    #     max_rate_ = df_[max_of_possion][i]
    #     max_rate.append(max_rate_)
    #
    # df_["max_rate"] = max_rate
    #
    logger.info(f"Identifying rate and algo selected")
    # df_["rate_selected"] = np.where(df_["sum_possion"]==0,df_["min_rate"],df_["max_rate"])
    df_["algo_selected"] = np.where(df_["sum_possion"]==0,df_["algo_min"],df_["possion_max"])

    dict_rename_algo = {"rate_rcp":"RCP",
            "rate_mpi":"MPI",
            "rate_pqm":"PQM",
            "rate_ari":"ARI"}
    df_["algo_selected"] = df_["algo_selected"].map(dict_rename_algo)

    dict_algo = {k: v for k, v in zip(df_["checkin_date"],df_["algo_selected"])}

    # df_["checkin_date"] = df_["checkin_date"].dt.date
    # df_["creation_date"] = df_["creation_date"].dt.date
    # df_.to_csv("E:/Jigar/Rate_Analysis_NewCal_.csv")

    logger.info(f"---- Single Rate Recomendation Done DONE ----")
    return dict_algo

# if __name__ == '__main__':
#     # cid = 64
#     # rate_recomendation(df_,cid,config)
#     print("\n\n DoNe \n")

