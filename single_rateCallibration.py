import pandas as pd
import numpy as np
from scipy.stats import poisson
import datetime
import configparser
import os
import connectdb
import rate_inf_callibration as dte
import getData
import loadData
import logging
# import log
import timeit


# logger = logging.getLogger(__name__)
logger = logging.getLogger(f'Calbrservicelog.{__name__}')
logger.info(f'Starting Rate Analysis environment for Recommendation')


## Creating objects to all classes we access
op = dte.Operations()
ot = dte.Outliers()
inf = dte.Inflation()
mu = dte.Mu_Sigma()
pace_dow_mu = dte.Pace_Dow()


def rate_inf_factor(cid,config):

    hotel_id_ = "Select hotel_id from clients where id = :prid"
    hotel_id = getData.getData(myquery=hotel_id_, pid=cid, db_confg=config)

    df_all_data_query = "SELECT occupancy_date as 'Date', " \
        "pace as Pace, no_of_rooms as Rooms, rpd as Revenue " \
        "FROM revseed.booking_pace_occupancy_by_date where client_id =:prid"
    try:
        df_all_data = getData.getData(myquery=df_all_data_query, pid=cid, db_confg=config)
        logger.info(f'Reading Data For Inflation RateCalibration')
    except:
        logger.info(f'Failed Reading Data For Inflation RateCalibration')

    # Modifying data based on dates
    df_all_data["Date"] = pd.to_datetime(df_all_data["Date"],errors = 'coerce')
    df_all_data = df_all_data[df_all_data["Date"].notna()]
    df_all_data = df_all_data.sort_values(["Date"])

    # Calculating Booking and Rate (ADR) in new columns
    df_all_data["Booking"] = df_all_data["Date"] - pd.to_timedelta(df_all_data["Pace"],unit='d')
    df_all_data["Rate"] = np.where(df_all_data["Rooms"]>0,df_all_data["Revenue"]/df_all_data["Rooms"],0)
    df_all_data["Rate"].replace([np.inf, -np.inf], 0, inplace=True)

    # Fetching day of week information from db
    dow_query = "select day as dow, type as daytype from dow_definitions WHERE client_id =:prid and season_no=0"
    logger.info("--- Fetching the Day of Week Definition ---")
    df_dow = getData.getData(myquery=dow_query, pid=cid, db_confg=config)
    df_dow_ = {k: v for k, v in zip(df_dow["dow"],df_dow["daytype"])}

    # adding columns for dow and wdwe, based the information received from db
    df_all_data["Date"] = pd.to_datetime(df_all_data["Date"])
    df_all_data["DOW"] = df_all_data["Date"].dt.day_name()
    df_all_data["WDWE"] = df_all_data["DOW"].map(df_dow_)
    df_all_data["WDWE"] = np.where(df_all_data["WDWE"] == "weekend","WE","WD")
    ## df_all_data["WDWE"] = np.where(df_all_data["Date"].dt.dayofweek > 3,"WE","WD")

    # Calculating week number for all dates.
    df_all_data['Week_Num'] = df_all_data['Date'].apply(lambda x:op.week_number(x))
    df_all_data['Week_Num'] = np.where((df_all_data['Week_Num'] == 53), 0, df_all_data['Week_Num'])
    df_all_data['Week_Num'] = np.where((df_all_data['Week_Num'] == 0), 52, df_all_data['Week_Num'])

    # Calculating week number for all dates (old method)
    # df_all_data['d1'] = 8
    # df_all_data['d2'] = 6
    # df_all_data['m1'] = 1
    # df_all_data['dow'] = df_all_data['Date'].dt.day_name()
    # df_all_data['dtyr'] = df_all_data['Date'].dt.year
    # df_all_data['dt1']=pd.to_datetime(df_all_data['dtyr'].astype(str) + '-' +
    #                         df_all_data['m1'].astype(str) + '-' +
    #                         df_all_data['d1'].astype(str))
    # df_all_data['dt2'] = pd.to_datetime(df_all_data['dtyr'].astype(str) + '-' +
    #                           df_all_data['m1'].astype(str) + '-' +
    #                           df_all_data['d2'].astype(str))
    # df_all_data['dtwk'] = df_all_data['dt2'].dt.dayofweek + 2
    # df_all_data['dt3'] = df_all_data['dt1'] - pd.TimedeltaIndex(df_all_data['dtwk'], unit='D')
    # df_all_data['dtdf'] = (df_all_data['dt3'] - df_all_data['Date']).apply(lambda x: x / np.timedelta64(1, 'D'))
    # df_all_data.loc[(df_all_data['dtdf'] > 0.0), 'WkNum'] = 52
    # df_all_data.loc[(df_all_data['dtdf'] <= 0.0), 'WkNum'] = df_all_data['Date'].apply(lambda x: x.strftime('%W'))
    # df_all_data['WkNum'] = pd.to_numeric(df_all_data['WkNum'])
    # df_all_data['Week_Num'] = np.where((df_all_data['WkNum'] == 53), 0, df_all_data['WkNum'])
    # df_all_data['Week_Num'] = np.where((df_all_data['Week_Num'] == 0), 52, df_all_data['WkNum'])
    #
    # df_all_data = df_all_data.drop(["d1","d2","m1","dow","dtyr","dt1","dt2","dtwk","dt3","dtdf","WkNum"],axis=1)

    # Fetching seasonality information from db for particular hotel
    season_query = "select hotel_id, start_week, end_week, max_capacity FROM seasonality_definitions WHERE client_id =:prid"
    logger.info("--- Fetching the Day of Season Definition ---")
    df_season = getData.getData(myquery=season_query, pid=cid, db_confg=config)

    # Adding season number column for each dates
    df_all_data["Season"] = df_all_data['Week_Num'].apply(lambda x:op.season_number(x,df_season))


    ## Removing outliers from data (above 95 percentile & below 5 percentile)
    ## Assigning Outliers as 0 and not Outliers1 as 1
    ## ## Removing outliers from data based on season (above 75 iqr & below 25 iqr)
    ## Assigning Outliers as 0 and not Outliers2 as 1
    df_after_outlier = ot.outliers_allData(df_all_data)
    logger.info(f'Assigned Outliers as 0 and non Outliers as 1')

    # passing parameters, period, seasons, wdwe, dow list that are used throughout the process
    parameters = ["Interest","Inflation","Elevation"]
    period = ["TYp","LYp","LYf"]
    seasons = list(df_after_outlier["Season"].unique())
    seasons.sort()
    dow = list(df_after_outlier["DOW"].unique())
    dow_key = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday","Saturday","Sunday"]
    dow.sort(key=dow_key.index)
    wdwe = list(df_after_outlier["WDWE"].unique())
    wdwe.sort()
    current_date = datetime.datetime.today()

    ## Filter DataFrame whose Outlier_cson is 1. This Dataframe will be used for calculating inflation factor and mu_sigma
    df_for_inf_mu = df_after_outlier[df_after_outlier["Outlier_cson"]==1]
    logger.info(f"Fetching data where outlier is set to 1, DF size == {df_for_inf_mu.shape}")

    ## extracting date based on today or date of running rate analysis from db
    system_date = "select system_today from revseed.clients where id =:prid"
    date_today_ = getData.getData(myquery=system_date, pid=cid, db_confg=config)
    date_today = date_today_["system_today"][0]
    date_today = date_today.replace(hour=00, minute=00, second=00)
    logger.info(f"Run Date == {date_today}")

    """
    Calculating Inflation Factor
    """
    logger.info(f"Calculating Inflation Factor")

    logger.info(f"Calculating pace and actuals for inflation")
    pace_df,act_df,range = inf.pace_act(df_for_inf_mu,date_today)
    logger.info(f"Range for Inflation factor :: {range}")


    ## Calculating Difference (i.e. increment)
    logger.info(f"Calculating difference between actual and pace, Increment values")
    incremented_df = act_df - pace_df
    incremented_df["ADR"] = incremented_df["Revenue"] / incremented_df["Sold"]


    ## Calculating parameters
    logger.info(f"Calculating Interest, Inflation and Elevation based on pace, actual and increment values")
    inflation_df = pd.DataFrame()
    inflation_df[parameters[0]] = act_df["ADR"]/pace_df["ADR"]
    inflation_df[parameters[1]] = incremented_df["ADR"]/pace_df["ADR"]
    inflation_df.replace([np.inf, -np.inf], 1, inplace=True)
    inflation_df[parameters[2]] = inflation_df[parameters[1]] / inflation_df[parameters[0]]


    ## Calculating possible ADR values
    logger.info(f"Calculating possible ADR values based on Interest,Inflation and Elevation ADR")
    adr_cal = []
    for i in period:
        for j in parameters:
            adr_value = inf.cal(pace_df,act_df,inflation_df,i,j)
            adr_cal.append(adr_value)

    adr_calculation = {"ADR":adr_cal}
    adr_calculation_df = pd.DataFrame(adr_calculation)

    ## Calulating median, mean and average (max & min) of possible adr values
    logger.info(f"Calulating median, mean and average (max & min) of possible adr values")
    median_adr = adr_calculation_df["ADR"].median()
    mean_adr = adr_calculation_df["ADR"].mean()
    average_adr = (adr_calculation_df["ADR"].max() + adr_calculation_df["ADR"].min()) / 2

    ## Calculating Possion Distribution for possible adr values
    logger.info(f"Calculating Possion Distribution for possible adr values")
    adr_calculation_df["Distribution"] = np.where(adr_calculation_df["ADR"]>0,
                                                  poisson._pmf(adr_calculation_df["ADR"],
                                                               median_adr),0)

    adr_calculation_df["Max"] = np.where(adr_calculation_df["Distribution"] == adr_calculation_df["Distribution"].max(),
                                         1,0)


    ## Calculating Scaled ADR and Inflation Factor
    logger.info(f"Calculating Scaled ADR and Inflation Factor")
    scaled_adr_typ = adr_calculation_df["ADR"][adr_calculation_df["Max"]==1].mean()


    adr_tyf = pace_df["ADR"]["TYf"]
    rt_inf_typ = scaled_adr_typ / adr_tyf if adr_tyf>0 else inflation_df["Interest"]["TYp"]
    logger.info(f"Inflaction Factor :: {rt_inf_typ}")
    logger.info(f"Scaled ADR :: {scaled_adr_typ}")


    inf_df_ = {'client_id':[cid],
         'hotel_id' : [hotel_id["hotel_id"][0]],
         'TYp0' : [date_today.date()],
         'Scaled ADR' : [scaled_adr_typ],
         'Inflation_Factor' : [rt_inf_typ],
         'Updated_Date' : [current_date]}

    inf_df = pd.DataFrame(inf_df_)

    # Deleting old inflation factor that is present in db and loading new inflation factor based on system date
    cnx = connectdb.conectdb(config)
    rate_inf_del = f"Delete From rate_inflation_factor where client_id ={cid}"
    cnx.execute(rate_inf_del)
    loadData.loadData(inf_df, 'rate_inflation_factor', config)

    """
    Calculating Mu_Sigma ADR based on Season and DOW
    """
    ## Calcualting mean and median ADR based season and DOW
    logger.info(f"Calcualting mean and median ADR based season and DOW")
    season_week_obs, adr_median,adr_avg,range = mu.sigma_adr(df_for_inf_mu,dow,seasons,wdwe,date_today)
    logger.info(f"Range for Mu_Sigma :: {range}")


    ## Calcualting callibrated ADR based season and DOW
    logger.info(f"Calcualting callibrated ADR based season and DOW")
    calibrated_adr = pd.DataFrame()
    calibrated_adr = pd.DataFrame(calibrated_adr, columns=dow+wdwe,index=seasons)

    calibrated_adr.loc[0] = mu.min_max(rt_inf_typ,list(adr_median.loc["Avg_Dow"][:-1]),list(adr_avg.loc["Avg_Dow"][:-1]))
    calibrated_adr["Total"] = mu.min_max(rt_inf_typ,list(adr_median["Avg_season"]),list(adr_avg["Avg_season"]))

    ## Calculating DOW Factor
    logger.info(f"Claculating DOW Factor")
    dow_factor_ = [i/calibrated_adr["Total"][0] for i in list(calibrated_adr.loc[0][:-1])]
    dow_factor = pd.DataFrame(dow_factor_).transpose()
    dow_factor_column = {k: v for k, v in zip(list(dow_factor.columns),dow+wdwe)}
    # dow_factor = dow_factor.rename(columns=dict(zip(list(dow_factor.columns),dow+wdwe)))
    dow_factor = dow_factor.rename(columns=dow_factor_column)
    # print("\n\n DOW Factor : \n",dow_factor)


    from collections import Counter
    # dow_wdwe = dict(zip(df["DOW"], df["WDWE"]))
    dow_wdwe = {k: v for k, v in zip(df_for_inf_mu["DOW"], df_for_inf_mu["WDWE"])}
    logger.info(f"Assigning values callibrated ADR based season and DOW and DOW Factor")
    for i in seasons:
        for j in dow+wdwe:
            try:
                calibrated_adr[j][i] = np.where(season_week_obs[j][i] < 30,
                                    np.where(
                                        season_week_obs["Tot"][i]<(22 * Counter(dow_wdwe.values())[dow_wdwe[j]]),
                                        calibrated_adr[j][0],
                                        calibrated_adr["Total"][i] * dow_factor[j][0])
                                        ,mu.min_max(rt_inf_typ,adr_median[j][i],adr_avg[j][i]))

            except:
                calibrated_adr[j][i] = np.where(season_week_obs[j][i] < 30,
                                                np.where(
                                                    season_week_obs["Tot"][i] < (22 * list(dow_wdwe.values()).count(j)),
                                                    calibrated_adr[j][0],
                                                    calibrated_adr["Total"][i] * dow_factor[j][0])
                                                , mu.min_max(rt_inf_typ, adr_median[j][i], adr_avg[j][i]))

    column_week_names = {"Monday":"dow1",
                         "Tuesday":"dow2",
                         "Wednesday":"dow3",
                         "Thursday":"dow4",
                         "Friday":"dow5",
                         "Saturday":"dow6",
                         "Sunday":"dow7",
                         "WD":"week_days",
                         "WE":"week_ends",
                         "Total":"all_days",}
    calibrated_adr = calibrated_adr.rename(columns = column_week_names)
    calibrated_adr["season_number"] = calibrated_adr.index
    calibrated_adr["hotel_id"] = hotel_id["hotel_id"][0]
    calibrated_adr["client_id"] = cid
    calibrated_adr["updated_date"] = current_date

    # Deleteing old mu data that is present in db based on clients and loading new calibrated rates
    rate_mu_del = f"Delete From rate_mu_calibration where client_id ={cid}"
    cnx.execute(rate_mu_del)
    loadData.loadData(calibrated_adr,'rate_mu_calibration',config)

    """
    Calculating Pace, Adjusted Pace wrt DOW
    """
    logger.info(f"Calculating Pace wrt DOW")
    df_for_pace = df_after_outlier[df_after_outlier["Outlier_dow"]==1]
    count_dow, pace_dow ,stats_df, adjusted_adr_df,range = pace_dow_mu.dow_count(df_for_pace,dow,wdwe,date_today)
    logger.info(f"Range for Pace :: {range}")


    adjusted_adr_df = adjusted_adr_df.rename(columns = column_week_names)
    adjusted_adr_df["pace_points"] = adjusted_adr_df.index
    adjusted_adr_df["hotel_id"] = hotel_id["hotel_id"][0]
    adjusted_adr_df["client_id"] = cid
    adjusted_adr_df["updated_date"] = current_date

    # Deleteing old pace data that is present in db based on clients and loading new calibrated rates
    rate_pace_del = f"Delete From rate_pace_calibration where client_id ={cid}"
    cnx.execute(rate_pace_del)
    loadData.loadData(adjusted_adr_df,'rate_pace_calibration',config)


    # writer = pd.ExcelWriter("E:\Jigar\Rate_Analysis_New/new_trial_03.xlsx", engine = 'xlsxwriter')
    # pace_df.to_excel(writer, sheet_name="Inflation", startrow=1)
    # act_df.to_excel(writer, sheet_name="Inflation", startrow=8)
    # incremented_df.to_excel(writer, sheet_name="Inflation", startrow=15)
    # inflation_df.to_excel(writer, sheet_name="Inflation", startrow=21)
    # adr_calculation_df.to_excel(writer, sheet_name="Inflation", startrow=28)
    #
    # inf_df.to_excel(writer, sheet_name="Inflation_DF", startrow=1)
    #
    # season_week_obs.to_excel(writer, sheet_name="Mu_Sigma", startrow=1)
    # adr_median.to_excel(writer, sheet_name="Mu_Sigma", startrow=11)
    # adr_avg.to_excel(writer, sheet_name="Mu_Sigma", startrow=21)
    # calibrated_adr.to_excel(writer, sheet_name="Mu_Sigma", startrow=31)
    #
    # pace_dow.to_excel(writer, sheet_name="Pace_DOW", startrow=1)
    # adjusted_adr_df.to_excel(writer, sheet_name="Pace_Dow_Mu", startrow=1)
    #
    # writer.save()
    # writer.close()

    return None

def run_single_rateCallibration(cid,config):
    logger.info(f"----Running Single Rate Inflation Callibration----")
    run_inflation_q = "Select param_value " \
                      "from property_parameters " \
                      "where param_name = 'Run_Inflation' and client_id = :prid" #Run_Inflation
    run_inflation_ = getData.getData(myquery=run_inflation_q, pid=cid, db_confg=config)
    logger.info(f"Getting param_value for Run_Inflation parameter from property parameters for {cid}")

    try:
        run_inflation = run_inflation_["param_value"][0]
        if run_inflation.lower() == '1':
            logger.info(f"Run_Inflation parameter for client {cid} is '{run_inflation}'")
            logger.info(f"Calibrating Inflation, Mu_sigma and Pace for client {cid}")
            rate_inf_factor(cid, config)
        else:
            logger.info(f"Run_Inflation parameter for client {cid} is '{run_inflation}'")
            pass
    except:
        # print(f"Run Inflation parameter not found for client {cid}")
        logger.info(f"Run Inflation parameter not found for client {cid}")

    logger.info(f"---- Single Rate Callibration DONE ----")
    return None


# if __name__ == '__main__':
    # cid = 64
    # run_rateAnalysis(cid,config)
    # print("\n\n DoNe \n")