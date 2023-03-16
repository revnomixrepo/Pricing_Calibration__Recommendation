import pandas as pd
import numpy as np
import datetime
import logging
from datetime import date,timedelta
import time

logger = logging.getLogger(f'Calbrservicelog.{__name__}')

class Operations():
    """
    Operation Class contains basic functions that are used throughout the Callibration Process
    """
    def __init__(self):
        self.logger = logging.getLogger(f'Calbrservicelog.{__name__}')
        pass

    def date_ops(self,date_today, range, operator):
        """
        date method calculates date that is required for the process based on operator and run_date
        :param date_today: Run Date of Calibration service
        :param range: number of days to the past or future
        :param operator: 2 values: "past" or "future"
        :return: If operator "past" returns returns the past date based on range.
        If operator "future" returns returns the future date based on range.
        """
        if operator == "past":
            date_t = date_today - datetime.timedelta(days=range)
        else:
            date_t = date_today + datetime.timedelta(days=range)
        return date_t

    def filter_df(self,df,date_past, date_future):
        """
        filter_df method fetches the data between certain date range (between past and future).
        :param df: dataframe that is required to slice
        :param date_past: start date for new datafarme
        :param date_future: end date for new dataframe
        :return: sliced dataframe between provided date range
        """
        self.logger.info(f"filtering dataframe between {date_past} to {date_future}")
        filtered_df = df[(df["Date"] >= date_past) & (df["Date"] <= date_future)]
        return filtered_df

    def values_df(self,df,col_name,list_2,list_1,col_1,col_2,method):
        """
        This method creates new data frame with calculated values from original data between date range.
        This Calculations extracts Mean, Count for provided column after slicing on col_1 and col_2.
        :param df: dataframe from which data is to calculated
        :param col_name: column name on which calculation method is to applied
        :param list_2: list that will be reflected as column of new dataframe
        :param list_1: list that will be reflected as index of new dataframe
        :param col_1: column name in dataframe that refers to list_1. This will help in slicing dataframe.
        :param col_2: column name in dataframe that refers to list_2. This will help in slicing dataframe.
        :param method: method that is to be applied for calculation to col_name in dataframe
        :return: new dataframe with calculated values
        """
        df_new = pd.DataFrame()
        df_new = pd.DataFrame(df_new, columns=list_2, index=list_1)
        self.logger.info(f"Creating new dataframe wrt {list_2} as columns and {list_1} as index")
        for i in list_1:
            for j in list_2:
                try:
                    df_new[j][i] = df[col_name][(df[col_1] == i) & (df[col_2] == j)].apply(method)
                except:
                    pass
        # df_new = df_new.fillna(0)
        return df_new


    def alldays(self,year):
        week_start = time.strptime('Monday', "%A").tm_wday
        d = date(year, 1, 1)
        # d += timedelta(days=(self.weeknum(week_start) - d.weekday()) % 7)
        d += timedelta(days=(week_start - d.weekday()) % 7)
        while d.year == year:
            yield d
            d += timedelta(days=7)

    def week_number(self,date):
        # print(date)
        specificdays = [d for d in self.alldays(date.year)]
        return len([specificday for specificday in specificdays if specificday <= date])

    def season_number(self,week_num,season_df):
        season_df = season_df.loc[1:]
        try:
            count = season_df.loc[(week_num >= season_df["start_week"]) & (week_num <= season_df["end_week"])].index.values.astype(int)[0]
        except:
            count = season_df.loc[(week_num >= season_df["start_week"]) | (week_num <= season_df["end_week"])].index.values.astype(int)[0]

        return count

class Outliers:
    def __init__(self):
        # self.logger = logging.getLogger(__name__)
        self.logger = logging.getLogger(f'Calbrservicelog.{__name__}')
        pass

    def outliers_allData(self,df):
        """
        This method identifies outliers from the data. It indentifes two types of outliers.
        Outlier_Tot is identified from the overall data where percentile on "RATE" is above 95 percentile & below 5 percentile and set to 1
        Outlier_cson is identified from the season wise data and Outlier_Tot is 1 where percentile on "RATE" is above IQR 25 percentile & below 75 percentile and set to 1.
        :param df: orginal dataframe fetched
        :return: new dataframe with two new columns Outlier_Tot & Outlier_cson & Outlier_dow.
        """
        ## Removing outliers from data (above 95 percentile & below 5 percentile)
        ## Assigning Outliers as 0 and not Outliers as 1
        percentile_ = df["Rate"][df["Rate"] > 0].to_list()

        self.logger.info(f"Identifying outliers from data (above 95 percentile & below 5 percentile)")
        P95 = np.percentile(percentile_, 95)
        P05 = np.percentile(percentile_, 5)

        df["P95"] = P95
        df["P05"] = P05
        df["Outlier_Tot"] = np.where((df["Rate"] < P95) & (df["Rate"] > P05), 1, 0)

        seasons = df["Season"].drop_duplicates().dropna().to_list()
        dow = df["DOW"].drop_duplicates().dropna().to_list()
        dow_key = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        dow.sort(key=dow_key.index)

        self.logger.info(f"Identifying outliers from data that is not outlier (below 95 percentile & above 5 percentile) for each 'season' (above 75 percentile IQR & below 25 IQR)")
        for i in seasons:
            iqr_percentile_ = df["Rate"][(df["Outlier_Tot"] == 1) & (df["Season"] == i)].to_list()
            Q75 = np.percentile(iqr_percentile_, 75)
            Q25 = np.percentile(iqr_percentile_, 25)

            upper_limit_iqr = Q75 + ((Q75 - Q25) * 1.5)
            lower_limit_iqr = Q25 - ((Q75 - Q25) * 1.5)

            try:
                df["Q75"] = np.where(df["Season"] == i, Q75, df["Q75"])
                df["Q25"] = np.where(df["Season"] == i, Q25, df["Q25"])
                df["upper_limit_iqr"] = np.where(df["Season"] == i, upper_limit_iqr, df["upper_limit_iqr"])
                df["lower_limit_iqr"] = np.where(df["Season"] == i, lower_limit_iqr, df["lower_limit_iqr"])
            except:
                df["Q75"] = np.where(df["Season"] == i,Q75,0)
                df["Q25"] = np.where(df["Season"] == i,Q25,0)
                df["upper_limit_iqr"] = np.where(df["Season"] == i,upper_limit_iqr,0)
                df["lower_limit_iqr"] = np.where(df["Season"] == i,lower_limit_iqr,0)

            try:
                df["Outlier_cson"] = np.where(df["Season"] == i, np.where(
                    (df["Rate"] < upper_limit_iqr) & (df["Rate"] > lower_limit_iqr), 1, 0), df["Outlier_cson"])
            except:
                df["Outlier_cson"] = np.where(df["Season"] == i, np.where(
                    (df["Rate"] < upper_limit_iqr) & (df["Rate"] > lower_limit_iqr), 1, 0), 0)

        for i in dow:
            iqr_percentile_ = df["Rate"][(df["Outlier_Tot"] == 1) & (df["DOW"] == i)].to_list()
            Q75_ = np.percentile(iqr_percentile_, 75)
            Q25_ = np.percentile(iqr_percentile_, 25)

            upper_limit_iqr_ = Q75_ + ((Q75_ - Q25_) * 1.5)
            lower_limit_iqr_ = Q25_ - ((Q75_ - Q25_) * 1.5)
            try:
                df["Q75_"] = np.where(df["DOW"] == i, Q75_, df["Q75_"])
                df["Q25_"] = np.where(df["DOW"] == i, Q25_, df["Q25_"])
                df["upper_limit_iqr_"] = np.where(df["DOW"] == i, upper_limit_iqr_, df["upper_limit_iqr_"])
                df["lower_limit_iqr_"] = np.where(df["DOW"] == i, lower_limit_iqr_, df["lower_limit_iqr_"])
            except:
                df["Q75_"] = np.where(df["DOW"] == i, Q75_, 0)
                df["Q25_"] = np.where(df["DOW"] == i, Q25_, 0)
                df["upper_limit_iqr_"] = np.where(df["DOW"] == i, upper_limit_iqr_, 0)
                df["lower_limit_iqr_"] = np.where(df["DOW"] == i, lower_limit_iqr_, 0)

            try:
                df["Outlier_dow"] = np.where(df["DOW"] == i, np.where(
                    (df["Rate"] < upper_limit_iqr_) & (df["Rate"] > lower_limit_iqr_), 1, 0), df["Outlier_dow"])
            except:
                df["Outlier_dow"] = np.where(df["DOW"] == i, np.where(
                    (df["Rate"] < upper_limit_iqr_) & (df["Rate"] > lower_limit_iqr_), 1, 0), 0)

        return df

class Inflation():
    """
    Inflation class contains functions that helps in calculating Inflation Factor
    """
    def __init__(self):
        self.ops = Operations()
        self.logger = logging.getLogger(f'Calbrservicelog.{__name__}')
        pass

    def date_inf(self,date_today, range):
        """
        date method calculates multiple dates required for Inflation. This method calls Date method of Operation class.
        :param date_today: Run Date
        :param range: number of days to the past or future
        :return: date 90 days back, same time last year, date 90 days back to stly date,
        past date & future date based on range, past date & future date to stly date based on range
        """
        ## extracting date based on today or date of running rate analysis
        d1_ty = self.ops.date_ops(date_today,90,"past")  ## date 90 days back
        d2_ty = self.ops.date_ops(date_today,range,"past")  ## past date
        d3_ty = self.ops.date_ops(date_today,range,"future")  ## future date

        date_stly = self.ops.date_ops(date_today,364,"past") ## same time last year
        d1_ly = self.ops.date_ops(date_stly,90,"past")  ## date 90 days back last year
        d2_ly = self.ops.date_ops(date_stly,range,"past")  ## past date last year
        d3_ly = self.ops.date_ops(date_stly,range,"future")  ## future date last year

        return d1_ty, d2_ty, d3_ty, date_stly, d1_ly,d2_ly,d3_ly

    ## Fetching pace/actual data
    def fetch(self,df, past, future, p90, pace):
        """
        This method fetches count of observation, Total rooms,Total Revenue & calculates ADR for particular date range.
        :param df: dataframe after getting the outliers, which helps in calculating adr for non-outlier observations
        :param past: start date for calculations
        :param future: end date for calculations
        :param p90: date that is 90 days back or None.
        If p90 is None, it calculates actual value for all date range (TYp,LYp,LYf,TYf).
        If p90 is date, it calculates pace values for inflation
        :param pace: True or False
        If True, it calculates pace for past date range (LYp,TYp).
        If False, it calculates pace for future date range (LYf)
        :return: pace dataframe or actual dataframe based on parameters for four time periods (TYp,LYp,LYf,TYf)
        """

        ## Filtering df based on date range
        df_1 = self.ops.filter_df(df,date_past= past,date_future=future)

        ## Filtering dataframe for Actuals
        if p90==None:
            df_1 = df_1

        ## Filtering dataframe for Pace
        else:
            if pace == True:
                df_1 = df_1[df_1["Pace"] >= 30]
            else:
                df_1 = df_1[df_1["Booking"] <= p90]

        ## Calculating number of observations
        df_ty_past_observations = df_1["Outlier_cson"].sum()

        df_2 = df_1[df_1["Date"]<future]

        ## Calculating total rooms
        df_ty_past_sold = df_2["Rooms"].sum()
        ## Calculating total revenue
        df_ty_past_revenue = df_2["Revenue"].sum()
        ## Calculating ADR
        df_ty_past_adr = (df_ty_past_revenue / df_ty_past_sold) if df_ty_past_sold != 0 else 0

        new_df_ = {"Observations": [df_ty_past_observations],
             "Sold": [df_ty_past_sold],
             "Revenue": [df_ty_past_revenue],
             "ADR": [df_ty_past_adr]}
        new_df = pd.DataFrame(new_df_)

        return new_df

    def pace_act(self,df,date_today):
        """
        This method calculates pace values and actual values with help of fetch method
        :param df: dataframe after getting the outliers, which helps in calculating adr for non-outlier observations
        :param date_today: Run Date
        :return: dataframe with pace values & actual values for four time periods (TYp,LYp,LYf,TYf)
        """
        range = 90  ## Setting start range to 90, increase by 30

        self.logger.info(f"Calculating Pace values until all period has 30 oservation or max date range is 180")
        ## Extracting Pace until all period has 30 oservation or max date range is 180
        while range <= 180:
            d1_ty, d2_ty, d3_ty, date_stly, d1_ly, d2_ly, d3_ly = self.date_inf(date_today, range)

            ## Calculating Pace
            typ_pace = self.fetch(df, d2_ty, date_today, d1_ty, pace=False)
            lyp_pace = self.fetch(df, d2_ly, date_stly, d1_ly, pace=False)
            lyf_pace = self.fetch(df, date_stly, d3_ly, date_stly, pace=False)

            ## Calculating TY act
            tyf_pace = self.fetch(df, date_today, d3_ty, p90=None, pace=False)

            pace_df_ = typ_pace.append([lyp_pace, lyf_pace, tyf_pace], ignore_index=True)
            pace_df = pace_df_.rename(index={0: "TYp", 1: "LYp", 2: "LYf", 3: "TYf"})

            if all(i >= 30 for i in pace_df["Observations"].to_list()):
                break
            else:
                range += 30
        self.logger.info(f"Range for calculating Inflation :: {range}, Not more than 180")
        self.logger.info(f"Calculating Actual values for particular range")
        ## Calculating Actuals values for particular range as of what pace values were calculated
        typ_act = self.fetch(df, d2_ty, date_today, p90=None, pace=False)
        lyp_act = self.fetch(df, d2_ly, date_stly, p90=None, pace=False)
        lyf_act = self.fetch(df, date_stly, d3_ly, p90=None, pace=False)

        act_df_ = typ_act.append([lyp_act, lyf_act], ignore_index=True)
        act_df = act_df_.rename(index={0: "TYp", 1: "LYp", 2: "LYf"})

        return pace_df, act_df,range

    def cal(self, pace_df, act_df, inflation_df, period, ratio):
        """
        This method calculates possible ADR values based on Interest,Inflation and Elevation ADR for all time periods
        :param pace_df: Dataframe with pace values
        :param act_df: Datafram with actual values
        :param inflation_df:  Dataframe with Interest,Inflation and Elevation ADR
        :param period: Time Periods (TYp,LYp,LYf,TYf)
        :param ratio: Interest, Inflation or Elevation
        :return: dataframe with all possible ADR values based on ratio
        """
        adr_tyf = pace_df["ADR"]["TYf"]

        if act_df["Observations"][period]>=30:
            if adr_tyf == 0:
                future_adr = act_df["ADR"][period]
            else:
                future_adr = adr_tyf * inflation_df[ratio][period]

        return future_adr


class Mu_Sigma():
    """
    Mu_Sigma Class contains functions that helps in calculting Calbrated ADR values based on seasons and dow.
    This class calculates median and avg ADR values for particular season and dow or WD/WE
    """
    def __init__(self):
        self.ops = Operations()
        self.logger = logging.getLogger(f'Calbrservicelog.{__name__}')
        pass

    def overall_adr(self,df,col_1,list,col_2,method):
        """
        This method calculates ADR values from dataframe for single filter based on given method
        :param df: dataframe after getting the outliers, which helps in calculating adr for non-outlier observations
        :param col_1: column name on which calculation method is to applied
        :param list: list that will be provide condition
        :param col_2: column name in dataframe that refers to list. This will help in slicing dataframe.
        :param method: method that is to be applied for calculation to col_name in dataframe
        :return: list with calculated ADR values
        """
        adr_values = []
        for i in list:
            adr_values.append(df[col_1][(df[col_2] == i)].apply(method))

        return adr_values

    def mu_overall_adr_iferror(self,list,overall_value):
        for i in range(len(list)):
            if isinstance(list[i],float):
                pass
            else:
                list[i] = overall_value
        return list

    def club_df(self,df,list_week,list_season,avg):
        """
        This method helps in joining two list with existing dataframe having columns as DOW and index as Seasons
        :param df: Calculted dataframe having adr values based on season an dow
        :param list_week: list having values of adr based on dow
        :param list_season: list having values of adr based on seasons
        :param avg: overall ADR value for date range
        :return: new data frame with merging lists with existing dataframe
        """
        list_season.append(avg)
        df.loc["Avg_Dow"] = list_week
        df["Avg_season"] = list_season
        return df

    def sigma_adr(self, df, dow, season, wdwe, date_today):
        """
        This method helps in calculting median and avg ADR values, number of observations for particular season and dow or WD/WE
        :param df: dataframe after getting the outliers, which helps in calculating adr for non-outlier observations
        :param dow: list have days of the week
        :param season: list having seasons (numeric, currently 6 seasons)
        :param wdwe: list saying wd or we. unique list extracted from WDWE column
        :param date_today: Run Date
        :return: dataframes having values count of observations, median adr, and avg adr
        """
        range = 365
        self.logger.info(f"Calculating median and avg values until all season and dow has 30 oservation or max date range is 740")
        while range <= 740:
            date_past = self.ops.date_ops(date_today,range,"past")
            df_1 = self.ops.filter_df(df,date_past,date_today)
            # df_1 = df[(df["Outlier_cson"] == 1) & (df["Date"] >= date_past) & (df["Date"] <= date_today)]
            df_2 = df[df["Date"] >= date_past]

            # season_week_obs = self.season_dow_adr(df_1,"Outlier_cson",dow,season,"count")
            season_week_obs = self.ops.values_df(df_1,"Outlier_cson",dow,season,"Season","DOW","count")
            season_week_o_bs = self.ops.values_df(df_1,"Outlier_cson",wdwe,season,"Season","WDWE","count")
            season_week_obs["WD"] = season_week_o_bs["WD"]
            season_week_obs["WE"] = season_week_o_bs["WE"]
            season_week_obs = season_week_obs.fillna(0)
            if all(i>=30 for j in season_week_obs.values.tolist() for i in j):
                break
            else:
                range += 15

        self.logger.info(f"Range for calculating MU_sigma :: {range}, Not more than 740")
        self.logger.info(f"Calculating number of observation for each season and dow")
        season_week_obs.loc["Tot"] = season_week_obs.sum()
        season_week_obs.loc["Avg"] = season_week_obs.loc["Tot"] / season.__len__()
        season_week_obs["Tot"] = season_week_obs.sum(axis=1)
        season_week_obs["Avg"] = season_week_obs["Tot"] / dow.__len__()

        self.logger.info(f"Calculating median adr for each season and dow")
        median = df_1["Rate"].apply("quantile")
        season_week_adr_median = self.ops.values_df(df_1,"Rate",dow,season,"Season","DOW","quantile")
        season_week_adr_median_wdwe = self.ops.values_df(df_1,"Rate",wdwe,season,"Season","WDWE","quantile")
        season_week_adr_median["WD"] = season_week_adr_median_wdwe["WD"]
        season_week_adr_median["WE"] = season_week_adr_median_wdwe["WE"]
        season_adr_median = self.overall_adr(df_1,"Rate",season,"Season","quantile")
        season_adr_median = self.mu_overall_adr_iferror(season_adr_median, median)
        week_adr_median = self.overall_adr(df_2,"Rate",dow,"DOW","quantile")
        week_adr_median_wdwe = self.overall_adr(df_2, "Rate", wdwe, "WDWE", "quantile")
        week_adr_median.extend(week_adr_median_wdwe)
        # median = df_1["Rate"].apply("quantile")
        adr_median = self.club_df(season_week_adr_median,week_adr_median,season_adr_median,median)
        for j in dow + wdwe:
            adr_median[j] = adr_median[j].fillna(adr_median[j]["Avg_Dow"])

        self.logger.info(f"Calculating avg adr for each season and dow")
        avg = df_1["Rate"].apply("mean")
        season_week_adr_avg = self.ops.values_df(df_1, "Rate", dow, season,"Season","DOW","mean")
        season_week_adr_avg_wdwe = self.ops.values_df(df_1, "Rate", wdwe, season,"Season","WDWE","mean")
        season_week_adr_avg["WD"] = season_week_adr_avg_wdwe["WD"]
        season_week_adr_avg["WE"] = season_week_adr_avg_wdwe["WE"]
        season_adr_avg = self.overall_adr(df_1, "Rate", season, "Season", "mean")
        season_adr_avg = self.mu_overall_adr_iferror(season_adr_avg, avg)
        week_adr_avg = self.overall_adr(df_1, "Rate", dow, "DOW", "mean")
        week_adr_avg_wdwe = self.overall_adr(df_1, "Rate", wdwe, "WDWE", "mean")
        week_adr_avg.extend(week_adr_avg_wdwe)
        # avg = df_1["Rate"].apply("mean")
        adr_avg = self.club_df(season_week_adr_avg,week_adr_avg,season_adr_avg,avg)
        for j in dow + wdwe:
            adr_avg[j] = adr_avg[j].fillna(adr_avg[j]["Avg_Dow"])

        return season_week_obs, adr_median, adr_avg,range

    def min_max(self,inf,value_1,value_2):
        """
        This method calculates min or max values based on inflation factor
        :param inf: inflation factor that was received from Inflation Class.
        :param value_1: data value that is to be compared
        :param value_2: data value that is to be compared with
        :return: min values if inflation is less than 1, or max value between the two
        """
        if inf < 1:
            adr = min(value_1,value_2)
        else:
            adr = max(value_1,value_2)

        return adr

class Pace_Dow():
    """
    Pace_Dow class contains functions that helps in calculating adr wrt pace for dow.
    """
    def __init__(self):
        self.ops = Operations()
        self.mu_sigma = Mu_Sigma()
        self.logger = logging.getLogger(f'Calbrservicelog.{__name__}')
        pass

    def dow_count(self,df,dow,wdwe, date_today):
        """
        This method calculates adjusted adr values for each pace and dow+wdwe
        :param df: dataframe after getting the outliers, which helps in calculating adr for non-outlier observations
        :param dow: list having days of the week
        :param wdwe: list saying wd or we. unique list extracted from WDWE column
        :param date_today: Run Date
        :return: returns dataframe with adjusted adr values
        """
        range = 90

        while range <= 360:
            date_past = self.ops.date_ops(date_today, range, "past")
            df_1 = self.ops.filter_df(df,date_past,date_today)
            dow_count = pd.DataFrame()
            dow_count = pd.DataFrame(dow_count, columns=dow+wdwe,index=[0])
            for j in dow:
                dow_count[j][0] = df_1["Outlier_dow"][df_1["DOW"]==j].count()

            for z in wdwe:
                dow_count[z][0] = df_1["Outlier_dow"][df_1["WDWE"] == z].count()

            if all(i >= 30 for i in dow_count.values.tolist()[0]):
                break
            else:
                range += 15

        self.logger.info(f"Range for calculating Pace :: {range}, Not more than 360")
        dow_count["Tot"] = dow_count.sum(axis=1)
        pace = np.arange(0,91,1)
        pace_dow = self.ops.values_df(df_1,"Rate",dow,pace,"Pace","DOW","mean")
        pace_dow = pace_dow.fillna(method='ffill')

        pace_wdwe = self.ops.values_df(df_1,"Rate",wdwe,pace,"Pace","WDWE","mean")
        pace_wdwe = pace_wdwe.fillna(method='ffill')

        pace_tot = []
        for i in pace:
            x = df_1["Rate"][df_1["Pace"] == i].mean()
            pace_tot.append(x)
        pace_tot = pd.Series(pace_tot)
        pace_tot = pace_tot.fillna(method='ffill').to_list()

        pace_dow["WD"] = pace_wdwe["WD"]
        pace_dow["WE"] = pace_wdwe["WE"]
        pace_dow["Total"] = pace_tot

        from collections import Counter
        dow_wdwe = {k: v for k, v in zip(df["DOW"], df["WDWE"])}

        pace_dow = pd.DataFrame(pace_dow)
        for i in pace:
            for j in dow:
                pace_dow[j][i] = np.where(
                    dow_count[j][0] < 30,
                    np.where(
                        dow_count["Tot"][0] < (24 * Counter(dow_wdwe.values())[dow_wdwe[j]]),
                        pace_dow["Total"][i],
                        pace_dow[dow_wdwe.get(j)][i]
                    ), pace_dow[j][i]
                )


        list = ["0th","90th","min","max","Adj0","Adj90"]
        stats_df = pd.DataFrame()
        stats_df = pd.DataFrame(stats_df, columns=dow+wdwe+["Total"], index=list)

        for j in dow+wdwe+["Total"]:
            stats_df[j]["0th"] = pace_dow[j][0]
            stats_df[j]["90th"] = pace_dow[j][90]
            stats_df[j]["min"] = pace_dow[j].min()
            stats_df[j]["max"] = pace_dow[j].max()
            stats_df[j]["Adj0"] = max(stats_df[j]["0th"],stats_df[j]["max"])
            stats_df[j]["Adj90"] = min(stats_df[j]["90th"],stats_df[j]["min"])


        adjusted_adr_df = pd.DataFrame()
        adjusted_adr_df = pd.DataFrame(adjusted_adr_df, columns=dow+wdwe+["Total"], index=np.arange(0,91,1))

        for j in dow+wdwe+["Total"]:
            adjusted_adr_df[j][0] = stats_df[j]["Adj0"] #change and add total
            for i in pace:
                adjusted_adr_df[j][i] = stats_df[j]['Adj0'] + ((stats_df[j]['Adj90'] - stats_df[j]['Adj0'])/90 * i)

        return dow_count, pace_dow, stats_df, adjusted_adr_df, range