import yfinance as yf
from datetime import datetime
import pandas as pd
import numpy as np
import dateutil.relativedelta as relativedelta


def generate_returns(dfRaw: pd.DataFrame) -> pd.DataFrame:
    """generate_returns _summary_

    _extended_summary_

    Args:
        dfRaw (pd.DataFrame): Dataframe downloaded from YahooFinace API (yfinance library)
        
    Returns:
        pd.DataFrame: Dataframe which contains the returns using close price for each ticket on dfRaw.

        Example:

                ticket	    QLD	        TQQQ
        date_hour		
        2022-11-23 15:00:00	0.019814	0.029905
        2022-11-25 15:00:00	-0.013672	-0.020677
        2022-11-28 15:00:00	-0.029669	-0.044025
        2022-11-29 15:00:00	-0.014787	-0.023966
        ...
    """    

    dfRaw = dfRaw.copy()
    dfRaw = dfRaw[["Open", "Close"]].unstack().to_frame()
    dfRaw = dfRaw.reset_index().rename(columns = {0: "price", "level_0": "type", "level_1": "ticket", "Date": "date"})
    dfRaw

    dateHoursList = []
    for row in range(len(dfRaw)):
        if dfRaw.loc[row, "type"] == "Open":
            dateHoursList.append(dfRaw.loc[row, "date"] + relativedelta.relativedelta(hours = 8))
        if dfRaw.loc[row, "type"] == "Close":
            dateHoursList.append(dfRaw.loc[row, "date"] + relativedelta.relativedelta(hours = 15))    

    dfRaw["date_hour"] = dateHoursList

    dfClean = dfRaw.set_index(["date_hour"])[["ticket", "price", "type"]]
    dfClean

    dfCleanPivot = dfClean.pivot_table(index = "date_hour", columns = "ticket", values = "price")
    dfCleanPivot


    dfCleanTemp = dfClean.loc[dfClean["type"] == "Close"].pivot_table(index = "date_hour", columns = "ticket", values = "price")
    dfReturns = (dfCleanTemp / dfCleanTemp.shift(1) - 1).dropna()
    dfReturns.head()

    return dfReturns



def running_moving_average(array: np.array, steps: int) -> np.array:

    """running_moving_average

    This function generates the moving average over a numpy array, over n periods

    Args:
        array (np.array): array that will be process
        steps (int): number of steps

    Returns:
        mov_avg (np.array): average array
    """    

    mov_avg = np.full_like(array, np.nan)
    mov_avg[steps] = array[1:steps+1].mean()
    for i in range(steps+1, len(array)):
        mov_avg[i] = (mov_avg[i-1] * (steps - 1) + array[i]) / steps

    return mov_avg



def generate_rsi_diff(dfReturns: pd.DataFrame, window: int = 14) -> pd.DataFrame:

    """ generate_rsi_diff

    This function will generate the RSI indicator per index of dfReturns dataframe 
    and will calculate the rsi_diff indicator as RSA_{QLD} - RSI_{TQQQ}.

    By defaulf, will use 14 days as rolling window and 

    Args:
        dfReturns (pd.DataFrame): Pandas dataframe with returns

    Returns:
        dfRSI (pd.DataFrame): Pandas dataframe with RSI and rsi_diff indictator

        Example:

                ticket	QLD	TQQQ	gain_QLD	loss_QLD	gain_TQQQ	loss_TQQQ	avg_gain_QLD	avg_loss_QLD	avg_gain_TQQQ	avg_loss_TQQQ	rs_QLD	rsi_QLD	rs_TQQQ	rsi_TQQQ	rsi_QLD_TQQQ
        date_hour															
        2023-02-14 15:00:00	0.013514	0.021643	0.013514	0.000000	0.021643	0.000000	0.017221	0.010572	0.025550	0.015638	1.628959	61.962135	1.633850	62.032771	-0.070636
        2023-02-15 15:00:00	0.016175	0.022362	0.016175	0.000000	0.022362	0.000000	0.017147	0.009817	0.025322	0.014521	1.746650	63.592012	1.743848	63.554832	0.037181
        ...
    """   

    dfRSI = dfReturns.copy()
    dfRSI['gain_QLD'] = dfRSI["QLD"].mask(dfRSI["QLD"] < 0, 0.0)
    dfRSI['loss_QLD'] = -dfRSI["QLD"].mask(dfRSI["QLD"] > 0, -0.0)

    dfRSI['gain_TQQQ'] = dfRSI["TQQQ"].mask(dfRSI["TQQQ"] < 0, 0.0)
    dfRSI['loss_TQQQ'] = -dfRSI["TQQQ"].mask(dfRSI["TQQQ"] > 0, -0.0)

    dfRSI['avg_gain_QLD'] = running_moving_average(dfRSI['gain_QLD'].to_numpy(), window)
    dfRSI['avg_loss_QLD'] = running_moving_average(dfRSI['loss_QLD'].to_numpy(), window)

    dfRSI['avg_gain_TQQQ'] = running_moving_average(dfRSI['gain_TQQQ'].to_numpy(), window)
    dfRSI['avg_loss_TQQQ'] = running_moving_average(dfRSI['loss_TQQQ'].to_numpy(), window)

    dfRSI['rs_QLD'] = dfRSI['avg_gain_QLD'] / dfRSI['avg_loss_QLD']
    dfRSI['rsi_QLD'] = 100 - (100 / (1 + dfRSI['rs_QLD']))

    dfRSI['rs_TQQQ'] = dfRSI['avg_gain_TQQQ'] / dfRSI['avg_loss_TQQQ']
    dfRSI['rsi_TQQQ'] = 100 - (100 / (1 + dfRSI['rs_TQQQ']))
    dfRSI["rsi_QLD_TQQQ"] = dfRSI['rsi_QLD'] - dfRSI['rsi_TQQQ']
    
    return dfRSI


def generate_purchases(dfRSI: pd.DataFrame, initValQLD: float, opt_size: float = 0.002) -> pd.DataFrame:

    """generate_purchases

    This function the purchases dataframe per date, given certain initial value of QLD investment and some
    parameter to size the size of the purchase (buy if it is a positive purchase and sell if it is negative
    purchase).

    Args:
        dfRSI (pd.DataFrame): Pandas dataframe with rsi_diff indicator
        initValQLD (float): Initial investment value of QLD in US dollars.
        opt_size (float): Parameter that defines the potencial size of each transaction. By default is equal
        to 20bps = 0.002

    Returns:
        opt_size (np.array): average array
    """    

    dfPurchases = pd.DataFrame(columns=["value_QLD"])
    dfPurchases["value_QLD"] = (dfRSI["rsi_QLD_TQQQ"] * opt_size * abs(initValQLD)).fillna(0)

    return dfPurchases


def generate_value_over_time(dfReturns: pd.DataFrame, dfPurchases: pd.DataFrame, initValTQQQ: float, initValQLD: float):

    dfReturns = dfReturns.copy()
    dfCompRet = (dfReturns + 1).cumprod() - 1

    dfValueRSI = dfCompRet.copy()
    dfValueRSI["value_TQQQ"] = initValTQQQ * (dfCompRet["TQQQ"] + 1)
    listValQLD_RSI = [initValQLD]

    for i, date in enumerate(dfReturns.index):
        listValQLD_RSI.append(listValQLD_RSI[-1] * (1 + dfReturns.loc[date, "QLD"]) + dfPurchases.loc[date, "value_QLD"])

    listValQLD_RSI.pop(0)
        
    dfValueRSI["value_QLD"] = listValQLD_RSI
    dfValueRSI["value_port"] = dfValueRSI["value_TQQQ"] + dfValueRSI["value_QLD"]

    return dfValueRSI



def generate_cum_ret(dfValueRSI: pd.DataFrame):

    dfReturnsRSI = dfValueRSI[["value_port"]].pct_change().dropna()
    dfCompRetRSI = ((dfReturnsRSI + 1).cumprod() - 1) * 100

    return dfCompRetRSI