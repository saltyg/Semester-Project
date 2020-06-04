from random import random
import numpy as np
import scipy.stats as st
import pyspark.sql.functions as f
from pyspark.sql.functions import col


def magic_K_search(dfAgg, aggColumn, z, error_bound):
    minStrataSize = 2.0
    maxStrataSize = dfAgg.agg(f.max("count(" + aggColumn + ")")).first()[0]
    
    magicK = 0.0; sampleSize = 0.0
    
    # use bisection to find the best (minimum) K
    while (minStrataSize + 1 < maxStrataSize):
        mid = (minStrataSize + maxStrataSize) // 2
        print("maxStrataSize: ", maxStrataSize, "minStrataSize: ", minStrataSize, "midStrataSize: ", mid)

        v_by_n = dfAgg.rdd.map(lambda row: cal_v_by_n(row, mid)).sum()
        estimatedError = np.sqrt(v_by_n) * z
        print("Estimated error with bisection: ", estimatedError, "User defined error bound", error_bound)
        if estimatedError <= error_bound:
            maxStrataSize = mid
        else:
            minStrataSize = mid
      
    if np.sqrt(dfAgg.rdd.map(lambda row: cal_v_by_n(row, minStrataSize)).sum()) * z <= error_bound:
        magicK = minStrataSize
    elif np.sqrt(dfAgg.rdd.map(lambda row: cal_v_by_n(row, minStrataSize)).sum()) * z <= error_bound:
        magicK = maxStrataSize
    else:
    # the required K can't be found
        print("The required K can't be found")
    return magicK

def cal_v_by_n(row, K):
    stratumSampleSize = row[0] if row[0] < K else K
    error = np.power(row[0], 2) * row[1] / stratumSampleSize
    return error


def ScaSRS(stra_block, K): 
    sigma = 0.00005
    stratumSize, stratum = len(stra_block[1]), stra_block[1]

    if stratumSize <= K:
        return stratum
    
    p = K / stratumSize
    gamma1 = -1.0 * (np.log(sigma) / stratumSize)
    gamma2 = -1.0 * (2 * np.log(sigma) / 3 / stratumSize)
    q1 = min(1, p + gamma1 + np.sqrt(gamma1 * gamma1 + 2 * gamma1 * p))
    q2 = max(0, p + gamma2 - np.sqrt(gamma2 * gamma2 + 3 * gamma2 * p))

    l = 0
    waitlist, res = [], []

    for nextRow in stratum:
        Xj = random()
        if Xj < q2:
            res.append(nextRow)
            l += 1
        elif Xj < q1:
            waitlist.append((Xj, nextRow))

    # select the smallest pn-l items from waitlist
    waitlist.sort()
    if int(K - l) > 0:
        res += [item[1] for item in waitlist[:int(K-l)]]
    return res


def generate_stratified_sample(df, QCS, e, ci):
    z = st.norm.ppf(ci)
    aggColumn = "l_extendedprice"
    error_bound = float(df.agg(f.sum(aggColumn)).first()[0])
    dfAgg = df.groupBy(QCS).agg(f.count(aggColumn), f.var_pop(aggColumn))\
            .select("count(" + aggColumn + ")", "var_pop(" + aggColumn + ")")
    magicK = magic_K_search(dfAgg, aggColumn, z, error_bound)
    stratifiedSample = df.rdd.map(lambda row: ('_'.join([str(row[each]) for each in QCS]), row))\
                        .groupByKey().flatMap(lambda stra_block: ScaSRS(stra_block, magicK))
    sampleDF = stratifiedSample.toDF()
    return sampleDF