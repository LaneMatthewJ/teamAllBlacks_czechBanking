import pandas as pd
import sqlite3

def accChangeToDemo(filepath):
    """
        Creates column to be added to demographic table. Column is taken from the 
        summed difference of the median account balances by district (1994-1998)
        relative to the median account value in all data in 1994.
    Args:
        filepath (str): path to financial.db.
    """
    conn = sqlite3.connect(filepath)
    accdist = pd.read_sql_query("SELECT account_id, district_id from account", conn)
    df = pd.read_sql_query("SELECT date as 'y-m-d', balance, account_id from trans ", conn)
    from datetime import date
    df['y-m-d'] = pd.to_datetime(df['y-m-d'])
    df['year'] = df['y-m-d'].map(lambda x: x.strftime('%Y'))
    df = pd.merge(accdist, df, on='account_id')
    
    # Gets the average value of each account per id per district per year
    acctMedianPerAcctPerYearPerDistrict = df.groupby(['year', 'district_id', 'account_id'])[['balance']].median()

    # Calculates median/average account balance of each district per year
    medianAcct = acctMedianPerAcctPerYearPerDistrict.groupby(['year', 'district_id'])[['balance']].median()
    avgAcct = acctMedianPerAcctPerYearPerDistrict.groupby(['year', 'district_id'])[['balance']].mean()
    
    
    # Gets difference of each account's median per year
#     districtchange = medianAcct.groupby(['district_id'])[['balance']].diff().fillna(0)
    
    medianChange93to98 = (medianAcct.loc['1998'] - medianAcct.loc['1993']) / medianAcct.loc['1993']
    meanChange93to98 = (medianAcct.loc['1998'] - medianAcct.loc['1993']) / medianAcct.loc['1993']
    acctMedianPerAcctPerYearPerDistrictChange = (acctMedianPerAcctPerYearPerDistrict.loc['1998']- acctMedianPerAcctPerYearPerDistrict.loc['1996'])/ acctMedianPerAcctPerYearPerDistrict.loc['1996']

    return (acctMedianPerAcctPerYearPerDistrict.reset_index(), medianAcct.reset_index(), avgAcct.reset_index(), medianChange93to98.reset_index(), meanChange93to98.reset_index(), acctMedianPerAcctPerYearPerDistrictChange.reset_index()) 


def categorizeGrowth(growthRate): 
    if growthRate > 0.5: 
        return 0
    elif growthRate >= 0: 
        return 1
    elif growthRate >= -0.5:
        return 2
    else: 
        return 3



def loadAccountMedianMeanDataframes(filePath): 
    (acctMedianPerAcctPerYearPerDistrict, medianPerDistrictDF, averagePerDistrictDF, medianChange93to98, meanChange93to98, acctMedianPerAcctPerYearPerDistrict93to98) = accChangeToDemo(filePath)
    conn = sqlite3.connect(filePath)
    districtDFNoNames = pd.read_sql_query("SELECT * from district", conn)
    districtDFNoNames.at[68,'A12'] = districtDFNoNames.at[68,'A13'] 
    districtDFNoNames.at[68,'A15'] = districtDFNoNames.at[68,'A16']
    
    conn.close()
    districtDF = updateColumnNames(districtDFNoNames)
    districtDF.drop(['districtName', 'region'], axis=1, inplace=True)
    
    dfAccts = districtDF.merge(acctMedianPerAcctPerYearPerDistrict, on=["district_id"])
    medianPerDistAccts = districtDF.merge(medianPerDistrictDF, on=["district_id"])
    meanPerDistAccts =  districtDF.merge(averagePerDistrictDF, on=["district_id"])
    medianPerDistChange93to98 =  districtDF.merge(medianChange93to98, on=["district_id"])
    meanPerDistChange93to98 =  districtDF.merge(meanChange93to98, on=["district_id"])
    dfAcctsChange93to98 =  districtDF.merge(acctMedianPerAcctPerYearPerDistrict93to98, on=["district_id"])


    
    return (dfAccts,medianPerDistAccts,meanPerDistAccts, medianPerDistChange93to98, meanPerDistChange93to98, dfAcctsChange93to98)


def updateColumnNames(df): 
    columnNames = ['district_id','districtName','region', 'numInhabitants', 
                   'municipalitiesLess500','municipalities500to2k','municipalities2kto10k',
                   'municipalitiesGreater10k','numCities','ratioUrbanInhabitants','avgSalary',
                   'unEmployment95','unEmployment96','entrepeneursPer1k','crimesIn95','crimesIn96' ] 
    df.columns = columnNames
        
    return df
