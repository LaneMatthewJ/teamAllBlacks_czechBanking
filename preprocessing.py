import numpy as np
import pandas as pd
import sqlite3
import getopt
import sys
from datetime import datetime


def updateFrequency(freq): 
    if freq == 'POPLATEK MESICNE': return 'Monthly'
    if freq == 'POPLATEK TYDNE': return 'Weekly'
    if freq == 'POPLATEK PO OBRATU': return 'AfterTransaction'
    
def updateKSymbol(kSym): 
    if kSym == 'POJISTNE': return 'Insurance'
    if kSym == 'SIPO': return 'Household'
    if kSym == 'LEASING': return 'Leasing'
    if kSym == 'UVER': return 'Loan'
    if kSym == 'SLUZBY': return 'Statement'
    if kSym == 'UROK': return 'Interest Credited'
    if kSym == 'SANKC. UROK': return 'Sanction Interest if Negative Balance'
    if kSym == 'DUCHOD': return 'Old Age Pension'

def updateType(transactionType): 
    if transactionType == 'PRIJEM': return 'Credit'
    if transactionType == 'VYDAJ': return 'Withdrawal'
    
def updateOperation(mode): 
    if mode == 'VYBER KARTOU': return 'Credit Card Withdrawal'
    if mode == 'VKLAD': return 'Credit in Cash'
    if mode == 'PREVOD Z UCTU': return 'Collection from another Bank'
    if mode == 'VYBER': return 'Withdrawal in Cash'
    if mode == 'PREVOD NA UCET': return 'Remittance to Another Bank'

def updateAccountStatus(status): 
    if status == 'A': return 'Finished: No Problems'
    if status == 'B': return 'Finished: Loan Not Payed'
    if status == 'C': return 'Running: OK'
    if status == 'D': return 'Running: In Debt'

def translateDBs(databasePath): 
    conn = sqlite3.connect(databasePath)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    accountDF = pd.read_sql_query("SELECT * FROM account", conn)
    loanDF = pd.read_sql_query("SELECT * FROM loan", conn)
    orderDF = pd.read_sql_query("SELECT * FROM \"order\"", conn)
    transDF = pd.read_sql_query("SELECT * FROM trans", conn)

    englishAccountDF = pd.DataFrame.copy(accountDF)
    englishAccountDF['frequency'] = accountDF['frequency'].apply(lambda x: updateFrequency(x))
    englishAccountDF.to_sql('accountEnglish', con=conn, if_exists='replace', index=False)


    englishOrderDF =  pd.DataFrame.copy(orderDF)
    englishOrderDF['k_symbol'] = orderDF['k_symbol'].apply(lambda x: updateKSymbol(x))
    englishOrderDF.to_sql('orderEnglish', con=conn,
                          if_exists='replace', index=False)
    
    englishTransDF = pd.DataFrame.copy(transDF)
    englishTransDF['type'] =  transDF['type'].apply(lambda x: updateType(x))
    englishTransDF['operation'] =  transDF['operation'].apply(lambda x: updateOperation(x))
    englishTransDF['k_symbol'] =  transDF['k_symbol'].apply(lambda x: updateOperation(x))
    englishTransDF.to_sql('transEnglish', con=conn,
                          if_exists='replace', index=False)

    englishLoanDF  =  pd.DataFrame.copy(loanDF)
    englishLoanDF['status'] = loanDF['status'].apply(lambda x: updateAccountStatus(x))
    englishLoanDF.to_sql('statusEnglish', con=conn,
                         if_exists='replace', index=False)


    conn.close()

    
# def loadAllData(path, encodeData=False): 
#     conn = sqlite3.connect(databasePath)
#     cursor = conn.cursor()
#     cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
#     accountDF = pd.read_sql_query("SELECT * FROM account", conn)
# #     cardDF = pd.read_sql_query("SELECT * FROM card", conn)
#     clientDF = pd.read_sql_query("SELECT * FROM client", conn)
#     dispositionDF = pd.read_sql_query("SELECT * FROM disp", conn)
#     districtDF = pd.read_sql_query("SELECT * FROM district", conn)
# #     loanDF = pd.read_sql_query("SELECT * FROM loan", conn)
#     orderDF = pd.read_sql_query("SELECT * FROM \"order\"", conn)
#     transDF = pd.read_sql_query("SELECT * FROM trans", conn)
    
#     transDF['type'] =  transDF['type'].apply(lambda x: updateType(x))
#     transDF['operation'] =  transDF['operation'].apply(lambda x: updateOperation(x))
#     transDF['k_symbol'] =  transDF['k_symbol'].apply(lambda x: updateOperation(x))
#     accountDF['frequency'] = accountDF['frequency'].apply(lambda x: updateFrequency(x))
#     orderDF['k_symbol'] = orderDF['k_symbol'].apply(lambda x: updateKSymbol(x))

#     transDF['type'] =  transDF['type'].apply(lambda x: updateType(x))
#     transDF['operation'] =  transDF['operation'].apply(lambda x: updateOperation(x))
#     transDF['k_symbol'] =  transDF['k_symbol'].apply(lambda x: updateOperation(x))
#     transDF['status'] = loanDF['status'].apply(lambda x: updateAccountStatus(x))


   
def loadAllData(path, encodeData=False): 
    conn = sqlite3.connect(path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    accountDF = pd.read_sql_query("SELECT * FROM account", conn)
    cardDF = pd.read_sql_query("SELECT * FROM card", conn)
    clientDF = pd.read_sql_query("SELECT * FROM client", conn)
    dispositionDF = pd.read_sql_query("SELECT * FROM disp", conn)
    districtDF = pd.read_sql_query("SELECT * FROM district", conn)
    loanDF = pd.read_sql_query("SELECT * FROM loan", conn)
    orderDF = pd.read_sql_query("SELECT * FROM \"order\"", conn)
    transDF = pd.read_sql_query("SELECT * FROM trans", conn)
    
    districtDF.at[68,'A12'] = districtDF.at[68,'A13'] 
    districtDF.at[68,'A15'] = districtDF.at[68,'A16']
    

    englishAccountDF = pd.DataFrame.copy(accountDF)
    englishAccountDF['frequency'] = accountDF['frequency'].apply(lambda x: updateFrequency(x))

    englishOrderDF =  pd.DataFrame.copy(orderDF)
    englishOrderDF['k_symbol'] = orderDF['k_symbol'].apply(lambda x: updateKSymbol(x))

    englishTransDF = pd.DataFrame.copy(transDF)
    englishTransDF['type'] =  transDF['type'].apply(lambda x: updateType(x))
    englishTransDF['operation'] =  transDF['operation'].apply(lambda x: updateOperation(x))
    englishTransDF['k_symbol'] =  transDF['k_symbol'].apply(lambda x: updateOperation(x))

    englishLoanDF  =  pd.DataFrame.copy(loanDF)
    englishLoanDF['status'] = loanDF['status'].apply(lambda x: updateAccountStatus(x))

    if encodeData: 
        englishAccountDF = encodeAccountDF(englishAccountDF)
        clientDF = encodeClient(clientDF)
        dispositionDF = encodeDisposition(dispositionDF)
        districtDF = encodeDistrict(districtDF)
        englishLoanDF = encodeLoanData(englishLoanDF)
        cardDF = encodeCard(cardDF)


    acct_disposition_client = englishAccountDF.merge(dispositionDF, on='account_id', how='inner').merge(clientDF, on='client_id', how='inner')  
    acct_disposition_client_loanDF = acct_disposition_client.merge(englishLoanDF, on='account_id', how='left')
    acct_disposition_client_loanDF.rename(columns={'district_id_x': 'district_id'}, inplace=True)
    acct_disp_client_loan_districtDF = acct_disposition_client_loanDF.merge(districtDF, on='district_id', how='inner')
    acct_disposition_client_cardDF = acct_disp_client_loan_districtDF.merge(cardDF, on='disp_id', how='left')

    return acct_disposition_client_cardDF
    



def extractXYValues(df, yVectorName):
    notNanDF = df[ df[yVectorName].notna() ]
    yValues = notNanDF[yVectorName]
    xValues = notNanDF.loc[:, notNanDF.columns != yVectorName] 

    one_hot_Xdata = pd.get_dummies(xValues)

    return (one_hot_Xdata, yValues)

def getTrainTestSplit(xValues, yValues): 
    X_train, X_test, y_train, y_test = train_test_split(xValues, yValues, test_size=0.33, random_state=42)

    return (X_train, X_test, y_train, y_test)



def encodeAccountDF(df):
    monthDict = {'Monthly': 0, 'Weekly': 1, 'AfterTransaction': 2}
    encodedDF = df.copy()
    encodedDF['frequency'] = encodedDF['frequency'].apply(
        lambda x: monthDict[x])
    encodedDF['date'] = encodedDF['date'].apply(
        lambda x: datetime.fromisoformat(x).timestamp())

    return encodedDF


def decodeAccountDF(df):
    month = ['Monthly', 'Weekly', 'AfterTransaction']

    decodedDF = df.copy()
    decodedDF['frequency'] = decodedDF['frequency'].apply(lambda x: month[x])
    decodedDF['date'] = decodedDF['date'].apply(
        lambda x: datetime.fromtimestamp(x).isoformat().split('T')[0])

    return decodedDF


def encodeCard(df):
    typeDict = {'gold': 0, 'classic': 1, 'junior': 2}
    encodedDF = df.copy()
    encodedDF['type'] = encodedDF['type'].apply(lambda x: typeDict[x])
    encodedDF['issued'] = encodedDF['issued'].apply(
        lambda x: datetime.fromisoformat(x).timestamp())

    return encodedDF


def decodeCard(df):
    typeArray = ['gold', 'classic', 'junior']
    decodedDF = df.copy()
    decodedDF['type'] = decodedDF['type'].apply(lambda x: typeArray[x])
    decodedDF['issued'] = decodedDF['issued'].apply(
        lambda x: datetime.fromtimestamp(x).isoformat().split('T')[0])

    return decodedDF


def encodeClient(df):
    genderDict = {'F': 0, 'M': 1}
    encodedDF = df.copy()
    encodedDF['gender'] = encodedDF['gender'].apply(lambda x: genderDict[x])
    encodedDF['birth_date'] = encodedDF['birth_date'].apply(
        lambda x: datetime.fromisoformat(x).timestamp())

    return encodedDF


def decodeClient(df):
    genderray = ['F', 'M']
    decodedDF = df.copy()
    decodedDF['gender'] = decodedDF['gender'].apply(lambda x: genderray[x])
    decodedDF['birth_date'] = decodedDF['birth_date'].apply(
        lambda x: datetime.fromtimestamp(x).isoformat().split('T')[0])

    return decodedDF


def encodeDisposition(df):
    dispType = {'OWNER': 0, 'DISPONENT': 1}
    encodedDF = df.copy()
    encodedDF['type'] = encodedDF['type'].apply(lambda x: dispType[x])
    return encodedDF


def decodeDisposition(df):
    dispArray = ['OWNER', 'DISPONENT']
    decodedDF = df.copy()

    decodedDF['type'] = decodedDF['type'].apply(lambda x: dispArray[x])

    return decodedDF


def encodeDistrict(df):
    region = {'Prague': 0, 'central Bohemia': 1, 'south Bohemia': 2, 'west Bohemia': 3,
              'north Bohemia': 4, 'east Bohemia': 5, 'south Moravia': 6, 'north Moravia': 7}
    encodedDF = df.copy()
    encodedDF['A3'] = encodedDF['A3'].apply(lambda x: region[x])
    encodedDF.drop(['A2'], axis=1, inplace=True)
    return encodedDF


def decodeDistrict(df):
    regionArray = ['Prague', 'central Bohemia', 'south Bohemia', 'west Bohemia',
                   'north Bohemia', 'east Bohemia', 'south Moravia', 'north Moravia']
    decodedDF = df.copy()
    decodedDF['A3'] = decodedDF['A3'].apply(lambda x: regionArray[x])
    return decodedDF


def encodeLoanData(df):
    statuses = {'Finished: No Problems': 0, 'Finished: Loan Not Payed': 1,
                'Running: In Debt': 2, 'Running: OK': 3}
    encodedDF = df.copy()
    encodedDF['status'] = encodedDF['status'].apply(lambda x: statuses[x])
    encodedDF['date'] = encodedDF['date'].apply(
        lambda x: datetime.fromisoformat(x).timestamp())

    return encodedDF


def decodeLoanData(df):
    statusArray = ['Finished: No Problems', 'Finished: Loan Not Payed',
                   'Running: In Debt', 'Running: OK']
    decodedDF = df.copy()
    decodedDF['status'] = decodedDF['status'].apply(lambda x: statusArray[x])
    decodedDF['date'] = decodedDF['date'].apply(
        lambda x: datetime.fromtimestamp(x).isoformat().split('T')[0])
    return decodedDF


def updateKsymbol(symbol):
    if symbol == "Household":
        return 0
    elif symbol == "Loan":
        return 1
    elif symbol == "Insurance":
        return 2
    elif symbol == "Leasing":
        return 3

    return 4


def encodeOrder(df):
    banksTo = {'YZ': 0, 'ST': 1, 'QR': 2, 'WX': 3, 'CD': 4, 'AB': 5, 'UV': 6, 'GH': 7, 'IJ': 8, 'KL': 9, 'EF': 10,
               'MN': 11, 'OP': 12}

    encodedDF = df.copy()
    encodedDF['bank_to'] = encodedDF['bank_to'].apply(lambda x: banksTo[x])
    encodedDF['k_symbol'] = encodedDF['k_symbol'].apply(updateKsymbol)

    return encodedDF


def decodeOrder(df):
    banksFrom = ['YZ', 'ST', 'QR', 'WX', 'CD', 'AB', 'UV', 'GH', 'IJ', 'KL', 'EF',
                 'MN', 'OP']
    kSymbol = ['Household', 'Loan', 'Insurance', 'Leasing', 'None']
    decodedDF = df.copy()

    decodedDF['bank_to'] = decodedDF['bank_to'].apply(lambda x: banksFrom[x])
    decodedDF['k_symbol'] = decodedDF['k_symbol'].apply(lambda x: kSymbol[x])

    return decodedDF


def encodeType(transType):
    if transType == 'Credit':
        return 0
    elif transType == 'Withdrawal':
        return 1
    else:
        return 2


def encodeOperation(operation):
    if operation == 'Credit in Cash':
        return 0
    elif operation == 'Withdrawal':
        return 1
    elif operation == 'Collection from another Bank':
        return 2
    elif operation == 'Remittance to Another Bank':
        return 3
    elif operation == 'Withdrawal in Cash':
        return 4
    elif operation == 'Credit Card Withdrawal':
        return 5
    else:
        return 6


def encodeBank(bank):
  if bank == 'AB':
    return 0
  elif bank == 'YZ':
    return 1
  elif bank == 'ST':
    return 2
  elif bank == 'QR':
    return 3
  elif bank == 'WX':
    return 4
  elif bank == 'CD':
    return 5
  elif bank == 'UV':
    return 6
  elif bank == 'KL':
    return 7
  elif bank == 'GH':
    return 8
  elif bank == 'OP':
    return 9
  elif bank == 'IJ':
    return 10
  elif bank == 'EF':
    return 11
  elif bank == 'MN':
    return 12

  return 13


def encodeTrans(df):
    encodedTrans = df.copy()
    encodedTrans['type'] = encodedTrans['type'].apply(encodeType)
    encodedTrans['operation'] = encodedTrans['operation'].apply(
        encodeOperation)
    encodedTrans['bank'] = encodedTrans['bank'].apply(encodeBank)
    encodedTrans['date'] = encodedTrans['date'].apply(
        lambda x: datetime.fromisoformat(x).timestamp())
    return encodedTrans


def decodeTrans(df):
    typeArray = ['Credit', 'Withdrawal', 'None']
    operationArray = ['Credit in Cash', 'Withdrawal', 'Collection from another Bank',
                      'Remittance to Another Bank', 'Withdrawal in Cash', 'Credit Card Withdrawal', 'None']
    bankArray = ['AB', 'YZ', 'ST', 'QR', 'WX', 'CD',
                 'UV', 'KL', 'GH', 'OP', 'IJ', 'EF', 'MN', 'None']

    decodedDF = df.copy()
    decodedDF['type'] = decodedDF['type'].apply(lambda x: typeArray[x])
    decodedDF['operation'] = decodedDF['operation'].apply(
        lambda x: operationArray[x])
    decodedDF['bank'] = decodedDF['bank'].apply(lambda x: bankArray[x])
    decodedDF['date'] = decodedDF['date'].apply(
        lambda x: datetime.fromtimestamp(x).isoformat().split('T')[0])

    return decodedDF


def usage(): 
    print(""" 
    
        Functions: 
            - translateDBs(filePath): 
                    Takes in file path as string and translates 
                    czech financial databases data to english under
                    the names: 

                            accountEnglish
                            orderEnglish
                            transEnglish
                            statusEnglish
    
            - Encoding: 
                Each of the below functions takes its associated dataframe and encodes it to numerical values.
                    encodeAccountDF
                    encodeCard
                    encodeClient
                    encodeDisposition
                    encodeDistrict
                    encodeLoanData
                    encodeOrder
                    encodeTrans

            - Decoding: 
                Each of the below functions takes an encoded dataframe and returns a decoded copy.
                    decodeAccountDF
                    decodeCard
                    decodeClient
                    decodeDisposition
                    decodeDistrict
                    decodeLoanData
                    decodeOrder
                    decodeTrans
    """)
def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:],  "h", ["help"])
    except getopt.GetoptError as err:
        # print help information and exit:
        print(str(err))  # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

    output = None
    verbose = False
    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        else:
            assert(False, "unhandled option")


if __name__ == "__main__":
    main()
