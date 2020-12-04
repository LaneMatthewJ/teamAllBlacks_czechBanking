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
    
    ## Check db if english translted tables exist, if so - then exit
    
    
    
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
