
# coding: utf-8

import json
import pandas as pd 
import numpy as np


try:
    with open("/Users/kaustubh/Downloads/Input_Transactions.txt") as datafile:
        data = json.load(datafile)
    dataframeInput_Transactions = pd.DataFrame(data)
    dataInput_StartOfDay_Positions = pd.read_csv("/Users/kaustubh/Downloads/Input_StartOfDay_Positions.txt") 

    #Apply TransactionQuantity into matching instrument records in the position file
    TransactionType_B = dataframeInput_Transactions['TransactionType'] == "B"

    #dataInput_StartOfDay_Positions
    AcountType_E = dataInput_StartOfDay_Positions['AccountType'] == "E"

    TransactionType_B_E = pd.merge(dataframeInput_Transactions[TransactionType_B], dataInput_StartOfDay_Positions[AcountType_E], on='Instrument',how='right')
    #Quantity=Quantity + TransactionQuantity
    TransactionType_B_E=TransactionType_B_E.fillna(0)

    TransactionType_B_E['Quantity'] = TransactionType_B_E['Quantity'] + TransactionType_B_E['TransactionQuantity']  # assigned to a column

except IOError: 
    print("Sorry ! File cannot open ") 


try:
    
    AcountType_I = dataInput_StartOfDay_Positions['AccountType'] == "I"


    TransactionType_B_I = pd.merge(dataframeInput_Transactions[TransactionType_B], dataInput_StartOfDay_Positions[AcountType_I], on='Instrument')
    #Quantity=Quantity + TransactionQuantity
    TransactionType_B_I.fillna(0)
    
    TransactionType_B_I['Quantity'] = TransactionType_B_I['Quantity'] - TransactionType_B_I['TransactionQuantity']  # assigned to a column

    #Apply TransactionQuantity into matching instrument records in the position file
    TransactionType_S = dataframeInput_Transactions['TransactionType'] == "S"

    #dataInput_StartOfDay_Positions
    AcountType_E = dataInput_StartOfDay_Positions['AccountType'] == "E"


    TransactionType_S_E = pd.merge(dataframeInput_Transactions[TransactionType_S], dataInput_StartOfDay_Positions[AcountType_E], on='Instrument',how='outer')
    #Quantity=Quantity + TransactionQuantity
    TransactionType_S_E = TransactionType_S_E.fillna(0)

    TransactionType_S_E['Quantity'] = TransactionType_S_E['Quantity'] - TransactionType_S_E['TransactionQuantity']  # assigned to a column

    df = pd.DataFrame(TransactionType_S_E)
    grouped= df.groupby(['Instrument','AccountType','Account'])
    #print grouped['Quantity'].agg(np.sum)
    groupedS = grouped['TransactionQuantity'].agg(np.sum)
    #df2_transposed = grouped.T
    pd.DataFrame(data=groupedS)


    AcountType_I = dataInput_StartOfDay_Positions['AccountType'] == "I"

    TransactionType_S_I = pd.merge(dataframeInput_Transactions[TransactionType_S], dataInput_StartOfDay_Positions[AcountType_I], on='Instrument',how='outer')
    #Quantity=Quantity + TransactionQuantity
    TransactionType_S_I=TransactionType_S_I.fillna(0)

    TransactionType_S_I['Quantity'] = TransactionType_S_I['Quantity'] + TransactionType_S_I['TransactionQuantity']  # assigned to a column

    import numpy as np
    dfUnionS = TransactionType_S_E.append(TransactionType_S_I)


    #dfUnionS = dfUnionS.groupby('Instrument')
    #print dfUnionS.groupby('AccountType').groups

    df = pd.DataFrame(dfUnionS)
    grouped= df.groupby(['Instrument','AccountType','Account'])
    #print grouped['Quantity'].agg(np.sum)
    groupedS = grouped['TransactionQuantity'].agg(np.sum)
    #df2_transposed = grouped.T
    pd.DataFrame(data=groupedS)

    dfUnionB = TransactionType_B_E.append(TransactionType_B_I)
    df1 = pd.DataFrame(dfUnionB)
    groupedB1= df1.groupby(['Instrument','AccountType','Account'])
    groupedB= groupedB1['Quantity'].agg(np.sum) #Transaction
    pd.DataFrame(data=groupedB)

    #groupedB.groupby('AccountType').add
    df_grouped1 = groupedB.groupby(["Instrument", "AccountType","Account"]).sum()

    df_grouped2 = groupedS.groupby(["Instrument", "AccountType","Account"]).sum()

    #I
    df_grouped3 = df_grouped2.add(df_grouped1, fill_value=0)

    df_grouped1.sub(df_grouped2, fill_value=0)

    df_grouped31=df_grouped1.sub(df_grouped2, fill_value=0)
    df1 = pd.DataFrame(df_grouped31)
    df_grouped31
    df_grouped31.to_csv('df_grouped31.csv')

    df_grouped32 = df_grouped2.add(df_grouped1, fill_value=0)
    df2 = pd.DataFrame(df_grouped32)
    df_grouped32
    df_grouped32.to_csv('df_grouped32.csv')

    groupedE = pd.read_csv("df_grouped31.csv", header=None) 
    groupedE.columns = ["Instrument", "AccountType", "Account", "Quantity"]
    groupedEE = groupedE['AccountType'] == "E"
    GroupedE = groupedE[groupedEE]

    groupedI = pd.read_csv("df_grouped32.csv", header=None) 
    groupedI.columns = ["Instrument", "AccountType", "Account", "Quantity"]
    groupedII = groupedI['AccountType'] == "I"
    GroupedI = groupedI[groupedII]

    GroupedEI = GroupedE.append(GroupedI)
    sorted_df=GroupedEI.sort_index(axis=0)

    sorted_df21 = dataInput_StartOfDay_Positions.sort_values(by=['Instrument'])
    #sorted_df21=sorted_df21.reset_index
    sorted_df21=sorted_df21.reset_index(drop=True)

    sorted_df['Delta'] =sorted_df['Quantity'] - sorted_df21['Quantity'] 
    sorted_df
    sorted_df.to_csv('Output.csv')
except IOError: 
    print("Sorry ! File cannot open ")  

