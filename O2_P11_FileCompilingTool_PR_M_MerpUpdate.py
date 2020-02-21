# -*- coding: utf-8 -*-
"""
Created on Mon Feb  3 10:00:45 2020

@author: asheikh
"""
#Load packages
import pandas as pd
import numpy as np
import os

#Load essential files
MainSheetSummary =pd.read_csv(
    "//192.168.2.32/Group/Data Team/Restricted_Data_Sources/3_Reference_Folder/MainSheetSummary.csv"
    ,encoding='utf-8')

internalsku_Ref =pd.read_csv(
    "//192.168.2.32/Group/Data Team/Restricted_Data_Sources/3_Reference_Folder/internalsku_Ref.csv"
    ,encoding='utf-8')

NonMatchingSkusList =pd.read_csv(
    "//192.168.2.32/Group/Data Team/Restricted_Data_Sources/2_Output_Folder/NonMatchingSkusList.csv"
    ,encoding='utf-8')

NonMatchingSkusData =pd.read_csv(
    "//192.168.2.32/Group/Data Team/Restricted_Data_Sources/2_Output_Folder/NonMatchingSkusData.csv"
    ,encoding='utf-8')

NewSalesData =pd.read_csv(
    "//192.168.2.32/Group/Data Team/Restricted_Data_Sources/1_Input_Folder/NewSalesData.csv"
    ,encoding='utf-8')

#Identify mismatching skus & Export them
AllSkuSet = set(MainSheetSummary["internal_sku"])
NewSkuSet = set(NewSalesData["SKU"])
RefSkuSet = set(internalsku_Ref["SKU"])

nonMatchingSkus = list((NewSkuSet - AllSkuSet) - RefSkuSet)
MS_MatchingSkus = list(NewSkuSet.intersection(AllSkuSet))
RS_MatchingSkus = list(NewSkuSet.intersection(RefSkuSet))

#Process non mathing skus
nonMatchdf = NewSalesData[NewSalesData["SKU"].isin(nonMatchingSkus)][["Product Description", "Brands", "SKU"]]
nonMatchlist = pd.DataFrame({"SKU":nonMatchdf["SKU"]})
print("Mismatching Skus = ",len(nonMatchlist))

UniqueMismatchlist = list((set(nonMatchlist["SKU"]) - set(internalsku_Ref["SKU"])))
NonMatchingSkusList = pd.DataFrame({"SKU":list(NonMatchingSkusList["SKU"]) + UniqueMismatchlist})
(NonMatchingSkusList.to_csv(
    "//192.168.2.32/Group/Data Team/Restricted_Data_Sources/2_Output_Folder/NonMatchingSkusList.csv",
    index = None, header=True,  encoding='utf-8'))


UniqueMismatchdf = list(set(nonMatchdf["Order Item Id"]) - set(NonMatchingSkusData["Order Item Id"]))
nonMatchdf = nonMatchdf[nonMatchdf["Order Item Id"].isin(UniqueMismatchdf)]
NonMatchingSkusData = NonMatchingSkusData.append(nonMatchdf, sort=False)
(NonMatchingSkusData.to_csv(
    "//192.168.2.32/Group/Data Team/Restricted_Data_Sources/2_Output_Folder/NonMatchingSkusData.csv",
    index = None, header=True, encoding='utf-8'))

#Process Skus from the internalsku_Ref.csv
MatchingSkudf = NewSalesData[(NewSalesData["SKU"].isin(MS_MatchingSkus))]
MatchingSkudf["internal_sku"] = MatchingSkudf["SKU"]

RefMatchingSkudf = NewSalesData[(NewSalesData["SKU"].isin(RS_MatchingSkus))]
RefMatchingSkudf = RefMatchingSkudf.merge(internalsku_Ref, on ="SKU", how = "left")

IS_cleandf = MatchingSkudf.append(RefMatchingSkudf, ignore_index=True, sort=False)

#Populate attribute_set with MatchingSkudf
CleanSalesData = (IS_cleandf.merge(MainSheetSummary[["internal_sku", "attribute_set"]]
                                  ,on ="internal_sku", how = "left"))

#Seperate data by year to input CleanSalesData into respective year tabel
CleanSalesData['OD_Year'] = pd.to_datetime(CleanSalesData['Order Date'], format="%d-%b-%y").dt.strftime('%Y')

YearList = CleanSalesData['OD_Year'].dropna().unique().tolist()

for y in range(len(YearList)):
    #Subset CleanSalesData based on year
    year = YearList[y]
    CleanSalesData_YSS = CleanSalesData[CleanSalesData["OD_Year"]==year]

    #Check if file exitst for year
    SalesDataFolderPath = "//TDOTFS01/Group/Data Team/Restricted_Data_Sources/3_Reference_Folder/Raw_Sales_Data"
    Allyearfiles = os.listdir(SalesDataFolderPath)
    fileName = str(year)+".csv"
    filepath = SalesDataFolderPath+"/"+fileName

    if (fileName in Allyearfiles) == True:

        OldYearData =pd.read_csv(filepath,encoding='utf-8')
        OldIDset = set(OldYearData["Order Item Id"])
        NewIDset = set(CleanSalesData_YSS["Order Item Id"])

        IDdifflist = list(NewIDset - OldIDset)
        CS_Unique = CleanSalesData_YSS[CleanSalesData_YSS["Order Item Id"].isin(IDdifflist)]
        OldYearData = OldYearData.append(CS_Unique,sort=False)
        OldYearData.to_csv(filepath, index = None, header=True,  encoding='utf-8')

    else:
        CleanSalesData_YSS.to_csv(filepath, index = None, header=True,  encoding='utf-8')
