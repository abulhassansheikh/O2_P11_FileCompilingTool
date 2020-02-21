# -*- coding: utf-8 -*-
"""
Created on Tue Feb  4 11:05:25 2020

@author: asheikh
"""
#Load packages
import pandas as pd
import numpy as np
import os

#Load supporting data
MainSheetSummary = pd.read_csv(
    "//192.168.2.32/Group/Data Team/Restricted_Data_Sources/3_Reference_Folder/MainSheetSummary.csv"
    ,encoding='utf-8')

#Create reference items
CompiledSales_R = pd.DataFrame()

#File Path info
SalesDataFolderPath = "//TDOTFS01/Group/Data Team/Restricted_Data_Sources/3_Reference_Folder/Raw_Sales_Data"
Allyearfiles = os.listdir(SalesDataFolderPath)

##Insert for loop here
for y in range(len(Allyearfiles)):
    SalesFile = Allyearfiles[y]

    YearData =pd.read_csv(SalesDataFolderPath+"/"+SalesFile ,encoding='utf-8')
    CompiledSales_R = CompiledSales_R.append(YearData, ignore_index=True, sort=False)

#Merge the updated summary main sheet data with CompiledSales_R
CompiledSales = CompiledSales_R.merge(MainSheetSummary, on = ["internal_sku", "attribute_set"], how = "left" )
CompiledSales["Net Retail Price (CAD)"] = [float((str(p).replace("$", "")).replace(",", "")) for p in list(CompiledSales["Net Retail Price (CAD)"])]
#https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior
CompiledSales['Formated_Order_Date'] = pd.to_datetime(CompiledSales['Order Date'], format= "%d-%b-%y")

-----
-----
#Price prediction model
priceAna = CompiledSales
#Id skus that has had more than 2 different prices
SkuPrice = priceAna.groupby(["internal_sku", "Net Retail Price (CAD)"]).agg("count").reset_index()[["internal_sku", "Net Retail Price (CAD)"]]
SkuPrice = SkuPrice.groupby("internal_sku").agg("count").reset_index()
SkuPrice.columns = ["internal_sku", "nPrice"]
SkuList = set(SkuPrice[SkuPrice["nPrice"]>(2)]["internal_sku"])



#Plan
- Build ARIMA model per sku/store it
- Build price/sales model
- Modify the price to see how St+1 is in relation to predicted sales value
- Based on the difference in price Increwase/decrease, recommend price change


-----
-----
#Fraud detection using logistic regression
import re
frauddf = CompiledSales[["Order Bucket", "Email"]]
list(CompiledSales.columns)

#Factors that play a role in fraud
- Brand
- YMM
- Email domain
- Possibly the price (ca_price)
- Payment Method ?
- IP Address maybe?

len(CompiledSales[CompiledSales["Order Bucket"] == "Fraud Bucket"])
len(CompiledSales[CompiledSales["Order Bucket"] != "Fraud Bucket"])

set(CompiledSales["attribute_set"])
set(CompiledSales["Year"])
set(CompiledSales["Make"])
set(CompiledSales["Model"])
set(CompiledSales["IP Address"])
set(CompiledSales["Email"])
set(CompiledSales["ca_price"])
set(CompiledSales["Payment Method"])

set(CompiledSales["attribute_set"][CompiledSales["Order Bucket"] == "Fraud Bucket"])
set(CompiledSales["Year"][CompiledSales["Order Bucket"] == "Fraud Bucket"])
set(CompiledSales["Make"][CompiledSales["Order Bucket"] == "Fraud Bucket"])
set(CompiledSales["Model"][CompiledSales["Order Bucket"] == "Fraud Bucket"])
set(CompiledSales["IP Address"][CompiledSales["Order Bucket"] == "Fraud Bucket"])
set(CompiledSales["Email"][CompiledSales["Order Bucket"] == "Fraud Bucket"])
set(CompiledSales["ca_price"][CompiledSales["Order Bucket"] == "Fraud Bucket"])
set(CompiledSales["Payment Method"][CompiledSales["Order Bucket"] == "Fraud Bucket"])

#I am going to build a base Fraud checker by using the email and build up from there
email = "ihwe23i_h.we23@we.weuu.ce"
for e in range(len(frauddf)):
    email = frauddf["Email"][1000]
    username = email.split("@")[0]
    username_len = len(username)
    username_num = len(re.findall(r'\d', username))
    username_sym = len(re.findall(r'([.\?#@+,_\-\<>%~`!$^&\(\):;])', username))


    domain = email.split("@")[1]


allsym = set()

for e in range(len(frauddf)):
    email =str(frauddf["Email"][e])
    username = email.split("@")[0]
    A = set(re.findall(r'([.\?#@+,_\-\<>%~`!$^&\(\):;])', username))
    allsym.update(A)

symlist = ['!', '#', '&', '+', '-', '.', '_', '`']
for e in range(len(frauddf)):
    email =str(frauddf["Email"][e])
    username = email.split("@")[0]
    A = len(re.findall(r'([!#&+\`])', username))
    if A>0:
        print(email)

-----
#Find the rev for l2/l3 from Aug-2018 to july-2019
A = CompiledSales[(CompiledSales['Formated_Order_Date']< pd.Timestamp(2019, 8, 1)) &
                  (CompiledSales['Formated_Order_Date']>= pd.Timestamp(2018, 8, 1)) &
                  (CompiledSales["delete"]=="N")
                  ]

B = A.groupby(['L2'])["Net Retail Price (CAD)"].agg("sum").reset_index()
B.to_csv(SalesDataFolderPath+"/L2Rev2018_2019.csv", index = None, header=True,  encoding='utf-8')

C = A.groupby(['L3'])["Net Retail Price (CAD)"].agg("sum").reset_index()
C.to_csv(SalesDataFolderPath+"/L3Rev2018_2019.csv", index = None, header=True,  encoding='utf-8')

-----
#Find the sales volume of each YMM from 2018-2019
A = (CompiledSales[(CompiledSales["OD_Year"].isin([2018, 2019])) &
                   (CompiledSales["delete"]=="N")
                  ])

B = A.groupby(['part_type_filter']).agg("count")["Order Date"].reset_index()
B = A.groupby(['part_type_filter'])["Net Retail Price (CAD)"].agg("sum").reset_index()
B = A.groupby(['series_parent']).agg("count")["Order Date"].reset_index()
B = A.groupby(['Make','Model']).agg("count")["Order Date"].reset_index()

B.to_csv(SalesDataFolderPath+"/PartTypeRev2018_2019.csv", index = None, header=True,  encoding='utf-8')

-------------------------------
#Determine the most popular skus
- interested in active wrt MS %%
- Frequently bought
- Specific brands
- Cumulative cost accross all years
- maybe look into the province to id US based purchases and add that to the mix

brands = ['Magnaflow',
          'Husky Liners',
          'WeatherTech',
          'Superchips',
          'Hawk Performance',
          'PIAA',
          'HSD HR Coilovers',
          'Covercraft',
          'EBC Brakes',
          'ReadyLift',
          'DiabloSport',
          'Truxedo',
          'KW Suspension',
          'MBRP',
          'JET Performance',
          'Retrax',
          'Fox Shox']

A = CompiledSales[(CompiledSales["delete"]=="N") &
                  (CompiledSales["attribute_set"].isin(brands))]

list(CompiledSales.columns)
set(CompiledSales["Province"])
B = A.groupby("internal_sku")["Order Date"].agg("count").sort_values(ascending = False).reset_index()


B.to_csv(SalesDataFolderPath+"/topskus.csv", index = None, header=True,  encoding='utf-8')

B = B.merge(MainSheetSummary[["internal_sku", "sku", "upc_code"]], on = ["internal_sku"], how = "left" )

MainSheetSummary.columns
_________________________
#Identify all mismatching skus depening on cost criteria, from up to 2 years prior
#Pool all necessary data and create criteria variables
internal_skuRef =pd.read_csv('//192.168.2.32/Group/Data Team/Recommender_System_Location/1_Reference_Files/internal_skuRef.csv', encoding='utf-8')
CostLimit = 15000 # = (sum of cost per sku)/(PriorYearLimit+1 Years), in this case, skus with >5K/year are considered
PriorYearLimit = 2
Sales_sku = set(CompiledSales["SKU"].unique())
MS_sku = set(CompiledMainSheet["internal_sku"].unique())
ISref = set(internal_skuRef["SKU"].unique())
CurrentYear = datetime.datetime.now().year
LatestMonth = (CompiledSales['Order_Date'].sort_values(ascending=False).reset_index()).iloc[0, :]
YearRange = list(range((CurrentYear)-PriorYearLimit, (CurrentYear)+1))

#Identify missing internal_skus
MissMatch = pd.DataFrame({"internal_sku": list((Sales_sku - ISref) - MS_sku)})
MissingSkus= CompiledSales[CompiledSales["SKU"].isin(MissMatch["internal_sku"])]

MissingSkusBrands = (MissingSkus[MissingSkus["OD_Year"].isin(YearRange)]
                     .groupby(["Brands"])["Product Cost (CAD)"]
                     .agg("sum")
                     .reset_index())
                    #.sort_values("Product Cost (CAD)", ascending=False)

MissingSkusBrands= MissingSkusBrands[MissingSkusBrands["Product Cost (CAD)"] >= CostLimit]
KeyMissingSkus= (pd.DataFrame({"internal_sku": (MissingSkus[MissingSkus["Brands"]
                            .isin(MissingSkusBrands["Brands"])]["SKU"].unique())}))

print(len(KeyMissingSkus)) #Print out number of skus that need fixing
KeyMissingSkus.to_csv(r'//192.168.2.32/Group/Data Team/Recommender_System_Location/1_Reference_Files/KeyMissingSkus.csv', index = None, header=True)

#Compile and subset data sources
CompiledSales_R = pd.concat([Y2013,Y2014], axis=0, ignore_index=True, sort=False)
CompiledSales_R = pd.concat([CompiledSales_R,Y2015], axis=0, ignore_index=True, sort=False)
CompiledSales_R = pd.concat([CompiledSales_R,Y2016], axis=0, ignore_index=True, sort=False)
CompiledSales_R = pd.concat([CompiledSales_R,Y2017], axis=0, ignore_index=True, sort=False)
CompiledSales_R = pd.concat([CompiledSales_R,Y2018], axis=0, ignore_index=True, sort=False)
CompiledSales_R = pd.concat([CompiledSales_R,Y2019], axis=0, ignore_index=True, sort=False)

CompiledSales_R = CompiledSales_R[ (CompiledSales_R["Year"] != "All") & (CompiledSales_R["Year"] != "ALL")
                             & (CompiledSales_R["Year"] != "B1800")
                             & (CompiledSales_R["Year"] != "nofit")
                             & ~CompiledSales_R["Year"].isnull()]

CompiledSales_R['Order_Date'] = pd.to_datetime(CompiledSales_R['Order Date'], format= "%d-%b-%y")
CompiledSales_R['OD_Year'] = pd.to_numeric(CompiledSales_R['Order_Date'].dt.strftime('%Y'))
CompiledSales_R['OD_MonthNum'] = CompiledSales_R['Order_Date'].dt.strftime('%m')
CompiledSales_R['OD_MonthLab'] = CompiledSales_R['Order_Date'].dt.strftime('%B')
CompiledSales_R['OD_MonthDay'] = CompiledSales_R['Order_Date'].dt.strftime('%d')
CompiledSales_R['OD_WeekDay'] = CompiledSales_R['Order_Date'].dt.strftime('%A')
CompiledSales_R["Product Cost (CAD)"] = pd.to_numeric(CompiledSales_R["Product Cost (CAD)"])

CompiledSales = CompiledSales_R

#Clean Sales data skus
CompiledSales = CompiledSales.merge(internal_skuRef, on = "SKU", how = 'outer')
CompiledSales["internal_sku"] = CompiledSales[~(CompiledSales["SKU"].isin(internal_skuRef["SKU"]))]["SKU"]

#Perform sku count match
A = len(CompiledSales["SKU"].unique())
B = len(CompiledSales["internal_sku"].unique())
C = len(internal_skuRef)
E = len(CompiledSales[CompiledSales["internal_sku"].isna() == True]["SKU"].unique())

if (A == (B + E - 1)) == True:
    CompiledSales.to_csv(r'//192.168.2.32/Group/Data Team/Recommender_System_Location/1_Reference_Files/CompiledSales.csv', index = None, header=True)
else:
    print("Issue Detected")