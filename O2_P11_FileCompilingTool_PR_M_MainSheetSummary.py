# -*- coding: utf-8 -*-
"""
Created on Mon Feb  3 10:42:49 2020

@author: asheikh
"""

#Import Packages
import os
import glob as gl
import pandas as pd
import re

#Check for duplicate main sheets
def MScheck():

    AllBrandFolderPath = "//192.168.2.32/GoogleDrive/Completed Magento Uploads (v 1.0)"
    AllBrandFolders = os.listdir(AllBrandFolderPath)

    for b in range(len(AllBrandFolders)):

        BrandPath = AllBrandFolderPath+"/"+AllBrandFolders[b]
        BrandMSpath = BrandPath+"/main--*.csv"
        BrandSSpath = BrandPath+"/sub--*.csv"
        MSfilecount = len(gl.glob(BrandMSpath))
        SSfilecount = len(gl.glob(BrandSSpath))
        if MSfilecount != 1:
            print("Following brand will be skipped since they have", MSfilecount,
                  "mainsheets:", AllBrandFolders[b])

MScheck()

#Create summary MS
AllBrandFolderPath = "//192.168.2.32/GoogleDrive/Completed Magento Uploads (v 1.0)"
AllBrandFolders = os.listdir(AllBrandFolderPath)

CompiledMainSheet = pd.DataFrame(columns = ["internal_sku"])
TargetCol = (["internal_sku", "sku", "delete", "attribute_set", "part_type_filter",
              "series_parent", "categories", "upc_code", "na_weight", "na_length", "na_width",
              "na_height", "ca_price", "ca_retail_price", "ca_cost","ca_jobber_price",
             "na_ca_shipping", "na_supplier"])

for b in range(len(AllBrandFolders)):
    print((len(AllBrandFolders)-b))
    Brand = AllBrandFolders[b];     print(Brand)
    BrandPath= str(AllBrandFolderPath+"/"+Brand)
    BrandMSpath = BrandPath+"/main--*.csv"
    filecount = len(gl.glob(BrandMSpath))

    if filecount == 1:
        MSFile = gl.glob(BrandMSpath)[0]
        BrandMS = pd.read_csv(MSFile, encoding='mac_roman')
        BrandMS.columns = map(str.strip, list(BrandMS.columns))
        BrandMS = BrandMS[(BrandMS["type"] == "simple") & ~(BrandMS["internal_sku"].isna())]
        BrandMS = BrandMS[TargetCol]

        #BrandMS = BrandMS[BrandMS["internal_sku"].isin(NonSoldSkus)]
        CompiledMainSheet = CompiledMainSheet.append(BrandMS, ignore_index=True, sort=False)



#Seperate category data and turn into unique list to extract L1/L2/L3 data
CatRef = pd.DataFrame({"categories":CompiledMainSheet["categories"].dropna().unique().tolist()})


for c in range(len(CatRef)):
    cat = CatRef.loc[c, "categories"]
    BrokenCat = re.split(";", cat)
    count = len(BrokenCat)
    BrokenCat = [b for b in BrokenCat if b.find("Brands/") == -1]
    Dub = 0
    L1_dub = 0

    if len(BrokenCat)>0:

        BrokenLevel = pd.DataFrame([re.split("/", l) for l in BrokenCat])
        L1_len = len(BrokenLevel.iloc[:,0].dropna().unique().tolist())

        if L1_len==1: L1_dub = 1
        if L1_len >=2 and len(BrokenLevel)>2:
            L1_subset = BrokenLevel.groupby(0).agg("count").sort_values(1, ascending = False).reset_index()
            BrokenLevel = BrokenLevel[BrokenLevel.loc[:,0] == L1_subset.iloc[0,0]]
            L1_dub = 1

        if L1_dub==1:
            if len(BrokenLevel.columns) ==3:
                BrokenLevel.columns = ["L1", "L2", "L3"]

                L1 = BrokenLevel["L1"].unique()
                L2 = BrokenLevel["L2"].unique()
                L3 = BrokenLevel["L3"].unique()
                if (len(L1)==1 and len(L2)==1 and len(L3)==1): levCount = 3
                if (len(L1)==1 and len(L2)==1 and len(L3)!=1): levCount = 2
                if (len(L1)==1 and len(L2)!=1 and len(L3)!=1): levCount = 1
                if (len(L1)==1 and len(L2)!=1 and len(L3)==1): levCount = 1; Dub = 1

            elif len(BrokenLevel.columns) ==2:
                BrokenLevel.columns = ["L1", "L2"]
                L1 = BrokenLevel["L1"].unique()
                L2 = BrokenLevel["L2"].unique()
                if (len(L1)==1 and len(L2)==1 ): levCount = 2
                if (len(L1)==1 and len(L2)!=1 ): levCount = 1

            elif len(BrokenLevel.columns) ==1:
                BrokenLevel.columns = ["L1"]
                L1 = BrokenLevel["L1"].unique()
                if (len(L1)==1 ): levCount = 1

            if levCount == 3:
                CatRef.loc[c, "count"] = count
                CatRef.loc[c, "levCount"] = levCount
                CatRef.loc[c, "L1"] = L1
                CatRef.loc[c, "L3"] = L3

                if Dub == 0: CatRef.loc[c, "L2"] = L2
                if Dub == 1: CatRef.loc[c, "L2"] = sorted(L2,key=len)[0]

            elif levCount == 2:
                CatRef.loc[c, "count"] = count
                CatRef.loc[c, "levCount"] = levCount
                CatRef.loc[c, "L1"] = L1
                CatRef.loc[c, "L2"] = L2
            elif levCount == 1:
                CatRef.loc[c, "count"] = count
                CatRef.loc[c, "levCount"] = levCount
                CatRef.loc[c, "L1"] = L1


CompiledMainSheet = CompiledMainSheet.merge(CatRef, on = "categories", how = "left")

(CompiledMainSheet.to_csv(
    r"//192.168.2.32/Group/Data Team/Restricted_Data_Sources/3_Reference_Folder/MainSheetSummary.csv",
    index = None, header=True))

print("done")