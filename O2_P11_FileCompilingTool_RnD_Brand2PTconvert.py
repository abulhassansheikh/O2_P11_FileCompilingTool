# -*- coding: utf-8 -*-
"""
Created on Fri Jan 24 15:33:31 2020

@author: asheikh
"""
#List necessary packages
import glob2 as gl
import pandas as pd
import numpy as np
import os
import re

#Create log file
Refpath = "//192.168.2.32/Group/Data Team/Restricted_Data_Sources/3_Reference_Folder/"
logpath =Refpath+"PT_Ms_Log.txt"
logref = list(os.listdir(Refpath))

if ("PT_Ms_Log.txt" in logref) == True:
    os.remove(logpath)

def logtxt(text):
    log = open(logpath,"a+")
    log.write(text + "\n")
    log.close()

#File paths
AllBrandFolderPath = "//192.168.2.32/GoogleDrive/Completed Magento Uploads (v 1.0)"
AllBrandFolders = os.listdir(AllBrandFolderPath)

PTFolderPath = "//192.168.2.32/GoogleDrive/PT_mainsheet"
AllPTfiles = os.listdir(PTFolderPath)

#Pull target PT reference df to ID PT groups that I will ETL
TargetPT = (pd.read_csv(
           "//192.168.2.32/Group/Data Team/Restricted_Data_Sources/3_Reference_Folder/PT_comp_ref.csv",
           encoding='utf-8', low_memory=False))
TargetPT = TargetPT[(TargetPT["compile"]==1)]


#Tidy PT names for file name creation
TargetPT["new_name"] = [re.sub("-", "_",c) for c in TargetPT["name"]]
TargetPT["new_name"] = [re.sub("&", "and",c) for c in TargetPT["new_name"]]
TargetPT["new_name"] = [re.sub(";", "_or_",c) for c in TargetPT["new_name"]]
TargetPT["new_name"] = [re.sub("/", "_",c) for c in TargetPT["new_name"]]
TargetPT["new_name"] = [re.sub(" ", "_",c) for c in TargetPT["new_name"]]
TargetPT["new_name"] = [re.sub("__", "_",c) for c in TargetPT["new_name"]]
TargetPT["new_name"] = [re.sub("__", "_",c) for c in TargetPT["new_name"]]

TargetPT["pt_file"] = "main--"+TargetPT["new_name"]+".csv"
TargetPT["pt_file_del"] = "main--"+TargetPT["new_name"]+"*.csv"
TargetPT["pt_path"] = PTFolderPath+"/"+TargetPT["pt_file"]
TargetPT["pt_path_del"] = PTFolderPath+"/"+TargetPT["pt_file_del"]

TargetPT["pt_fileSS"] = "sub--"+TargetPT["new_name"]+".csv"
TargetPT["pt_fileSS_del"] = "sub--"+TargetPT["new_name"]+"*.csv"
TargetPT["pt_pathSS"] = PTFolderPath+"/"+TargetPT["pt_fileSS"]
TargetPT["pt_pathSS_del"] = PTFolderPath+"/"+TargetPT["pt_fileSS_del"]


#ID and delete all pre existing MS files
Deletefile_ref = (TargetPT.groupby(["pt_file", "pt_file_del", "pt_path", "pt_path_del",
                                    "pt_fileSS", "pt_fileSS_del", "pt_pathSS", "pt_pathSS_del"])
                                      .agg("count").reset_index())

for f in range(len(Deletefile_ref)):
    msFile_path = Deletefile_ref.loc[f, "pt_path_del"]
    ssFile_path = Deletefile_ref.loc[f, "pt_pathSS_del"]

    MSdelList = gl.glob(msFile_path)
    SSdelList = gl.glob(ssFile_path)

    dellist = MSdelList + SSdelList

    for d in range(len(dellist)):
        if len(gl.glob(dellist[d])) ==1:
            os.remove(dellist[d])


for b in range(len(AllBrandFolders)):

    print("->"+str(len(AllBrandFolders)-b))
    Brand = AllBrandFolders[b];     print("--->"+Brand); logtxt("--->"+Brand)
    BrandPath= str(AllBrandFolderPath+"/"+Brand)
    BrandMSpath = BrandPath+"/main--*.csv"
    BrandSSpath = BrandPath+"/sub--*.csv"
    filecount = len(gl.glob(BrandMSpath))
    filecountSS = len(gl.glob(BrandSSpath))

    AllPTfiles = os.listdir(PTFolderPath)


    if filecount == 1:
        MSFile = gl.glob(BrandMSpath)[0]
        BrandMS = pd.read_csv(MSFile, encoding='mac_roman', low_memory=False)
        BrandMS.columns = [c.strip() for c in BrandMS.columns]

        brandPT = BrandMS["part_type_filter"].dropna().unique().tolist()
        brandPT = [c.strip() for c in brandPT]
        brandPT = [p for p in brandPT if p in list(TargetPT["part_type_filter"])]

        if len(brandPT) != 0:
            brandPT = TargetPT[TargetPT["part_type_filter"].isin(brandPT)]
            PTgroups = list(brandPT["group"].unique())

            for p in range(len(PTgroups)):
                PTsubset_df = brandPT[brandPT["group"]==PTgroups[p]]
                PTlist = PTsubset_df["part_type_filter"].dropna().unique().tolist()
                pt = PTsubset_df["name"].dropna().unique().tolist()[0]
                print("----->"+pt) ; logtxt("----->"+pt)
                pt_new = PTsubset_df["new_name"].dropna().unique().tolist()[0]
                pt_file = PTsubset_df["pt_file"].dropna().unique().tolist()[0]
                pt_path = PTsubset_df["pt_path"].dropna().unique().tolist()[0]
                pt_fileSS = PTsubset_df["pt_fileSS"].dropna().unique().tolist()[0]
                pt_pathSS = PTsubset_df["pt_pathSS"].dropna().unique().tolist()[0]

                #Mainsheet Sku Pull
                PtSubset = BrandMS[BrandMS["part_type_filter"].isin(PTlist)]
                PtSubset = PtSubset.dropna(axis=1,how='all')
                PTintSku = PtSubset["internal_sku"].dropna().unique().tolist()

                #Series Pull
                Pt_serieslist = list(PtSubset["series_parent"].unique())
                SeriesSubset = BrandMS[(BrandMS["type"]== "series") &
                                       (BrandMS["internal_sku"].isin(Pt_serieslist))]
                SeriesSubset = SeriesSubset.dropna(axis=1,how='all')

                #Append sku with series
                finalsubset = PtSubset.append(SeriesSubset,sort=False)
                print("--------> MS: "+str(len(PTintSku)))
                logtxt("--------> MS: "+str(len(PTintSku)))


                if (pt_file in AllPTfiles) == True:
                    pt_fileMS_test = PTFolderPath+"/"+"main--"+pt_new+"*.csv" #*Here
                    num = str(len(gl.glob(pt_fileMS_test)))
                    LatestMSfile = gl.glob(pt_fileMS_test)[-1]
                    MSfileSize = os.path.getsize(LatestMSfile)*0.001

                    if MSfileSize <= 100000:
                        Old_PTfile=pd.read_csv(LatestMSfile, encoding='utf-8', low_memory=False)
                        New_PTfile = Old_PTfile.append(finalsubset, sort=False)
                        New_PTfile.to_csv(LatestMSfile, index = None, header=True,  encoding='utf-8')

                    else:
                        newMSPath = PTFolderPath+"/"+"main--"+pt_new+"_"+num+".csv"
                        finalsubset.to_csv(newMSPath, index = None, header=True,  encoding='utf-8')

                else:
                    finalsubset.to_csv(pt_path, index = None, header=True,  encoding='utf-8')

               #Pull out Subsheet
                if filecountSS >= 1:
                    SSFile = gl.glob(BrandSSpath)
                    BrandSS = pd.DataFrame()

                    for s in range(len(SSFile)):
                        BrandSS_single = pd.read_csv(SSFile[s], encoding='mac_roman', low_memory=False)
                        BrandSS_single.columns = [c.strip() for c in BrandSS_single.columns]
                        BrandSS = BrandSS.append(BrandSS_single, sort=False)

                    #Subset Subsheet
                    PTSubSheet = BrandSS[BrandSS["internal_sku"].isin(PTintSku)]
                    PTSubSheet = PTSubSheet.dropna(axis=1,how='all')
                    print("--------> SS: "+str(len(PTSubSheet)))
                    logtxt("--------> SS: "+str(len(PTSubSheet)))


                    if (pt_fileSS in AllPTfiles) == True:

                        pt_fileSS_test = PTFolderPath+"/"+"sub--"+pt_new+"*.csv"
                        num = str(len(gl.glob(pt_fileSS_test)))
                        LatestSSfile = gl.glob(pt_fileSS_test)[-1]
                        SSfileSize = os.path.getsize(LatestSSfile)*0.001

                        if SSfileSize <= 100000:
                            Old_SSfile=pd.read_csv(LatestSSfile, encoding='utf-8', low_memory=False)
                            New_SSfile = Old_SSfile.append(PTSubSheet, sort=False)
                            New_SSfile.to_csv(LatestSSfile, index = None, header=True,  encoding='utf-8')

                        else:
                            newSSPath = PTFolderPath+"/"+"sub--"+pt_new+"_"+num+".csv"
                            PTSubSheet.to_csv(newSSPath, index = None, header=True,  encoding='utf-8')

                    else:
                        PTSubSheet.to_csv(pt_pathSS, index = None, header=True,  encoding='utf-8')

print("**********")
print("***Done***")
print("**********")