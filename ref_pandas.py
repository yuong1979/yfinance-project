from firebase_admin import firestore
import pandas as pd
import numpy as np



# #### export required data to google sheets ######
# db = firestore.Client.from_service_account_json("secret/serviceAccountKey.json")
# docs = db.collection('tickerlisttest').stream()
# datalist = []
# for i in docs:
#     datalist.append(i._data['kpi'])
# df = pd.DataFrame(datalist)
# df.replace(np.nan, '', inplace=True)



# # listing all the columns
# print (df.columns)

## presenting the df only with the columns desired and rows 0 to 3
# print (df[['symbol', 'sector']][0:3])

# print (df.iloc[0:2])

# #get a rows you want into a list
# for index, row in df.iterrows():
#     print(index, row['symbol'])

# column_names = df.columns.values.tolist()
# print (column_names)

# #get data according to a certain condition
# print (df.loc[df['sector'] == "Technology"])

# # #sorting values by first symbol return on equity
# print (df.sort_values(['symbol', 'returnOnEquity'], ascending=False))


# ## creating a new column with a new formula - issues
# df['diff'] = df['dayHigh'] + df['dayLow'] 

# ## removing a specific column
# df = df.drop(columns=['diff'])

# ## selecting a few columns using numbers
# df = df.iloc[:, 2:10]
# print (df)

## group by functions that are powerful



# this dataset is based on https://youtu.be/txMdrV1Ut64
## list of all columns
# ResponseId,MainBranch,Employment,RemoteWork,CodingActivities,EdLevel,LearnCode,LearnCodeOnline,LearnCodeCoursesCert,YearsCode,YearsCodePro,DevType,OrgSize,PurchaseInfluence,BuyNewTool,Country,Currency,CompTotal,CompFreq,LanguageHaveWorkedWith,LanguageWantToWorkWith,DatabaseHaveWorkedWith,DatabaseWantToWorkWith,PlatformHaveWorkedWith,PlatformWantToWorkWith,WebframeHaveWorkedWith,WebframeWantToWorkWith,MiscTechHaveWorkedWith,MiscTechWantToWorkWith,ToolsTechHaveWorkedWith,ToolsTechWantToWorkWith,NEWCollabToolsHaveWorkedWith,NEWCollabToolsWantToWorkWith,OpSysProfessional use,OpSysPersonal use,VersionControlSystem,VCInteraction,VCHostingPersonal use,VCHostingProfessional use,OfficeStackAsyncHaveWorkedWith,OfficeStackAsyncWantToWorkWith,OfficeStackSyncHaveWorkedWith,OfficeStackSyncWantToWorkWith,Blockchain,NEWSOSites,SOVisitFreq,SOAccount,SOPartFreq,SOComm,Age,Gender,Trans,Sexuality,Ethnicity,Accessibility,MentalHealth,TBranch,ICorPM,WorkExp,Knowledge_1,Knowledge_2,Knowledge_3,Knowledge_4,Knowledge_5,Knowledge_6,Knowledge_7,Frequency_1,Frequency_2,Frequency_3,TimeSearching,TimeAnswering,Onboarding,ProfessionalTech,TrueFalse_1,TrueFalse_2,TrueFalse_3,SurveyLength,SurveyEase,ConvertedCompYearly

df  = pd.read_csv('secret/test.csv', index_col="ResponseId")

# # ConvertedComp is salary
# # Getting media salary from the column ConvertedCompYearly
# med_sal_df = df['ConvertedCompYearly'].median()
# print (med_sal_df)

# # Applying the median to all columns in dataframe
# med_all_df = df.median()
# print (med_all_df)

# # Applying describe to all columns in dataframe
# describe_df = df.describe()
# print (describe_df)

# # Applying describe to only one column in dataframe
# describe_df = df['ConvertedCompYearly'].describe()
# print (describe_df)

# # Getting count of people who filled in the ConvertedCompYearly (salary)
# count_sal_df = df['ConvertedCompYearly'].median()
# print (count_sal_df)


# #getting the value breakdown
# val_brkdown_df = df['Country'].value_counts()
# print (val_brkdown_df)

# #getting the value breakdown %
# val_brkdown_df = df['Country'].value_counts(normalize=True)
# print (val_brkdown_df)

#Groupby two categories
gb_country = df.groupby(['Country'])
print (gb_country)

##Groupby Country and employment type
# gb_country_employment = gb_country['Employment'].value_counts().head(20)
# print (gb_country_employment)

##Groupby Country and employment type - filterd by country
# gb_country_employment = gb_country['Employment'].value_counts().loc['Thailand']
# print (gb_country_employment)

# #Groupby two categories - without breakdown
# gb_country = df.groupby(['Country'])['Employment'].value_counts()
# print (gb_country)

#Groupby two categories - without breakdown
gb_country = df.groupby(['Country', 'Employment'])['CompTotal'].sum()
print (gb_country)


# #checking if any data in empty or NA in a dataset
# df.isna()

# ## fill all NA values with "MISSING" string - useful if used to replace empty cells with zeros
# df.fillna('MISSING')

#replacing all customized empty data"NA" and "missing" with np.nan
df.replace("NA", np.nan, inplace=True)
df.replace("Missing", np.nan, inplace=True)

##dropping all rows with NaN data in any of the data
# https://youtu.be/KdmPHEnPJPs?t=215
df = df.dropna(axis='index', how="any")

##dropping all rows with NaN data in email and lastname
df = df.dropna(axis='index', how="any", subset=["lastname","email"])

















# #Groupby two categories - without breakdown
# gb_country = df.groupby(['Country', 'Employment'])['CompTotal'].sum()
# print (gb_country)






# ResponseId
# MainBranch
# Employment
# RemoteWork
# CodingActivities
# EdLevel
# LearnCode
# LearnCodeOnline
# LearnCodeCoursesCert
# YearsCode
# YearsCodePro
# DevType
# OrgSize
# PurchaseInfluence
# BuyNewTool
# Country
# Currency
# CompTotal
# CompFreq
# LanguageHaveWorkedWith
# LanguageWantToWorkWith
# DatabaseHaveWorkedWith
# DatabaseWantToWorkWith
# PlatformHaveWorkedWith
# PlatformWantToWorkWith
# WebframeHaveWorkedWith
# WebframeWantToWorkWith
# MiscTechHaveWorkedWith
# MiscTechWantToWorkWith
# ToolsTechHaveWorkedWith
# ToolsTechWantToWorkWith
# NEWCollabToolsHaveWorkedWith
# NEWCollabToolsWantToWorkWith
# OpSysProfessional use
# OpSysPersonal use
# VersionControlSystem
# VCInteraction
# VCHostingPersonal use
# VCHostingProfessional use
# OfficeStackAsyncHaveWorkedWith
# OfficeStackAsyncWantToWorkWith
# OfficeStackSyncHaveWorkedWith
# OfficeStackSyncWantToWorkWith
# Blockchain
# NEWSOSites
# SOVisitFreq
# SOAccount
# SOPartFreq
# SOComm
# Age
# Gender
# Trans
# Sexuality
# Ethnicity
# Accessibility
# MentalHealth
# TBranch
# ICorPM
# WorkExp
# Knowledge_1
# Knowledge_2
# Knowledge_3
# Knowledge_4
# Knowledge_5
# Knowledge_6
# Knowledge_7
# Frequency_1
# Frequency_2
# Frequency_3
# TimeSearching
# TimeAnswering
# Onboarding
# ProfessionalTech
# TrueFalse_1
# TrueFalse_2
# TrueFalse_3
# SurveyLength
# SurveyEase
# ConvertedCompYearly