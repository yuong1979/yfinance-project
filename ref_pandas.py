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


#####################################
###### company.info reference #######
#####################################

# {
#    "zip":"33134",
#    "sector":"Industrials",
#    "fullTimeEmployees":509,
#    "longBusinessSummary":"AerSale Corporation provides aftermarket commercial aircraft, engines, and its parts to passenger and cargo airlines, leasing companies, original equipment manufacturers, and government and defense contractors, as well as maintenance, repair, and overhaul (MRO) service providers worldwide. It operates in two segments, Asset Management Solutions and Technical Operations (TechOps). The Asset Management Solutions segment engages in the sale and lease of aircraft, engines, and airframes, as well as disassembly of these assets for component parts. The TechOps segment provides internal and third-party aviation services, including internally developed engineered solutions, heavy aircraft maintenance and modification, and component MRO, as well as end-of-life disassembly services. This segment also provides aircraft modifications, cargo and tanker conversions of aircraft, and aircraft storage; and MRO services for landing gear, thrust reversers, hydraulic systems, and other aircraft components. The company was founded in 2008 and is headquartered in Coral Gables, Florida.",
#    "city":"Coral Gables",
#    "phone":"305 764 3200",
#    "state":"FL",
#    "country":"United States",
#    "companyOfficers":[
      
#    ],
#    "website":"https://www.aersale.com",
#    "maxAge":1,
#    "address1":"255 Alhambra Circle",
#    "industry":"Airports & Air Services",
#    "address2":"Suite 435",
#    "ebitdaMargins":0.19462,
#    "profitMargins":0.117639996,
#    "grossMargins":0.3768,
#    "operatingCashflow":111647000,
#    "revenueGrowth":0.519,
#    "operatingMargins":0.16798,
#    "ebitda":88067000,
#    "targetLowPrice":20,
#    "recommendationKey":"hold",
#    "grossProfits":119392000,
#    "freeCashflow":116220248,
#    "targetMedianPrice":20,
#    "currentPrice":18.66,
#    "earningsGrowth":0.214,
#    "currentRatio":6.769,
#    "returnOnAssets":0.10125,
#    "numberOfAnalystOpinions":1,
#    "targetMeanPrice":20,
#    "debtToEquity":"None",
#    "returnOnEquity":0.12687999,
#    "targetHighPrice":20,
#    "totalCash":200446000,
#    "totalDebt":0,
#    "totalRevenue":452495008,
#    "totalCashPerShare":3.875,
#    "financialCurrency":"USD",
#    "revenuePerShare":9.477,
#    "quickRatio":4.87,
#    "recommendationMean":3,
#    "exchange":"NCM",
#    "shortName":"AerSale Corporation",
#    "longName":"AerSale Corporation",
#    "exchangeTimezoneName":"America/New_York",
#    "exchangeTimezoneShortName":"EDT",
#    "isEsgPopulated":false,
#    "gmtOffSetMilliseconds":"-14400000",
#    "quoteType":"EQUITY",
#    "symbol":"ASLE",
#    "messageBoardId":"finmb_84236826",
#    "market":"us_market",
#    "annualHoldingsTurnover":"None",
#    "enterpriseToRevenue":1.655,
#    "beta3Year":"None",
#    "enterpriseToEbitda":8.502,
#    "52WeekChange":0.3306744,
#    "morningStarRiskRating":"None",
#    "forwardEps":0.68,
#    "revenueQuarterlyGrowth":"None",
#    "sharesOutstanding":51726800,
#    "fundInceptionDate":"None",
#    "annualReportExpenseRatio":"None",
#    "totalAssets":"None",
#    "bookValue":8.908,
#    "sharesShort":799237,
#    "sharesPercentSharesOut":0.0155,
#    "fundFamily":"None",
#    "lastFiscalYearEnd":1640908800,
#    "heldPercentInstitutions":0.66689,
#    "netIncomeToCommon":53230000,
#    "trailingEps":0.92,
#    "lastDividendValue":"None",
#    "SandP52WeekChange":-0.056043804,
#    "priceToBook":2.0947464,
#    "heldPercentInsiders":0.22884001,
#    "nextFiscalYearEnd":1703980800,
#    "yield":"None",
#    "mostRecentQuarter":1656547200,
#    "shortRatio":9.85,
#    "sharesShortPreviousMonthDate":1656547200,
#    "floatShares":14261595,
#    "beta":0.503827,
#    "enterpriseValue":748740736,
#    "priceHint":2,
#    "threeYearAverageReturn":"None",
#    "lastSplitDate":"None",
#    "lastSplitFactor":"None",
#    "legalType":"None",
#    "lastDividendDate":"None",
#    "morningStarOverallRating":"None",
#    "earningsQuarterlyGrowth":0.599,
#    "priceToSalesTrailing12Months":2.133111,
#    "dateShortInterest":1659052800,
#    "pegRatio":"None",
#    "ytdReturn":"None",
#    "forwardPE":27.441175,
#    "lastCapGain":"None",
#    "shortPercentOfFloat":0.045300003,
#    "sharesShortPriorMonth":480929,
#    "impliedSharesOutstanding":0,
#    "category":"None",
#    "fiveYearAverageReturn":"None",
#    "previousClose":18.35,
#    "regularMarketOpen":18.16,
#    "twoHundredDayAverage":16.07535,
#    "trailingAnnualDividendYield":0,
#    "payoutRatio":0,
#    "volume24Hr":"None",
#    "regularMarketDayHigh":18.681,
#    "navPrice":"None",
#    "averageDailyVolume10Day":413920,
#    "regularMarketPreviousClose":18.35,
#    "fiftyDayAverage":16.3438,
#    "trailingAnnualDividendRate":0,
#    "open":18.16,
#    "toCurrency":"None",
#    "averageVolume10days":413920,
#    "expireDate":"None",
#    "algorithm":"None",
#    "dividendRate":"None",
#    "exDividendDate":"None",
#    "circulatingSupply":"None",
#    "startDate":"None",
#    "regularMarketDayLow":18.16,
#    "currency":"USD",
#    "trailingPE":20.282608,
#    "regularMarketVolume":34079,
#    "lastMarket":"None",
#    "maxSupply":"None",
#    "openInterest":"None",
#    "marketCap":965222080,
#    "volumeAllCurrencies":"None",
#    "strikePrice":"None",
#    "averageVolume":148101,
#    "dayLow":18.16,
#    "ask":18.39,
#    "askSize":900,
#    "volume":34079,
#    "fiftyTwoWeekHigh":24.2,
#    "fromCurrency":"None",
#    "fiveYearAvgDividendYield":"None",
#    "fiftyTwoWeekLow":12.55,
#    "bid":18.31,
#    "tradeable":false,
#    "dividendYield":"None",
#    "bidSize":1200,
#    "dayHigh":18.681,
#    "coinMarketCapLink":"None",
#    "regularMarketPrice":18.66,
#    "preMarketPrice":18,
#    "logo_url":"https://logo.clearbit.com/aersale.com"
# }












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