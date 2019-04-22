from query_parser import QueryParser
import pandas as pd
import datetime as dt
import time
from nlp_parser import NlpParser
from suggestion_generator import Suggestion_generator
from query_executor import QueryExecutor
import csv

NLP_PARSER = NlpParser()
QUERY_PARSER = QueryParser()
SUGGESTIONS = Suggestion_generator()
QUERY_EXECUTOR = QueryExecutor()

# data=pd.read_csv('"/Users/sujis/Downloads/SNap_Office/CMR.csv',error_bad_lines=False,index_col=False)
date_converter = lambda d: pd.Timestamp.strptime(d, '%m/%d/%Y').date()
# data = pd.read_csv('/Users/sujit.venkata/Downloads/Market Share Data.csv', converters={"Data": date_converter})
# data = pd.read_csv('/Users/bhagath.babu/Projects/data/Market Share Data_V2.csv', converters={"Date": date_converter})
# data = pd.read_csv('/home/gopal/Downloads/MarketShareData.csv', converters={"Date": date_converter})
data = pd.read_csv('D:/Compiler/data/A1 Market Share Sample Data.csv')

#
# catalog = [{"business_friendly_name":"Channel","column_role":"","count_distinct":True,"criticality":"Critical","datatype":"string","display_format":"","equivalents":"","filterable":False,"format":"string","group_by":True,"is_dttm":False,"max":False,"mean":False,"median":False,"min":False,"name":"Channel","position":1,"preview_data":["Mass","Prestige","Mass","Mass","Mass","Mass","Mass","Mass","Mass","Mass"],"std_dev":False,"sum":False},
#            {"business_friendly_name":"Category","column_role":"","count_distinct":True,"criticality":"Critical","datatype":"string","display_format":"","equivalents":"","filterable":False,"format":"string","group_by":True,"is_dttm":False,"max":False,"mean":False,"median":False,"min":False,"name":"Category","position":2,"preview_data":["APDEO","APDEO","APDEO","APDEO","APDEO","APDEO","APDEO","APDEO","APDEO","APDEO"],"std_dev":False,"sum":False},
#            {"business_friendly_name":"SubCategory","column_role":"","count_distinct":True,"criticality":"Critical","datatype":"string","display_format":"","equivalents":"","filterable":False,"format":"string","group_by":True,"is_dttm":False,"max":False,"mean":False,"median":False,"min":False,"name":"SubCategory","position":3,"preview_data":["A/O FORM","A/O FORM","A/O FORM","A/O FORM","A/O FORM","A/O FORM","A/O FORM","A/O FORM","A/O FORM","A/O FORM"],"std_dev":False,"sum":False},
#            {"business_friendly_name":"Brand","column_role":"","count_distinct":True,"criticality":"Critical","datatype":"string","display_format":"","equivalents":"","filterable":False,"format":"string","group_by":True,"is_dttm":False,"max":False,"mean":False,"median":False,"min":False,"name":"Brand","position":4,"preview_data":["A/O BRAND ","A/O BRAND ","A/O BRAND ","A/O BRAND ","A/O BRAND ","A/O BRAND ","A/O BRAND ","A/O BRAND ","A/O BRAND ","A/O BRAND "],"std_dev":False,"sum":False},
#            {"business_friendly_name":"Manufacturer","column_role":"","count_distinct":True,"criticality":"Critical","datatype":"string","display_format":"","equivalents":"","filterable":False,"format":"string","group_by":True,"is_dttm":False,"max":False,"mean":False,"median":False,"min":False,"name":"Manufacturer","position":5,"preview_data":["Theta","Theta","Theta","Theta","Theta","Theta","Theta","Theta","Theta","Theta"],"std_dev":False,"sum":False},
#            {"business_friendly_name":"SalesDollars","column_role":"","count_distinct":True,"criticality":"Critical","datatype":"numeric","display_format":"","equivalents":"","filterable":False,"format":"_.##","group_by":False,"is_dttm":False,"max":True,"mean":True,"median":True,"min":True,"name":"SalesDollars","position":6,"preview_data":["2418","2889","4623","3545","3179","3678","2612","3079","2890","4921"],"std_dev":True,"sum":True},
#            {"business_friendly_name":"SalesUnits","column_role":"","count_distinct":True,"criticality":"Critical","datatype":"numeric","display_format":"","equivalents":"","filterable":False,"format":"_.##","group_by":False,"is_dttm":False,"max":True,"mean":True,"median":True,"min":True,"name":"SalesUnits","position":8,"preview_data":["8112","4065","6563","6875","213","7599","8770","9652","8630","8053"],"std_dev":True,"sum":True},
#            {"business_friendly_name":"Date","column_role":"","count_distinct":True,"criticality":"Critical","datatype":"date","display_format":"","equivalents":"'dt_time','date','time'","filterable":False,"format":"mm/dd/yyyy","group_by":True,"is_dttm":False,"max":False,"mean":False,"median":False,"min":False,"name":"Date","position":9,"preview_data":["4/1/2017","4/1/2018","8/1/2016","8/1/2017","12/1/2016","12/1/2017","2/1/2017","2/1/2018","1/1/2017","1/1/2018"],"std_dev":False,"sum":False,"addon_columns":["DateWeek","DateMonth","DateQuarter","DateYear"]},
#            {"business_friendly_name":"TYSalesDollars","column_role":"","count_distinct":True,"criticality":"Critical","datatype":"numeric","display_format":"","equivalents":"","filterable":False,"format":"_.##","group_by":False,"is_dttm":False,"max":True,"mean":True,"median":True,"min":True,"name":"TYSalesDollars","position":10,"preview_data":["0","2889","0","0","0","0","0","3079","0","4921"],"std_dev":True,"sum":True},
#            {"business_friendly_name":"TYSalesUnits","column_role":"","count_distinct":True,"criticality":"Critical","datatype":"numeric","display_format":"","equivalents":"","filterable":False,"format":"_.##","group_by":False,"is_dttm":False,"max":True,"mean":True,"median":True,"min":True,"name":"TYSalesUnits","position":11,"preview_data":["0","4065","0","0","0","0","0","9652","0","8053"],"std_dev":True,"sum":True},
#            {"business_friendly_name":"LYSalesDollars","column_role":"","count_distinct":True,"criticality":"Critical","datatype":"numeric","display_format":"","equivalents":"","filterable":False,"format":"_.##","group_by":False,"is_dttm":False,"max":True,"mean":True,"median":True,"min":True,"name":"LYSalesDollars","position":12,"preview_data":["2418","0","0","3545","0","3678","2612","0","2890","0"],"std_dev":True,"sum":True},
#            {"business_friendly_name":"LYSalesUnits","column_role":"","count_distinct":True,"criticality":"Critical","datatype":"numeric","display_format":"","equivalents":"","filterable":False,"format":"_.##","group_by":False,"is_dttm":False,"max":True,"mean":True,"median":True,"min":True,"name":"LYSalesUnits","position":13,"preview_data":["8112","0","0","6875","0","7599","8770","0","8630","0"],"std_dev":True,"sum":True},
#            {"business_friendly_name":"FiscalMonth","column_role":"","count_distinct":True,"criticality":"Critical","datatype":"string","display_format":"","equivalents":"","filterable":False,"format":"string","group_by":True,"is_dttm":False,"max":False,"mean":False,"median":False,"min":False,"name":"FiscalMonth","position":14,"preview_data":["April","April","August","August","December","December","February","February","January","January"],"std_dev":False,"sum":False},
#            {"business_friendly_name":"FiscalQuarter","column_role":"","count_distinct":True,"criticality":"Critical","datatype":"numeric","display_format":"","equivalents":"","filterable":False,"format":"_.##","group_by":False,"is_dttm":False,"max":True,"mean":True,"median":True,"min":True,"name":"FiscalQuarter","position":15,"preview_data":["2","2","3","3","4","4","1","1","1","1"],"std_dev":True,"sum":True},
#            {"business_friendly_name":"FiscalYear","column_role":"","count_distinct":True,"criticality":"Critical","datatype":"numeric","display_format":"","equivalents":"","filterable":False,"format":"_.##","group_by":False,"is_dttm":False,"max":True,"mean":True,"median":True,"min":True,"name":"FiscalYear","position":16,"preview_data":["2017","2018","2016","2017","2016","2017","2017","2018","2017","2018"],"std_dev":True,"sum":True},
#            {"business_friendly_name":"FiscalMonthYear","column_role":"","count_distinct":True,"criticality":"Critical","datatype":"numeric","display_format":"","equivalents":"","filterable":False,"format":"_.##","group_by":False,"is_dttm":False,"max":True,"mean":True,"median":True,"min":True,"name":"FiscalMonthYear","position":17,"preview_data":["201704","201804","201608","201708","201612","201712","201702","201802","201701","201801"],"std_dev":True,"sum":True},
#            {"business_friendly_name":"FiscalYrMth","column_role":"","count_distinct":True,"criticality":"Critical","datatype":"string","display_format":"","equivalents":"","filterable":False,"format":"string","group_by":True,"is_dttm":False,"max":False,"mean":False,"median":False,"min":False,"name":"FiscalYrMth","position":18,"preview_data":["2017-04","2018-04","2016-08","2017-08","2016-12","2017-12","2017-02","2018-02","2017-01","2018-01"],"std_dev":False,"sum":False},
#            {"business_friendly_name":"FiscalYrQtr","column_role":"","count_distinct":True,"criticality":"Critical","datatype":"numeric","display_format":"","equivalents":"","filterable":False,"format":"_.##","group_by":False,"is_dttm":False,"max":True,"mean":True,"median":True,"min":True,"name":"FiscalYrQtr","position":19,"preview_data":["2017-2","2018-2","2016-3","2017-3","2016-4","2017-4","2017-1","2018-1","2017-1","2018-1"],"std_dev":True,"sum":True},
#            {"business_friendly_name":"Country","column_role":"","count_distinct":True,"criticality":"Critical","datatype":"string","display_format":"","equivalents":"","filterable":False,"format":"string","group_by":True,"is_dttm":False,"max":False,"mean":False,"median":False,"min":False,"name":"Country","position":20,"preview_data":["USA","USA","USA","USA","USA","USA","USA","USA","USA","USA"],"std_dev":False,"sum":False},
#            {"business_friendly_name":"Region","column_role":"","count_distinct":True,"criticality":"Critical","datatype":"string","display_format":"","equivalents":"","filterable":False,"format":"string","group_by":True,"is_dttm":False,"max":False,"mean":False,"median":False,"min":False,"name":"Region","position":21,"preview_data":["North","North","North","North","North","North","North","North","North","North"],"std_dev":False,"sum":False}
#           ]

catalog = [
    {"business_friendly_name": "Market Code", "column_role": "", "count_distinct": True, "criticality": "Critical",
     "datatype": "string", "display_format": "", "equivalents": "", "filterable": False, "format": "string",
     "group_by": True, "is_dttm": False, "max": False, "mean": False, "median": False, "min": False,
     "name": "MarketCode", "position": 1,
     "preview_data": ["MUSXA", "WSSMR", "SWMTL", "WTFTL", "SFYCO", "ALLOS", "TUSXA", "HYVTL", "SSIND", "SINT6"],
     "std_dev": False, "sum": False},
    {"business_friendly_name": "Product Key", "column_role": "", "count_distinct": True, "criticality": "Critical",
     "datatype": "numeric", "display_format": "", "equivalents": "", "filterable": False, "format": "_.##",
     "group_by": False, "is_dttm": False, "max": True, "mean": True, "median": True, "min": True, "name": "ProductKey",
     "position": 2,
     "preview_data": ["72587177", "72588789", "73142780", "110952421", "72485964", "72587718", "77691738", "110795992",
                      "72569027", "72588153"], "std_dev": True, "sum": True},
    {"business_friendly_name": "Period Tag", "column_role": "", "count_distinct": True, "criticality": "Critical",
     "datatype": "string", "display_format": "", "equivalents": "", "filterable": False, "format": "string",
     "group_by": True, "is_dttm": False, "max": False, "mean": False, "median": False, "min": False,
     "name": "PeriodTag", "position": 3,
     "preview_data": ["MAR2914.1", "MAR2914.1", "MAR2914.1", "MAR2914.1", "MAR2214.1", "MAR2214.1", "MAR2214.1",
                      "MAR2214.1", "MAR1514.1", "MAR0814.1"], "std_dev": False, "sum": False},
    {"business_friendly_name": "Units", "column_role": "", "count_distinct": True, "criticality": "Critical",
     "datatype": "numeric", "display_format": "", "equivalents": "Volume", "filterable": False, "format": "_.##",
     "group_by": False, "is_dttm": False, "max": True, "mean": True, "median": True, "min": True, "name": "Units",
     "position": 4, "preview_data": ["169233.48", "5727", "2", "586", "22", "185", "3192.87", "97", "13702.89", "7.05"],
     "std_dev": True, "sum": True},
    {"business_friendly_name": "Sales", "column_role": "", "count_distinct": True, "criticality": "Critical",
     "datatype": "numeric", "display_format": "", "equivalents": "Value, Sales", "filterable": False, "format": "_.##",
     "group_by": False, "is_dttm": False, "max": True, "mean": True, "median": True, "min": True, "name": "Revenue",
     "position": 5,
     "preview_data": ["139770.35", "19973.68", "3.98", "2903.4", "54.78", "793.65", "11244.53", "79.93", "20115.3",
                      "28.06"], "std_dev": True, "sum": True},
    {"business_friendly_name": "% All Commodity Volume", "column_role": "", "count_distinct": True,
     "criticality": "Critical", "datatype": "numeric", "display_format": "", "equivalents": "%ACV", "filterable": False,
     "format": "_.##", "group_by": False, "is_dttm": False, "max": True, "mean": True, "median": True, "min": True,
     "name": "ACV", "position": 6,
     "preview_data": ["0.32", "0.99", "0.01", "0.12", "0.01", "0.74", "0.04", "0.14", "0.83", "0.09"], "std_dev": True,
     "sum": True},
    {"business_friendly_name": "Feat w/o Disp %ACV", "column_role": "", "count_distinct": True,
     "criticality": "Critical", "datatype": "numeric", "display_format": "", "equivalents": "", "filterable": False,
     "format": "_.##", "group_by": False, "is_dttm": False, "max": True, "mean": True, "median": True, "min": True,
     "name": "FeatwoDispACV", "position": 7, "preview_data": ["7.98", "0", "0", "0", "0", "0", "0", "0", "6.67", "0"],
     "std_dev": True, "sum": True},
    {"business_friendly_name": "Any Disp %ACV", "column_role": "", "count_distinct": True, "criticality": "Critical",
     "datatype": "numeric", "display_format": "", "equivalents": "", "filterable": False, "format": "_.##",
     "group_by": False, "is_dttm": False, "max": True, "mean": True, "median": True, "min": True, "name": "AnyDispACV",
     "position": 8, "preview_data": ["1.07", "6.56", "0.56", "0.13", "0", "0", "0.12", "0", "3.13", "0"],
     "std_dev": True, "sum": True},
    {"business_friendly_name": "Disp w/o Feat %ACV", "column_role": "", "count_distinct": True,
     "criticality": "Critical", "datatype": "numeric", "display_format": "", "equivalents": "", "filterable": False,
     "format": "_.##", "group_by": False, "is_dttm": False, "max": True, "mean": True, "median": True, "min": True,
     "name": "DispwoFeatACV", "position": 9,
     "preview_data": ["0.77", "6.56", "0.56", "0.13", "0", "0", "0.12", "0", "3.13", "0"], "std_dev": True,
     "sum": True},
    {"business_friendly_name": "Feat & Disp %ACV", "column_role": "", "count_distinct": True, "criticality": "Critical",
     "datatype": "numeric", "display_format": "", "equivalents": "", "filterable": False, "format": "_.##",
     "group_by": False, "is_dttm": False, "max": True, "mean": True, "median": True, "min": True, "name": "FeatDispACV",
     "position": 10, "preview_data": ["0.3", "0", "0", "0", "0", "0", "0", "0", "0", "0"], "std_dev": True,
     "sum": True},
    {"business_friendly_name": "Price Decr %ACV", "column_role": "", "count_distinct": True, "criticality": "Critical",
     "datatype": "numeric", "display_format": "", "equivalents": "", "filterable": False, "format": "_.##",
     "group_by": False, "is_dttm": False, "max": True, "mean": True, "median": True, "min": True,
     "name": "PriceDecrACV", "position": 11,
     "preview_data": ["8.62", "61.58", "0", "0", "0", "0", "0.47", "0", "7.76", "0"], "std_dev": True, "sum": True},
    {"business_friendly_name": "Any Price Decr %ACV", "column_role": "", "count_distinct": True,
     "criticality": "Critical", "datatype": "numeric", "display_format": "", "equivalents": "", "filterable": False,
     "format": "_.##", "group_by": False, "is_dttm": False, "max": True, "mean": True, "median": True, "min": True,
     "name": "AnyPriceDecrACV", "position": 12,
     "preview_data": ["15.16", "68.14", "0", "0", "0", "0", "0.47", "0", "16.37", "0"], "std_dev": True, "sum": True},
    {"business_friendly_name": "Period", "column_role": "", "count_distinct": True, "criticality": "Critical",
     "datatype": "date", "display_format": "", "equivalents": "Period", "filterable": False, "format": "dd/mm/yy",
     "group_by": True, "is_dttm": False, "max": False, "mean": False, "median": False, "min": False,
     "name": "PERIODDESCRIPTION", "position": 13,
     "preview_data": ["27/03/19", "27/03/19", "27/03/19", "27/03/19", "20/03/19", "20/03/19", "20/03/19", "20/03/19",
                      "13/03/19", "06/03/19"], "std_dev": False, "sum": False,
     "addon_columns": ["PERIODDESCRIPTIONWeek", "PERIODDESCRIPTIONMonth", "PERIODDESCRIPTIONQuarter",
                       "PERIODDESCRIPTIONYear"]},
    {"business_friendly_name": "SKU", "column_role": "", "count_distinct": True, "criticality": "Critical",
     "datatype": "numeric", "display_format": "", "equivalents": "SKU", "filterable": False, "format": "_.##",
     "group_by": False, "is_dttm": False, "max": True, "mean": True, "median": True, "min": True, "name": "PRODUCTCODE",
     "position": 14,
     "preview_data": ["2800034654", "8036824120", "80247553450", "2800099888", "4126900837", "6508232303", "7674007807",
                      "5025521300", "69802841131", "60900005"], "std_dev": True, "sum": True},
    {"business_friendly_name": "Product", "column_role": "", "count_distinct": True, "criticality": "Critical",
     "datatype": "string", "display_format": "", "equivalents": "'prod','product','products'", "filterable": False,
     "format": "string", "group_by": True, "is_dttm": False, "max": False, "mean": False, "median": False, "min": False,
     "name": "PRODUCT", "position": 15,
     "preview_data": ["DE MET'S TREASURES MILK", "RC CONFISERIE MILK", "WARNER HUDSON MILK", "NESTLE NEST EGGS",
                      "PALMER ORIGINAL MILK", "BOTTICELLI DARK TABLETS", "WHITMANS SAMPLER MILK", "RITTER SPORT MILK",
                      "BELFINE MILK", "MADELAINE MILK"], "std_dev": False, "sum": False},
    {"business_friendly_name": "Product Description", "column_role": "", "count_distinct": True,
     "criticality": "Critical", "datatype": "string", "display_format": "", "equivalents": "Product Description",
     "filterable": False, "format": "string", "group_by": True, "is_dttm": False, "max": False, "mean": False,
     "median": False, "min": False, "name": "PRODUCTLONGDESCRIPTION", "position": 16,
     "preview_data": ["DE MET'S TREASURES UNWRPPD PCS IC SNGL NS PKG 2.03 OUNCE",
                      "RC CONFISERIE WRPD PCS/TRFFL FC BXD AO NS PKG 4.5 OUNCE",
                      "WARNER HUDSON UNWRPPD PCS FC THTR BX NS PKG 5.25 OUNCE",
                      "NESTLE NEST EGGS SHP FC SP PCS WRPPD SEAS PKG 38 OUNCE",
                      "PALMER ORIGINAL SHP FC LYDWN PCS UNWRPPD SEAS PKG 9 OUNCE",
                      "BOTTICELLI TABLETS IC SNGL NS PKG 1.4 OUNCE",
                      "WHITMANS SAMPLER WRPD PCS/TRFFL FC GFT BX SEAS PKG 1.75 OUNCE",
                      "RITTER SPORT TABLETS FC LRG/GNT NS PKG 3.5 OUNCE",
                      "BELFINE SHP FC LYDWN PCS UNWRPPD SEAS PKG 2.625 OUNCE",
                      "MADELAINE WRPD PCS/TRFFL FC AO CNSMR VW PCK TYP NS PKG 1 COUNT"], "std_dev": False,
     "sum": False},
    {"business_friendly_name": "Manufacturer", "column_role": "", "count_distinct": True, "criticality": "Critical",
     "datatype": "string", "display_format": "", "equivalents": "Manufacturer, mfr", "filterable": False,
     "format": "string", "group_by": True, "is_dttm": False, "max": False, "mean": False, "median": False, "min": False,
     "name": "MANUFACTURER", "position": 17,
     "preview_data": ["YILDIZ HOLDING A.S.", "AO MANUFACTURERS", "AO MANUFACTURERS", "NESTLE", "RM PALMER CORP",
                      "AO MANUFACTURERS", "LINDT", "ALFRED RITTER GMBH & CO KG", "AO MANUFACTURERS",
                      "AO MANUFACTURERS"], "std_dev": False, "sum": False},
    {"business_friendly_name": "Brand", "column_role": "", "count_distinct": True, "criticality": "Critical",
     "datatype": "string", "display_format": "", "equivalents": "Brand, Brands", "filterable": False,
     "format": "string", "group_by": True, "is_dttm": False, "max": False, "mean": False, "median": False, "min": False,
     "name": "BRAND", "position": 18,
     "preview_data": ["DE MET'S BRAND", "AO BRANDS", "AO BRANDS", "NESTLE AO BRANDS", "PALMER AO BRANDS", "AO BRANDS",
                      "WHITMANS BRAND", "RITTER SPORT BRAND", "AO BRANDS", "AO BRANDS"], "std_dev": False,
     "sum": False},
    {"business_friendly_name": "Consumption", "column_role": "", "count_distinct": True, "criticality": "Critical",
     "datatype": "string", "display_format": "", "equivalents": "Consumption", "filterable": False, "format": "string",
     "group_by": True, "is_dttm": False, "max": False, "mean": False, "median": False, "min": False,
     "name": "CONSUMPTION", "position": 19,
     "preview_data": ["IC", "FC", "FC", "FC", "FC", "IC", "FC", "FC", "FC", "FC"], "std_dev": False, "sum": False},
    {"business_friendly_name": "Package Type", "column_role": "", "count_distinct": True, "criticality": "Critical",
     "datatype": "string", "display_format": "", "equivalents": "Packaging Type, Packaging", "filterable": False,
     "format": "string", "group_by": True, "is_dttm": False, "max": False, "mean": False, "median": False, "min": False,
     "name": "SEASONALPACKAGING", "position": 20,
     "preview_data": ["NON SEASONAL PKG", "NON SEASONAL PKG", "NON SEASONAL PKG", "SEASONAL PKG", "SEASONAL PKG",
                      "NON SEASONAL PKG", "SEASONAL PKG", "NON SEASONAL PKG", "SEASONAL PKG", "NON SEASONAL PKG"],
     "std_dev": False, "sum": False},
    {"business_friendly_name": "Package Sub_Type", "column_role": "", "count_distinct": True, "criticality": "Critical",
     "datatype": "string", "display_format": "", "equivalents": "Packaging subtype, packaging sub type",
     "filterable": False, "format": "string", "group_by": True, "is_dttm": False, "max": False, "mean": False,
     "median": False, "min": False, "name": "CNSMRVIEWPACKTYPE", "position": 21,
     "preview_data": ["SINGLE", "BOXED AO", "THEATER BOX", "SUP PIECES WRAPPED", "LAYDOWN PIECES UNWRAPPED", "SINGLE",
                      "GIFT BOX", "LARGE/GIANT", "LAYDOWN PIECES UNWRAPPED", "AO CONSUMER VIEW PACK TYPE"],
     "std_dev": False, "sum": False},
    {"business_friendly_name": "Package Size", "column_role": "", "count_distinct": True, "criticality": "Critical",
     "datatype": "string", "display_format": "", "equivalents": "Package Size, Size", "filterable": False,
     "format": "string", "group_by": True, "is_dttm": False, "max": False, "mean": False, "median": False, "min": False,
     "name": "FCPACKSIZE", "position": 22,
     "preview_data": ["NOT APPLICABLE IC", "AO FC PACK SIZE", "AO FC PACK SIZE", "X-LARGE", "SMALL",
                      "NOT APPLICABLE IC", "AO FC PACK SIZE", "AO FC PACK SIZE", "SMALL", "AO FC PACK SIZE"],
     "std_dev": False, "sum": False},
    {"business_friendly_name": "Chocolate Type", "column_role": "", "count_distinct": True, "criticality": "Critical",
     "datatype": "string", "display_format": "", "equivalents": "Chocolate Type", "filterable": False,
     "format": "string", "group_by": True, "is_dttm": False, "max": False, "mean": False, "median": False, "min": False,
     "name": "CHOCOLATETYPE", "position": 23,
     "preview_data": ["MILK", "MILK", "MILK", "MILK", "MILK", "DARK", "MILK", "MILK", "MILK", "MILK"], "std_dev": False,
     "sum": False},
    {"business_friendly_name": "Market", "column_role": "", "count_distinct": True, "criticality": "Critical",
     "datatype": "string", "display_format": "", "equivalents": "Market", "filterable": False, "format": "string",
     "group_by": True, "is_dttm": False, "max": False, "mean": False, "median": False, "min": False,
     "name": "MARKETDISPLAYNAME", "position": 24,
     "preview_data": ["Total US xAOC", "Walmart SC MS River Delta Div TA", "Spartan Wholesale Only TA",
                      "Walmart Total US TA", "Safeway Corp TA", "Albertsons Los Angeles Metro TA",
                      "Total US xAOC Incl Conv", "Hy-Vee Total TA", "SSIND", "SINT6"], "std_dev": False, "sum": False},
    {"business_friendly_name": "Price Tier", "column_role": "", "count_distinct": True, "criticality": "Critical",
     "datatype": "string", "display_format": "", "equivalents": "Price Tier", "filterable": False, "format": "string",
     "group_by": True, "is_dttm": False, "max": False, "mean": False, "median": False, "min": False,
     "name": "PriceTier", "position": 25,
     "preview_data": ["a.$0-$5", "a.$0-$5", "a.$0-$5", "a.$0-$5", "a.$0-$5", "a.$0-$5", "a.$0-$5", "a.$0-$5", "a.$0-$5",
                      "a.$0-$5"], "std_dev": False, "sum": False},
    {"business_friendly_name": "Category", "column_role": "", "count_distinct": True, "criticality": "Critical",
     "datatype": "string", "display_format": "", "equivalents": "Categories", "filterable": False, "format": "string",
     "group_by": True, "is_dttm": False, "max": False, "mean": False, "median": False, "min": False, "name": "Category",
     "position": 26, "preview_data": ["MILK", "MILK", "MILK", "MILK", "MILK", "DARK", "MILK", "MILK", "MILK", "MILK"],
     "std_dev": False, "sum": False},
    {"business_friendly_name": "Sub Category", "column_role": "", "count_distinct": True, "criticality": "Critical",
     "datatype": "string", "display_format": "", "equivalents": "Segment, segments, sub categories",
     "filterable": False, "format": "string", "group_by": True, "is_dttm": False, "max": False, "mean": False,
     "median": False, "min": False, "name": "SubCategory", "position": 27,
     "preview_data": ["SINGLE", "BOXED AO", "THEATER BOX", "SUP PIECES WRAPPED", "LAYDOWN PIECES UNWRAPPED", "SINGLE",
                      "GIFT BOX", "LARGE/GIANT", "LAYDOWN PIECES UNWRAPPED", "AO CONSUMER VIEW PACK TYPE"],
     "std_dev": False, "sum": False},
    {"business_friendly_name": "Channel", "column_role": "", "count_distinct": True, "criticality": "Critical",
     "datatype": "string", "display_format": "", "equivalents": "Channels", "filterable": False, "format": "string",
     "group_by": True, "is_dttm": False, "max": False, "mean": False, "median": False, "min": False, "name": "Channel",
     "position": 28,
     "preview_data": ["Club", "Convenience", "Misc", "Military", "Military", "Convenience", "Military", "Food", "Club",
                      "Misc"], "std_dev": False, "sum": False},
    {"business_friendly_name": "Average Items Carried", "column_role": "", "count_distinct": True,
     "criticality": "Critical", "datatype": "numeric", "display_format": "", "equivalents": "AIC", "filterable": False,
     "format": "_.##", "group_by": False, "is_dttm": False, "max": True, "mean": True, "median": True, "min": True,
     "name": "AIC", "position": 29, "preview_data": ["1", "4", "3", "3", "4", "1", "5", "1", "4", "4"], "std_dev": True,
     "sum": True},
    {"business_friendly_name": "Price", "column_role": "", "count_distinct": True, "criticality": "Critical",
     "datatype": "numeric", "display_format": "", "equivalents": "", "filterable": False, "format": "_.##",
     "group_by": False, "is_dttm": False, "max": True, "mean": True, "median": True, "min": True, "name": "Price",
     "position": 30, "preview_data": ["0.83", "3.49", "1.99", "4.95", "2.49", "4.29", "3.52", "0.82", "1.47", "3.98"],
     "std_dev": True, "sum": True},
    {"business_friendly_name": "Total Distribution Points", "column_role": "", "count_distinct": True,
     "criticality": "Critical", "datatype": "numeric", "display_format": "", "equivalents": "TDP", "filterable": False,
     "format": "_.##", "group_by": False, "is_dttm": False, "max": True, "mean": True, "median": True, "min": True,
     "name": "TDP", "position": 31, "preview_data": ["32", "396", "3", "36", "4", "74", "20", "14", "332", "36"],
     "std_dev": True, "sum": True}]

default_date_flag = 'PERIODDESCRIPTION'

# default_date_flag = 'Date'

qr_li = ["What is the total Sales and units for the last 12 months",
    "Compare sales and units for 2017, 2018 and 2019",
    "Get me the distribution of sales by channel and category for the last 12 months",
    "Get me the sales by channel and category for the last 12 months",
    "Can we get the YoY % change in sales by Category",
    "Compare sales and units for dark, milk and mixed chocolates",
    "Show me the sales distribution by price tier for the last 12 months",
    "Show me the units distribution by price tier for the last 12 months",
    "Get me the market share by manufacturer for the last 12 months",
    "Sales growth by manufacturer",
    "Get me the market share by price tier for the last 12 months",
    "Can we have the sales trend by mfr for the last 12 months",
    "Get the top 5 brands by average TDP for the last 12 months"]


# query = "top 10 manufacturer by revenue for 2018 and 2019"
# query = "market share by price tier"
# query = "show me sales by channel for the last 3 years"
query = "Can we get the YoY % change in sales by Category"
def dim_value_mapping_map(df):
    dim_value_mapping = {}
    for col in df.columns:
        if df[col].dtype == "object":
            dim_value_mapping.update({col: df[col].unique()})
    return dim_value_mapping

dv = dim_value_mapping_map(data)
start = time.time()
query_result = NLP_PARSER.nlp_parser(query, catalog, default_date_flag)
print('\n', query, '\n\n', query_result, '\n')
result = QUERY_PARSER.sql_query_generator(query_result, catalog, default_date_flag,dv)
print(result,'\n')
# data_res = QUERY_EXECUTOR.query_executor(result, data, default_date_flag, catalog, query_result)
# print(data_res,'\n')
sugg = SUGGESTIONS.return_suggestions(query_result, catalog, default_date_flag,dv)
print('Suggestions : \t',sugg)
end = time.time()
print('\n', 'Execution Time:', end - start)

# sugg_dict = {}
# for i in qr_li:
#     start = time.time()
#     query_result = NLP_PARSER.nlp_parser(i, catalog, default_date_flag)
#     print('\n', i, '\n\n', query_result, '\n')
#     result = QUERY_PARSER.sql_query_generator(query_result, catalog, default_date_flag, dv)
#     print(result, '\n')
#     # data_res = QUERY_EXECUTOR.query_executor(result, data, default_date_flag, catalog, query_result)
#     # print(data_res, '\n')
#     sugg_dict[i] = SUGGESTIONS.return_suggestions(query_result, catalog, default_date_flag, dv)
#     # print('Suggestions : \t', sugg)
#     end = time.time()
#     print('\n', 'Execution Time:', end - start)
#
# print("Queries and Suggestions: \n{}".format(sugg_dict))
# # with open("temp.csv","w") as f:
#     f=csv.DictWriter()
#     f.writerow(sugg)
#     f.clos
# pd.DataFrame(sugg_dict).to_dict("temp.csv")
# queries = pd.read_csv('/Volumes/D-Drive/queries.csv')
# # print(queries['queres'])
# i = 1
# for each in queries['queries'][:]:
#     print(i,'\t', each)
#     query_result = NLP_PARSER.nlp_parser(each, catalog, default_date_flag)
#     print(query_result)
#     result = QUERY_PARSER.sql_query_generator(query_result, catalog, default_date_flag)
#     print(result)
#     data_res = QUERY_EXECUTOR.query_executor(result, data, default_date_flag, catalog, query_result)
#     print(data_res)
#     sugg = SUGGESTIONS.return_suggestions(query_result, catalog, default_date_flag)
#     print(sugg)
#     print('\n')
#     i=i+1
