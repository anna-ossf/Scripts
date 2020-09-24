# -*- coding: utf-8 -*-
"""
Created on Tue Oct 22 16:53:08 2019

@author: Annapoorani

Extracted fund name and desc from security master and adding in prospectus corpus
"""

## ---(Tue Oct 22 16:51:16 2019)---
import pandas as pd
import csv
import argparse

#corp_etf = pd.read_csv(r"C:\Users\Opensesafi\Documents\VIPProjects\STP_Corpus\corp_2456.csv")
#corp_mf = pd.read_csv(r"C:\Users\Opensesafi\Documents\VIPProjects\STP_Corpus\mf_corp_6426_new.csv")
#folder_path = r"C:\Users\Opensesafi\Documents\VIPProjects\STP_Corpus\\"

def _read(document):
    
    """
    Read in the data.
    """
    
    print('Reading data...')
    return pd.read_csv(document, low_memory=False)

def _add_fund_name(data):
    
    """
    Adds the meta data - name and desc 
    in the first line of the article
    text. 
    """
    all_names = pd.read_csv(r"C:\Users\Opensesafi\Documents\VIPProjects\STP_Corpus\ticker_name_desc.csv")    
    ticker_list = data.organization_name.str.replace("\[TICKER\]","").tolist()    
    data.set_index('organization_name',inplace = True)
    all_names.set_index('ticker',inplace = True)    
    for t in ticker_list:
        if t in all_names.index:
            x = "[TICKER]"+t
            data.at[x,'article_text'] = "short_name "+ str(all_names.at[t,'short_name']) + "\n"+ "short_desc "+ str(all_names.at[t,'short_desc']) +" "+ str(data.at[x,'article_text'])
    return data


'''
ETF Corpus
'''
data     = _read(r"C:\Users\Opensesafi\Documents\Python Scripts\Missing prospectus\missing_2020_filelist_VIP_CSV_2020_09_22T10-16_strictclean_processed.csv")
data     = _add_fund_name(data)
data.to_csv(r"C:\Users\Opensesafi\Documents\VIPProjects\STP_Corpus\2020_2532.csv", quoting = csv.QUOTE_ALL)

'''
MF Corpus
'''
data     = _read(r"C:\Users\Opensesafi\Documents\VIPProjects\STP_Corpus\mf_corp_6426_new.csv")
data     = _add_fund_name(data)
data.to_csv(r"C:\Users\Opensesafi\Documents\VIPProjects\STP_Corpus\mf_corp_w_name.csv")

print('\r\nDONE!')