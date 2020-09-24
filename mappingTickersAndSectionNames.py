import pandas as pd
import csv, argparse, re

def _read(document, docType):
    print('Reading...')
    if docType.lower() == 'corpus':
        return pd.read_csv(document, low_memory=True, dtype=str, usecols=['organization_name', 'ticker'])
    else:
        return pd.read_csv(document, low_memory=True, dtype=str)

def _map_tickers(results, tickers):
    print('Mapping tickers...')
    results['Ticker'] = pd.merge(results, tickers, left_on=['Main Company'], right_on=['organization_name'])['ticker']
    return results

def _extract_section(data):
    print('Mapping sections...')
    for idx, row in data.iterrows():
        event  = re.sub(row['Event Type'], r'[ ]+', '_')
        regexp = re.compile(r'EVENT_' + event + r'[^>]*prop_SectionName="([^"]+)"')
        data.loc[idx, 'Section Name'] = re.search(regexp, row['Sentence(Inc. Annotations)'])
    return data

def _write(document, data):
    print('Writing...')
    data.to_csv(document, index=False, quoting=csv.QUOTE_ALL)
    

if __name__ == '__main__':
    args       = argparse.ArgumentParser(description='Adds tickers and section names to OpenSeSaFi results.')
    args.add_argument('--results', dest='ResultsDoc', required=True, help='Final results document (after macro).')
    args.add_argument('--corpus1', dest='CorpusDoc1', required=True, help='First corpus document.')
    #args.add_argument('--corpus2', dest='CorpusDoc2', required=True, help='Second corpus document.')
    params     = args.parse_args()
    resultsDoc = params.ResultsDoc
    corpusDoc1 = params.CorpusDoc1
    #corpusDoc2 = params.CorpusDoc2
    
    resultsData = _read(resultsDoc, 'Results')
    corpusData1 = _read(corpusDoc1, 'Corpus')
    #corpusData2 = _read(corpusDoc2, 'Corpus')
    tickerData  = corpusData1.drop_duplicates().reset_index(drop=True)
    #tickerData  = corpusData1.append(corpusData2).drop_duplicates().reset_index(drop=True)
    
    resultsData = _map_tickers(resultsData, tickerData)
    resultsData = _extract_section(resultsData)
    _write('{}_wTickers.csv'.format(resultsDoc[:-4]), resultsData)
    