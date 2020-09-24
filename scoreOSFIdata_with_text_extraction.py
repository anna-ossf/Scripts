import pandas as pd
import re, csv, argparse, sys, ast
from lxml import etree
from itertools import chain

"""

This document is used to go from the final output stage from the VIP
batch process to a scoring system broken down by Company, Date, and
topic for OpenSesafi.

Originally created: Kevin Ludwig, 10/8/2018
Last updated: Kevin Ludwig, 12/17/2018

"""


def _read(document):
    
    """
    Read in the data.
    """
    
    print('Reading data...')
    return pd.read_csv(document, low_memory=False)
    

def _init_topics():
    
    """
    Defines the unique topics
    that must be found in the 
    results.
    """

    print('Defining unique topics...')
    return ['topic_3D_printing','topic_agriculture','topic_artificial_intelligence','topic_babyboomers','topic_big_data','topic_bitcoin','topic_bitcoin_cash','topic_blockchain','topic_buybacks','topic_cars','topic_catholic','topic_clean_energy','topic_cleantech','topic_climate_change','topic_cloud_computing','topic_coal','topic_coins','topic_computer_vision','topic_consumerpreference','topic_cybersecurity','topic_demographics','topic_development','topic_drip_irrigation','topic_driverless_cars','topic_drone','topic_ecommerce','topic_electric_vehicle','topic_employee_treatment','topic_entrepreneurship','topic_environmental','topic_esg','topic_ethereum','topic_financial','topic_fintech','topic_food','topic_fossil_free','topic_gaming','topic_gender','topic_genx','topic_geothermal','topic_global_goals','topic_governance','topic_health_tech','topic_home_ownership','topic_human_rights','topic_hydro','topic_immunotherapy','topic_insurtech','topic_iot','topic_ipo','topic_leisure','topic_lendingplatforms','topic_lgbt','topic_logistics','topic_luxury','topic_manufactring','topic_marijuana','topic_medical_devices','topic_military','topic_military_spending','topic_millenials','topic_mri','topic_music','topic_natural_language_processing','topic_nuclear','topic_organic','topic_outsourcing','topic_payments','topic_pets','topic_philantropy','topic_precision_agriculture','topic_qsr','topic_renewable_energy','topic_ripple','topic_roboadvisors','topic_robotics','topic_seniors','topic_sensors','topic_shale','topic_social','topic_social_media','topic_solar_energy','topic_spinoffs','topic_sports','topic_stemcells','topic_streaming','topic_tech','topic_travel','topic_vice','topic_virtual_reality','topic_water','topic_wealthtech','topic_wearable_tech','topic_wellness','topic_wind_energy','topic_women']


def _add_topics_columns(data, topics):
    
    """
    Add a column for each
    topic mentioned in the
    results. Each is assigned
    a score of 0 to begin.
    """
    
    print('Addings unique topics as column headers...')
    for topic in topics:
        data[topic] = 0
    return data


def _format_xmls_correctly(data):
    
    """
    This correctly formats
    the XML data to be parsed
    by the lxml package.
    """
    
    print('Formatting XML language...')
    data['Sentence(Inc. Annotations)'] = data['Sentence(Inc. Annotations)'].str.replace(r"(=)([a-z0-9_]+)", r"\1'\2'")
    data['Sentence(Inc. Annotations)'] = data['Sentence(Inc. Annotations)'].str.replace(r'&', '&amp;')
    data['Sentence(Inc. Annotations)'] = data['Sentence(Inc. Annotations)'].str.replace(r'<\/?sa\.irrelevant\.rule>', '')
    return data
    

def _stringify_children(node):
    
    """
    Used in the _add_scores_to_topic_columns
    function. Turns the extraction XML
    data into a single string, regardless
    of whether there are slots (or other
    tagged data) within the string.
    """
    
    parts = ([node.text] + list(chain(*([etree.tostring(c, encoding=str, with_tail=False), c.tail] for c in node.getchildren()))))
    return ''.join(filter(None, parts))


def _add_scores_to_topic_columns(data, docType):
    
    """
    Matches up the extraction to the correct
    extraction in the XML data, and adds
    a 1 in the corresponding column. Must
    be used to avoid double-counting (as 
    many sentences contain more than one
    extraction, but each extraction has 
    its own row).
    """
    
    print('Addings scores to topic columns...')
    if docType.lower() == 'batch':
        for index, row in data.iterrows():
            xml = row['Sentence(Inc. Annotations)']
            try:
                tree = etree.fromstring(xml).xpath("//*[starts-with(local-name(), 'TRUEEVENT')]")
            except:
                print('\r\nERROR in XML for document {}'.format(str(index)))
                sys.exit(1)
            for elem in tree:
                extraction = _stringify_children(elem)
                extraction = re.sub('<[^>]+>', '', extraction)
                if extraction == data.loc[index, 'Extraction']:
                    #topics = [key for key, value in elem.attrib.items() if re.search(r'prop_topic_[^=]*="?sentence', key)]
                    topics = [key for key, value in elem.attrib.items() if ('prop_topic_' in key and value == 'sentence')]
                    for topic in set(topics):
                        topic = re.sub('prop_', '', topic)
                        data.loc[index, topic] += 1
    else:
        for index, row in data.iterrows():
            props = [key for x in ast.literal_eval(row['properties']) for key in x.keys() if re.search(r'^topic_', key)]
            for topic in props:
                data.loc[index, topic] += 1
    return data


def _add_weights_to_scores(data, topics, docType):
    
    """
    Multiples the counts in each
    topic column by the default weight
    for normal extractions, then
    adjusts for if it is a catch rule, 
    forecast, etc.
    """
    
    print('Adding weights to topic scores...')
    scoreDict = {
            'Catch': 0.002,
            'ELSE': 1,
            'Investment Objective': 3,
            'Principle Strategy': 2,
            }
    for topic in topics:
        data[topic] = data[topic]*scoreDict['ELSE']
        if docType.lower() == 'batch':
            colName = 'Sentence(Inc. Annotations)'
        else:
            colName = 'eventName'
        data[topic] = pd.np.where(data[colName].str.contains(r'(?i)_Catch', regex=True), data[topic]*scoreDict['Catch'], data[topic])
        # data[topic] = pd.np.where(data[colName].str.contains(r'(?i)prop_forecast', regex=True), data[topic]*scoreDict['Forecast'], data[topic])
        data[topic] = pd.np.where(data[colName].str.contains(r'(?i)prop_investmentobj', regex=True), data[topic]*scoreDict['Investment Objective'], data[topic])
        data[topic] = pd.np.where(data[colName].str.contains(r'(?i)prop_principlestrat', regex=True), data[topic]*scoreDict['Principle Strategy'], data[topic])
    return data


def _aggregate_scores(data, topics, docType):
    
    """
    Aggregates the scores by 
    organization and date (so
    each publication gets its own
    score).
    """
    
    print('Aggregating scores...')
    if docType.lower() == 'batch':
        firstCols = ['Main Company', 'Article Date']
        sortCols  = ['Main Company', 'Article Date']
    else:
        firstCols = ['companyName', 'ticker', 'eventDate']
        sortCols  = ['ticker', 'eventDate']
    columns = firstCols + sorted(topics, key=str.lower)
    try:
        data = data[columns]
        data = data.groupby(sortCols, as_index=False).sum()
    except KeyError:
        columns.remove('Ticker')
        sortCols.remove('Ticker')
        data = data[columns]
        data = data.groupby(sortCols, as_index=False).sum()
    return data


def _normalize_scores(data, topics):
    
    """
    Normalizes each column so
    each score is between 0 and 1.
    """
    
    print('Normalizing data...')
    for col in data.columns[2:]:
        data[col] = (data[col] - min(data[col]))/(max(data[col]) - min(data[col]))
    return data


def _uppercase_regexp_groups(match):
    
    """
    Used in the _clean_topic_names
    function to uppercase some
    match groups.
    """
    
    return match.group(1).upper() + match.group(2) + match.group(3).upper() + match.group(4)


def _clean_topic_names(data):
    
    """
    Cleans the topic name columns
    so they are not all lowercase.
    """
    
    print('Cleaning topic names...')
    data = data.rename(columns=lambda x: re.sub(r'^topic_(.*)$', r'\1', x))
    data = data.drop(['financial', 'consumerpreference'], axis=1)
    topicMapping = {'ai': 'artificial_intelligence',
                    'babyboomers': 'baby_boomers',
                    'bigdata': 'big_data',
                    'bitcoincash': 'bitcoin_cash',
                    'cleanenergy': 'clean_energy',
                    'climatechange': 'climate_change',
                    'cloud': 'cloud_computing',
                    'computervision': 'computer_vision',
                    'driverlessvehicle': 'driverless_cars',
                    'electricvehicle': 'electric',
                    'employeetreatment': 'employee_treatment',
                    'fossilfree': 'fossil_free',
                    'genx': 'gen_x',
                    'globalgoals': 'global_goals ',
                    'healthtech': 'health_tech',
                    'homeownership': 'home_ownership',
                    'humanrights': 'human_rights',
                    'ipo': 'IPOs',
                    'lendingplatforms': 'lending_platforms',
                    'lgbtq': 'lgbt',
                    'medicaldevices': 'medical_devices',
                    'nlp': 'natural_language_processing',
                    'philanthropy': 'philantrophy',
                    'renewableenergy': 'renewable_energy',
                    'socialmedia': 'social_media',
                    'solarenergy': 'solar_energy',
                    'stemcells': 'stemcell',
                    'virtualreality': 'virtual_reality',
                    'wearabletech': 'wearable_tech',
                    'windenergy': 'wind_energy'
                    }
    data = data.rename(columns=lambda x: topicMapping[x] if x in topicMapping else x)
    return data


def _write(data, aggCheck, doc, add_text=False):
    
    """
    Writes both the normal score file
    (which is just the results with
    a tally of each topic contained
    in the extraction in new columns
    on the right) and the aggregated
    score file.
    """
    
    print('Writing new documents...')
    document = doc[:-4] + '_wScoring.csv'
    if aggCheck:
        document = doc[:-4] + '_wAggregateScoring.csv'
    if add_text:
        document = doc[:-4] + '_wAggregateScoring_with_Text_Extraction.csv'
    data.to_csv(document, index=False, quoting=csv.QUOTE_ALL)

def _add_text_extraction(aggData,data):
    """
    write to add text extraction 
    for themes which has theme score
    created: Zhen Lu 1/9/2019
    """
    
    print ("Adding text extraction...")
    aggData2 = aggData.set_index('Main Company',drop=False)
    theme_columns = list(aggData.columns[2:])
    text_columns = [column+'_text' for column in theme_columns]
    for column in text_columns:
        aggData2[column] = ''
    for ticker in aggData['Main Company']:
        for column in theme_columns:
            if aggData2.at[ticker,column] > 0:
                text_column = column+'_text'
                aggData2.at[ticker,text_column] = data[data['Main Company']==ticker].sort_values(by=[column],ascending=False).iloc[0]['Sentence']
    return aggData2

##########################################################################
##########################################################################
##########################################################################
    
    

if __name__ == '__main__':
    parser   = argparse.ArgumentParser(description='Scores OpenSeSaFi documents to produce their final output.')
    parser.add_argument('document', help='The results document to score (.csv).')
    parser.add_argument('resultsType', help='"Batch" or "API"')
    args     = parser.parse_args()
    doc      = args.document
    docType  = args.resultsType
    
    data     = _read(doc)
    topics   = _init_topics()
    data     = _add_topics_columns(data, topics)
    if docType.lower() == 'batch':
        data = _format_xmls_correctly(data)
    data     = _add_scores_to_topic_columns(data, docType)
    data     = _add_weights_to_scores(data, topics, docType)
    aggData  = _aggregate_scores(data, topics, docType)
    aggData  = _normalize_scores(aggData, topics)
    data     = _clean_topic_names(data)
    aggData  = _clean_topic_names(aggData)
    aggData_with_text = _add_text_extraction(aggData,data)
    _write(data, False, doc)
    _write(aggData, True, doc)
    _write(aggData_with_text, True, doc, True)
    print('\r\nDONE!')