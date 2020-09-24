import csv, re, time, warnings, argparse, sys
import pandas as pd
import numpy as np 

### This script should be used to migrate from the raw results
### file, generated using the ConvertXMLtoFinalCSV.exe script, 
### to a more user-friendly format that can be opened and manipulated
### easily within Excel. 
### Conversion: CSV -> CSV

### Last edited by: Kevin Ludwig, 10/01/2018

""" Ignore annoying warnings. """
warnings.simplefilter('ignore')

""" Enable large CSV files to be read. """
maxInt    = sys.maxsize
sizeError = True
while sizeError:
    sizeError = False
    try:
        csv.field_size_limit(maxInt)
    except OverflowError:
        maxInt    = int(maxInt/5)
        sizeError = True


def _print_line(line, stageNum, spaceChar):
    
    """
    Prints the lines indicating what stage 
    of the process is being completed.
    """

    string = '{:<%s}{:%s>%s}' % (0, spaceChar, 70-len(line))
    print(string.format(line, 'stage ' + str(stageNum) + '/8'))


def _read_results(document):
    
    """
    Read in the results document using a CSV
    reader object.
    """
    
    _print_line('Reading results file', 1, '')
    try:
        file = open(document, 'r')
    except:
        print('\r\nERROR: This is not a valid document. '
              'Please check your input and try again.')
        sys.exit(1)
    reader = csv.reader(file, delimiter=',')
    data   = []
    for row in reader:
        data.append(row)
    file.close()
    return data


def _remove_scoring_section(data):
    
    """
    This removes the scoring section from
    the results CSV, which is not used
    in later stages of the analysis.
    """
    
    _print_line('Removing scoring section', 2, '')
    newData      = []
    firstSection = False
    lineNum      = 0
    while not firstSection:
        try:
            data[lineNum]
        except:
            print('\r\nERROR: This document might not be from '
                  'the correct stage in the results process. '
                  '\r\nConfirm this is from the stage after '
                  'using the ConvertXMLtoFinalCSV.exe script.')
            sys.exit(1)
        if len(data[lineNum]) == 0:
            firstSection = True
        lineNum += 1
    for line in data[lineNum:]:
        newData.append(line)
    return newData
        

def _create_dict_with_event_type(data):
    
    """
    This breaks the results into chunks,
    each chunk representing the results
    from a specific event type. The 
    event type is stored as a dictionary
    key, with the value being the results
    chunk corresponding to that event
    type.
    """
    
    _print_line('Splitting results by event type', 3, '')
    newData   = {}
    eventType = ''
    for line in data:
        if len(line) == 1 and '-----' not in line[0]:
            eventType = line[0]
            newData[eventType] = []
        elif len(line) > 1:
            newData[eventType].append(line)
    return newData


def _create_df_for_each_event_type(dataDict):
    
    """
    Migrates the results chunks for each
    event type into dataframes. Again, the
    dictionary key is the event type and the
    value is the dataframe. Each dataframe is
    then given a new column, which contains
    the event type as its value. The column
    is located in the second position (after
    'Main Company').
    """
    
    newData = {}
    for et, data in dataDict.items():
        newData[et]               = pd.DataFrame(columns=data[0], data=data[1:]).drop(columns=[''])
        newData[et]['Event Type'] = et
    return newData
    

def _merge_dataframes(dataDict):
    
    """
    Merges all of the individual event
    type dataframes into one large dataframe.
    Contains language to avoid issues that
    arrise when individual event types have 
    duplicate slot columns.
    """
    
    _print_line('Merging event types into single dataframe', 4, '')
    newData = pd.DataFrame()
    for et, data in dataDict.items():
        data.rename(columns={data.columns[1]: 'Article Date'}, inplace=True)
        colNames = pd.Series(data.columns)
        for dup in data.columns[data.columns.duplicated()].unique():
            i = 0
            for d_idx in range(len(data.columns.values)):
                if data.columns.get_loc(dup)[d_idx]:
                    if i > 0:
                        colNames[d_idx] = colNames[d_idx] + '_'+ str(i)
                    i += 1
        data.columns = colNames
        newData = newData.append(data, sort=False, ignore_index=True)
    newData.replace(r'^\s*$', pd.np.nan, regex=True, inplace=True)
    #print(newData.apply(lambda x: x.isnull().sum(), axis='rows'))
    for column in newData.filter(regex=(r'_[0-9]+$')).columns:
        baseET = re.findall(r'^([^_]*)_[0-9]+$', column)[0]
        newData[baseET].fillna(newData[column], inplace=True)
        
        newData.loc[newData[column] == newData[baseET], column] = pd.np.nan
    #print(newData.apply(lambda x: x.isnull().sum(), axis='rows'))
    newData = newData.rename(columns=lambda x: re.sub(r'_[0-9]+$', '', x))
    goodCols = ['Main Company','Article Date','Event Type','Event Target','Extraction','Polarity','Other','Sentence','URL','Sentence(Inc. Annotations)']
    optionCols = ['Event Target','Extraction','Polarity']
    for option in optionCols:
        if option not in newData.columns.values:
            goodCols.remove(option)
    if 'Other' not in newData.columns.values:
        goodCols.remove('Other')
    splitNum = 3 + len(optionCols)
    newData = newData[goodCols[:splitNum] + [name for name in newData if name not in goodCols] + goodCols[splitNum:]]
    #print(newData.apply(lambda x: x.isnull().sum(), axis='rows'))
    return newData


def _add_neutral_polarity(data):
    
    """
    Adds the string 'NEUTRAL' to the polarity
    column for all extractions of neutral
    polarity; currently, those cells are
    simply left blank.
    """
    
    _print_line('Cleaning data', 5, '')
    try:
        data['Polarity']
    except KeyError:
        return data
    except:
        print('\r\nERROR: Results look empty...')
        sys.exit(1)
    print(type(data['Polarity'][0]))
    # data['Polarity'] = data['Polarity'].str.replace(r'^$', r'NEUTRAL', regex=True)
    #data['Polarity'] = 'NEUTRAL'
    return data


def _remove_dashes_from_sentence_start(data):
    
    """
    Sentences that start with at least one
    '-' produce an error when viewed in 
    Excel, where the value appears as "#NAME".
    This would remove those dashes (which
    are unnecessary to the structure of
    the data) in order to avoid this
    """
    
    data['Sentence'] = data['Sentence'].str.replace(r'^[ ]*(-)+[ ]*', '', regex=True)
    return data
    

def _pull_other_entity_values(data, option):
    
    """
    Pulls out any missing slot values
    from the 'Other' column into their
    respective columns iff that cell
    was blank. Temporarily renames 
    identical column names with _X, where
    X = column index, then replaces the
    names back to their original form.
    """
    
    if not option:
        _print_line('SKIPPING "Other" column values', 6, ':')
        return data
    _print_line('Pulling extra slot values from "Other" column', 6, '')
    colNames = pd.Series(data.columns)
    for dup in data.columns[data.columns.duplicated()].unique():
        print(dup)
        colNames[data.columns.get_loc(dup)] = [dup+'_'+str(d_idx) if d_idx!=0 
                 else dup for d_idx in range(len(data.columns.values))]
    data.columns = colNames
    startIdx     = ['Polarity', 'Extraction', 'Event Target', 'Event Type']
    for option in startIdx:
        if option not in data.columns.values:
            startIdx.remove(option)
    polIdx = colNames[colNames == startIdx[0]].index[0]
    try:
        otherIdx = colNames[colNames == 'Other'].index[0]
    except KeyError:
        print('\r\nERROR: There is no "Other" column...')
        data = data.rename(columns=lambda x: re.sub(r'_[0-9]+$', '', x))
        return data
    for column in colNames[polIdx+1:otherIdx]:
        colRegex = re.findall(r'([A-Za-z]+)[_0-9]*$', column)[0]
        regex = re.compile(colRegex + ':"([^"]*)"')
        try:
            data[column] = pd.np.where(data['Other'].str.contains(regex) 
                            & data[column].str.contains(r'^$'), 
                            data['Other'].str.extract(regex, expand=False), data[column])
        except AttributeError:
            continue
    data = data.rename(columns=lambda x: re.sub(r'_[0-9]+$', '', x))
    return data


def _delete_blank_columns(data):
    
    """
    Deletes slot columns that
    contain no slot values, as well
    as slot values that are repeats
    in the same extraction row.
    """
    
    _print_line('Deleting blank columns and duplicate slot values', 7, '')
    data.replace('', pd.np.nan, inplace=True)
    data.dropna(axis='columns', how='all', inplace=True)
    return data


def _write_document(data, document):
    
    """
    Writes the final document. It uses the
    same name as the original results, plus
    '_final' at the end.
    """
    
    _print_line('Writing document', 8, '')
    document = document[:-4] + '_final.csv'
    data.to_csv(document, index=False, quoting=csv.QUOTE_ALL)
    
    
    
##############################################################################
##############################################################################
##############################################################################
    
    
    
if __name__ == '__main__':
    
    parser    = argparse.ArgumentParser(description='Converts the results CSV file into a more user-friendly CSV file.')
    parser.add_argument('Document', help='The results document to convert (.csv).')
    parser.add_argument('-pullothervalues', action='store_true', help='Pull slot values from "Other" column to their respective columns (defualt FALSE).')
    args      = parser.parse_args()
    
    startTime = time.perf_counter()
    
    data      = _read_results(args.Document)                            # read data from document
    data      = _remove_scoring_section(data)                           # remove the top scoring data
    data      = _create_dict_with_event_type(data)                      # break results into chunks corresponding to event type
    data      = _create_df_for_each_event_type(data)                    # convert from array to pandas DataFrame for each event type
    data      = _merge_dataframes(data)                                 # merge all individual DataFrames into one
    data      = _add_neutral_polarity(data)                             # adds 'NEUTRAL' to the 'Polarity' column
    data      = _remove_dashes_from_sentence_start(data)                # removes "-"s from the beginning of sentences
    data      = _pull_other_entity_values(data, args.pullothervalues)   # pull slot values from 'Other' column
    data      = _delete_blank_columns(data)                             # delete slot columns containing no values 
    #print(data.apply(lambda x: x.isnull().sum(), axis='rows'))
    _write_document(data, args.Document)                                # write final document
    
    print(f'\r\nAll done; finished in {str(round(time.perf_counter() - startTime,2))} seconds.')