import csv, argparse, sys, re, os
        
maxInt = sys.maxsize
decrement = True
while decrement:
    decrement = False
    try:
        csv.field_size_limit(maxInt)
    except OverflowError:
        maxInt = int(maxInt/20)
        decrement = True
            
def _clean_slashes(fileName):
    fileName = os.path.normpath(fileName)
    fileName = re.escape(fileName)
    fileName = re.sub(r'(\\)(\.)', r'\2', fileName)
    if not re.search('.csv', fileName):
        fileName += r'\\'
    return fileName
    
def _read(document):
    xmlData = []
    r = csv.reader(open(document, 'r', newline=''), delimiter=',')
    for row in r:
        #if len(row) != 0:
        xmlData.append(row[6])
    return xmlData

def _clean(data):
    data = data.replace('&lt;', '<')
    data = data.replace('&gt;', '>')
    data = data.replace('&quot;', '"')
    data = re.sub(r'=([0-9_.-]+)(>| )', r'="\1"\2', data)
    data = re.sub(r'(negated=|future=|modal=|prop__[a-z]+=)(true|false)', r'\1"\2"', data)
    return data
    
def _find_all_ets(data, eventTypes):
    ets = re.findall(r'<[TRUE]*EVENT_[^ ]+', data)
    for et in ets:
        if et[0:4] == '<TRU':
            et = et[11:]
        else:
            et = et[7:]
        et = re.sub(r'_(N|P|X)$', '', et)
        et = et.replace('_', ' ')
        if et not in eventTypes:
            eventTypes.append(et)
    return eventTypes

def _write_et(directory, eventTypes, tag):
    document = directory + '\eventtypes' + tag + '.txt'
    slots    = ', metadata_analyst, metadata_orgid, metadata_organization, metadata_hyperlink, metadata_domicile, metadata_region, metadata_broad, metadata_city, metadata_date, metadata_specific, SectionName\r\n'
    with open(document, 'w') as doc:
        for et in eventTypes:
            doc.write(et + slots)
    
def _main(document, directory, tag):
    print('Reading results document...')
    allXmls    = _read(document)
    eventTypes = []
    for xmlNum, xml in enumerate(allXmls):
        print(f'Working on document {str(xmlNum + 1)} out of {str(len(allXmls))}')
        xml = _clean(xml)
        eventTypes = _find_all_ets(xml, eventTypes)
    _write_et(directory, eventTypes, tag)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Makes an eventtypes.txt based on all events in results file.')
    parser.add_argument('--document', type=str, help='the results file.', required=True)
    parser.add_argument('--tag', type=str, help='what to add to the end of the eventtypes.txt file name.', required=True)
    args = parser.parse_args()
    
    (directory, x) = os.path.split(args.document)
    _main(args.document, directory, args.tag)