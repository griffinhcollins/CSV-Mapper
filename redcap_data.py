import requests
import numpy as np
import xml.etree.ElementTree as ET
import os
import csv





def project_xml(token, testcap = False):

    data = {
        'token': token,
        'content': 'project_xml',
        'format': 'json',
        'returnMetadataOnly': 'false',
        'exportFiles': 'false',
        'exportSurveyFields': 'false',
        'exportDataAccessGroups': 'false',
        'returnFormat': 'json'
    }
    r = requests.post(f'https://{"testcap" if testcap else "redcap"}.florey.edu.au/api/',data=data)
    print('XML HTTP Status: ' + str(r.status_code))
    return ET.fromstring(r.text)
    
        
def get_dictionary(token, testcap = False):
    reformat = False
    data = {
        'token': token,
        'content': 'metadata',
        'format': 'csv',
        'returnFormat': 'json'
    }
    r = requests.post(f'https://{"testcap" if testcap else "redcap"}.florey.edu.au/api/',data=data)
    print('Dictionary HTTP Status: ' + str(r.status_code))
    
    
    csv_raw = []
    
    for row in r.text.split('\n')[:-1]:
        csv_raw.append(row)
    
    array = []
    for read_row in csv.reader(csv_raw):
        array.append(read_row)

    return np.array(array)
    
    
    # return 

def import_file(token, record_id, field_name, filename, testcap = False, event = ""):
    data = {
        'token': token,
        'content': 'file',
        'action': 'export',
        'record': record_id,
        'field': field_name,
        'event' : event,
        'returnFormat': 'json'
    }
    r = requests.post(f'https://{"testcap" if testcap else "redcap"}.florey.edu.au/api/',data=data)
    print('Import HTTP Status: ' + str(r.status_code))
    # print(f"Filename: {filename}")
    if (not os.path.exists("tmp/")):
        os.makedirs("tmp/")
    with open(f"tmp/{filename}", 'wb+') as f:
        f.write(r.content)
        f.close()
    return r


def export_file(token, record_id, field_name, filename, testcap = False, event = ""):
    data = {
        'token': token,
        'content': 'file',
        'action': 'import',
        'record': record_id,
        'field': field_name,
        'event' : event,
        'returnFormat': 'json'
    }
    print(data)
    with open(f"tmp/{filename}", 'rb') as file_obj:
        r = requests.post(f'https://{"testcap" if testcap else "redcap"}.florey.edu.au/api/',data=data,files={'file':file_obj})
        file_obj.close()
    print('Export HTTP Status: ' + str(r.status_code))

    


if (__name__ == '__main__'):
    print(get_dictionary('2A7FB1BE285EA4CC00BB2920F97F5865'))
    # project_xml('2A7FB1BE285EA4CC00BB2920F97F5865')