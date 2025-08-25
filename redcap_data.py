import requests
import numpy as np
import xml.etree.ElementTree as ET
import os





def project_xml(token):

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
    r = requests.post('https://testcap.florey.edu.au/api/',data=data)
    print('HTTP Status: ' + str(r.status_code))
    return ET.fromstring(r.text)
    
        
def get_dictionary(token, format):
    reformat = False
    if (format == 'list'):
        reformat = True
        format = 'csv'
    data = {
        'token': token,
        'content': 'metadata',
        'format': format,
        'returnFormat': 'json'
    }
    r = requests.post('https://testcap.florey.edu.au/api/',data=data)
    print('HTTP Status: ' + str(r.status_code))
    if (not reformat):
        return r
    return np.array(list(x.split(',') for x in r.text.split('\n')[:-1]))

def import_file(token, record_id, field_name, filename):
    data = {
    'token': token,
    'content': 'file',
    'action': 'export',
    'record': record_id,
    'field': field_name,
    'event': '',
    'returnFormat': 'json'
    }
    r = requests.post('https://testcap.florey.edu.au/api/',data=data)
    print('HTTP Status: ' + str(r.status_code))
    print(f"Filename: {filename}")
    if (not os.path.exists("tmp/")):
        os.makedirs("tmp/")
    with open(f"tmp/{filename}", 'wb+') as f:
        f.write(r.content)
        f.close()
    return r
    


if (__name__ == '__main__'):
    print(get_dictionary('2A7FB1BE285EA4CC00BB2920F97F5865', 'csv'))
    # project_xml('2A7FB1BE285EA4CC00BB2920F97F5865')