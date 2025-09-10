import requests
import numpy as np
import xml.etree.ElementTree as ET
import os





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
    
        
def get_dictionary(token, format, testcap = False):
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
    r = requests.post(f'https://{"testcap" if testcap else "redcap"}.florey.edu.au/api/',data=data)
    print('Dictionary HTTP Status: ' + str(r.status_code))
    if (not reformat):
        return np.array(r)
    return np.array(list(x.split(',') for x in r.text.split('\n')[:-1]))

def import_file(token, record_id, field_name, filename, testcap = False):
    data = {
    'token': token,
    'content': 'file',
    'action': 'export',
    'record': record_id,
    'field': field_name,
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


def export_file(token, record_id, field_name, filename, testcap = False):
    data = {
        'token': token,
        'content': 'file',
        'action': 'import',
        'record': record_id,
        'field': field_name,
        'event': '',
        'returnFormat': 'json'
    }
    with open(f"tmp/{filename}", 'rb') as file_obj:
        r = requests.post(f'https://{"testcap" if testcap else "redcap"}.florey.edu.au/api/',data=data,files={'file':file_obj})
        file_obj.close()
    print('Export HTTP Status: ' + str(r.status_code))

    


if (__name__ == '__main__'):
    print(get_dictionary('2A7FB1BE285EA4CC00BB2920F97F5865', 'csv'))
    # project_xml('2A7FB1BE285EA4CC00BB2920F97F5865')