import requests
import numpy as np
import xml.etree.ElementTree as ET





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


if (__name__ == '__main__'):
    print(get_dictionary('2A7FB1BE285EA4CC00BB2920F97F5865', 'csv'))
    # project_xml('2A7FB1BE285EA4CC00BB2920F97F5865')