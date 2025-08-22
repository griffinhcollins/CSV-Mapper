import csv
import requests
import numpy as np
import json
# A tool to map REDCap export CSVs to import CSVs with different instrument formats

# Input project token: 2A7FB1BE285EA4CC00BB2920F97F5865
# Output project token: 68836BC63FBEB3532CBDAD7636417A06

input_data_dictionary = []
output_data_dictionary = []

with open('InputProject_DataDictionary_2025-08-22.csv') as imp_csv:
    reader = csv.reader(imp_csv, delimiter=',')
    for row in reader:
        input_data_dictionary.append(row)


with open('OutputProject_DataDictionary_2025-08-22.csv') as imp_csv:
    reader = csv.reader(imp_csv, delimiter=',')
    for row in reader:
        output_data_dictionary.append(row)
        
input_fields = np.array(input_data_dictionary)[1:,0]
output_fields = np.array(output_data_dictionary)[1:,0]

print("Input data fields:")
print(input_fields)

print("Export data fields:")
print(output_fields)

# Keys are input fields, values are output fields
field_map = {
    "record_id" : "record_id",
    "first_name" : "given_name",
    "last_name" : "family_name",
    "person_age" : "age"
}


# Get data from input project
import requests
data = {
    'token': '2A7FB1BE285EA4CC00BB2920F97F5865',
    'content': 'record',
    'action': 'export',
    'format': 'json',
    'type': 'flat',
    'csvDelimiter': '',
    'rawOrLabel': 'raw',
    'rawOrLabelHeaders': 'raw',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}
r = requests.post('https://testcap.florey.edu.au/api/',data=data)
print('HTTP Status: ' + str(r.status_code))

print(r.json())

out_json = []
for row in r.json():
    
    mapped_record = {}
    for key in row:
        if (key in field_map):
            mapping = field_map[key]
            # print(f"match: {key} -> {mapping}")
            mapped_record[mapping] = row[key]

    out_json.append(mapped_record)

print(out_json)

data = {
    'token': '68836BC63FBEB3532CBDAD7636417A06',
    'content': 'record',
    'action': 'import',
    'format': 'json',
    'type': 'flat',
    'overwriteBehavior': 'normal',
    'forceAutoNumber': 'false',
    'data': json.dumps(out_json),
    'returnContent': 'count',
    'returnFormat': 'json'
}
r = requests.post('https://testcap.florey.edu.au/api/',data=data)
print('HTTP Status: ' + str(r.status_code))
print(r.text)