import csv
import requests
import numpy as np
import json
import xml.etree.ElementTree as ET
import redcap_data


# A tool to map REDCap export CSVs to import CSVs with different instrument formats

# Input project token: 2A7FB1BE285EA4CC00BB2920F97F5865
# Output project token: 68836BC63FBEB3532CBDAD7636417A06

# with open('InputProject_DataDictionary_2025-08-22.csv') as imp_csv:
#     reader = csv.reader(imp_csv, delimiter=',')
#     for row in reader:
#         input_data_dictionary.append(row)


# with open('OutputProject_DataDictionary_2025-08-22.csv') as imp_csv:
#     reader = csv.reader(imp_csv, delimiter=',')
#     for row in reader:
#         output_data_dictionary.append(row)

input_data_dictionary = redcap_data.get_dictionary("2A7FB1BE285EA4CC00BB2920F97F5865", 'list')
output_data_dictionary = redcap_data.get_dictionary("68836BC63FBEB3532CBDAD7636417A06", 'list')

# print(input_data_dictionary)

for r in input_data_dictionary:
    print(r)

# used to look up types
input_fields = dict(x for x in input_data_dictionary[1:,0:4:3])
output_fields = dict(x for x in output_data_dictionary[1:,0:4:3])

# used to check for '<form>_complete' records
form_complete_fields = np.unique(input_data_dictionary[1:,1] + "_complete")


print("Input data fields:")
print(input_fields)

print("Export data fields:")
print(output_fields)

# Keys are input fields, values are output fields
field_map = {
    "record_id" : "record_id",
    "first_name" : "given_name",
    "last_name" : "family_name",
    "person_age" : "age",
    "pdf_upload" : "file_upload"
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
for record in r.json():
    
    mapped_record = {}
    for field in record:
        if field in form_complete_fields:
            print(f"Skipping {field}")
            continue
        # print(f"{field} of type {input_fields[field]}")
        if (field in field_map):
            mapping = field_map[field]
            # print(f"match: {key} -> {mapping}")
            mapped_record[mapping] = record[field]
        else:
            print(f"Skipping {field} with value {record[field]} (no map)")

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