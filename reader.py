import csv
import requests
import numpy as np
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
for row in r.json():
    record_id = row[0]
    for pair in row[1:]:
        print(f"{pair[0]} : {pair[1]}")
    print(row)
    
