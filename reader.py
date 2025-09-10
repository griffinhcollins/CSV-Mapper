import os
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

in_token = "A09C902757EC3E81ECC23A46A5378C79"
out_token = "7D6C3FA703928B4A3ADDC26099D5C63A"
testcap = False

input_data_dictionary = redcap_data.get_dictionary(in_token, 'list')
output_data_dictionary = redcap_data.get_dictionary(out_token, 'list')

# print(input_data_dictionary)

for r in input_data_dictionary:
    print(r)

# used to look up types
input_field_types = dict(x for x in input_data_dictionary[1:,0:4:3])
output_field_types = dict(x for x in output_data_dictionary[1:,0:4:3])

# used to check for '<form>_complete' records
form_complete_fields = np.unique(input_data_dictionary[1:,1] + "_complete")


print("Input data fields:")
print(input_field_types)

print("Export data fields:")
print(output_field_types)

# Keys are input fields, values are output fields
field_map = {
    "record_id" : "record_id",
    "first_name" : "given_name",
    "last_name" : "family_name",
    "person_age" : "age",
    "pdf_upload" : "file_upload",
    "file_upload_var" : "fav_file"
}


# Get data from input project
data = {
    'token': in_token,
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
r = requests.post(f'https://{"testcap" if testcap else "redcap"}.florey.edu.au/api/',data=data)
print('HTTP Status: ' + str(r.status_code))

print(r.json())

out_json = []
for record in r.json():
    
    mapped_record = {}
    for field in record:
        if field in form_complete_fields:
            print(f"Skipping {field} (form complete)")
            continue
        # print(f"{field} of type {input_fields[field]}")
        if (field in field_map):
            if (field_map[field] not in output_field_types.keys()):
                print(f"Skipping {field} (couldn't find {field_map[field]} in export project).")
                continue
            if input_field_types[field] == 'file':
                if record[field] == '':
                    continue
                print(f"Writing {field} {record[field]} to tmp")
                redcap_data.import_file(in_token, record['record_id'], field, record[field])
                print(f"Uploading {field_map[field]} {record[field]} to project")
                redcap_data.export_file(out_token, record['record_id'], field_map[field], record[field])
                print(f"Deleting temp file {record[field]}")
                os.remove(f"tmp/{record[field]}")
                continue
            mapping = field_map[field]
            # print(f"match: {key} -> {mapping}")
            mapped_record[mapping] = record[field]
        else:
            print(f"Skipping {field} with value {record[field]} (no map)")

    out_json.append(mapped_record)

print(out_json)

data = {
    'token': out_token,
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
r = requests.post(f'https://{"testcap" if testcap else "redcap"}.florey.edu.au/api/',data=data)
print('HTTP Status: ' + str(r.status_code))
print(r.text)