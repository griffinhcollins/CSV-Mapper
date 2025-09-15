import os
import requests
import numpy as np
import json
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



#   REDCAP PROJECT TOKENS
#   Current Prod Token (DANGER):        622963362CB2339A4DF099C171BA7492
#   New Setup Token (DANGER):           F1718768DEA7197DDA9BD3A2EF862195
#
#   REDCap-created duplicate of prod:   AAF352AC73709A6AE89C45881A227FBB
#   Test export target for prod:        A101E9985884C4423C2508B396784378
#   
#   


in_token = "AAF352AC73709A6AE89C45881A227FBB" # the project that data will be imported FROM
out_token = "A101E9985884C4423C2508B396784378" # the project that data will be exported TO
testcap = False

dangerous_tokens = ["622963362CB2339A4DF099C171BA7492", "F1718768DEA7197DDA9BD3A2EF862195"]
if (testcap is False and (in_token in dangerous_tokens or out_token in dangerous_tokens)):
    print("WARNING: PROD TOKEN DETECTED. MAKE SURE YOU WANT TO DO THIS AND CONFIGURATIONS ARE CORRECT.")
    confirm = input("TYPE \"YES\" TO CONFIRM: ")
    if confirm.upper() != "YES":
        quit()

input_data_dictionary = redcap_data.get_dictionary(in_token)
output_data_dictionary = redcap_data.get_dictionary(out_token)
# print(input_data_dictionary)


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
    # "record_id" : "record_id",
    # "first_name" : "given_name",
    # "last_name" : "family_name",
    # "person_age" : "age",
    # "pdf_upload" : "file_upload",
    # "file_upload_var" : "fav_file"
}

auto_match = True # whether the mapper should automatially map fields that have identical names


# Get data from input project
data = {
    'token': in_token,
    'content': 'record',
    'action': 'export',
    'format': 'json',
    'type': 'eav',
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


in_json = r.json()

with open("out.json", "w+") as o:
    json.dump(in_json, o)

# print(r.json())

out_json = []



class FileUploadInfo:
    def __init__(self, record, field, mapped_field, event="") -> None:
        self.record = record
        self.field = field
        self.mapped_field = mapped_field
        self.event = event
        pass

file_uploads: list[FileUploadInfo] = []


# for field in r.json():
#     print(field.keys())
#     trial_no = field['record']
#     event = field['redcap_event_name']
#     mapped_record = {}
#     for field in record:
#         if field in form_complete_fields:
#             print(f"Skipping {field} (form complete)")
#             continue
#         if not record[field]:
#             print(f"Skipping {field} (input empty)")
#             continue
            
#         # print(f"{field} of type {input_fields[field]}")
        
#         mapped_field = ""
#         if (auto_match and field in input_field_types and field in output_field_types):
#             # Auto match means that if two fields have exactly the same name, they're mapped
#             mapped_field = field
            
#         if (field in field_map):
#             # if the input field is in the field map, that overrides the auto match
#             mapped_field = field_map[field]
            
#         if mapped_field == "":
#             print(f"Skipping {field} with value {record[field]} (no map)")
#             continue
        
#         # We have found a match
#         if (mapped_field not in output_field_types.keys()):
#             print(f"Skipping {field} (couldn't find {mapped_field} in export project).")
#             continue
        
#         # File maps need to be done afterwards, as the records we want to upload to don't exist yet.
#         if input_field_types[field] == 'file':
#             if record[field] == '':
#                 continue
#             file_uploads.append(FileUploadInfo(record, field, mapped_field, event))
#             continue
        
#         # Map everything else
#         # print(f"match: {key} -> {mapping}")
#         print(f"Mapping {field} to {mapped_field}")
#         mapped_record[mapped_field] = record[field]

#     out_json.append(mapped_record)


data = {
    'token': out_token,
    'content': 'record',
    'action': 'import',
    'format': 'json',
    'type': 'eav',
    'overwriteBehavior': 'normal',
    'forceAutoNumber': 'new',
    'data': json.dumps(in_json),
    'returnContent': 'count',
    'returnFormat': 'json'
}
r = requests.post(f'https://{"testcap" if testcap else "redcap"}.florey.edu.au/api/',data=data)
print('HTTP Status: ' + str(r.status_code))
print(r.text)

# for file_upload in file_uploads:
#     field = file_upload.field
#     record = file_upload.record
#     mapped_field = file_upload.mapped_field
#     event = file_upload.event
#     print(f"Writing {field} {record[field]} to tmp")
#     redcap_data.import_file(in_token, record[input_record_id_alias], field, record[field], testcap, event)
#     print(f"Uploading {mapped_field}: {record[field]} to project")
#     redcap_data.export_file(out_token, record[input_record_id_alias], mapped_field, record[field], testcap, event)
#     print(f"Deleting temp file {record[field]}")
    # os.remove(f"tmp/{record[field]}")