import os
import requests
import numpy as np
import json
import redcap_data
from redcap_data import FileUploadInfo


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
#
#   Test export target for prod:        A101E9985884C4423C2508B396784378
#   2nd Test Export:                    B143B1B57E6F071FC41C5D08F9226EFB
#


in_token = (
    "AAF352AC73709A6AE89C45881A227FBB"  # the project that data will be imported FROM
)
out_token = (
    "B143B1B57E6F071FC41C5D08F9226EFB"  # the project that data will be exported TO
)
testcap = False

dangerous_tokens = [
    "622963362CB2339A4DF099C171BA7492",
    "F1718768DEA7197DDA9BD3A2EF862195",
]
if testcap is False and (in_token in dangerous_tokens or out_token in dangerous_tokens):
    print(
        "WARNING: PROD TOKEN DETECTED. MAKE SURE YOU WANT TO DO THIS AND CONFIGURATIONS ARE CORRECT."
    )
    confirm = input('TYPE "YES" TO CONFIRM: ')
    if confirm.upper() != "YES":
        quit()

input_data_dictionary = redcap_data.get_dictionary(in_token)
output_data_dictionary = redcap_data.get_dictionary(out_token)
# print(input_data_dictionary)

input_events = redcap_data.get_events(in_token)
output_events = redcap_data.get_events(out_token)

input_event_names = [e["unique_event_name"] for e in input_events]
output_event_names = [e["unique_event_name"] for e in output_events]

# used to look up types
input_field_types = dict(x for x in input_data_dictionary[1:, 0:4:3])
output_field_types = dict(x for x in output_data_dictionary[1:, 0:4:3])

# used to check for '<form>_complete' records
form_complete_fields = np.unique(input_data_dictionary[1:, 1] + "_complete")

if not os.path.exists("logs/"):
    os.makedirs("logs/")

print("Writing input data fields to in_fields")
with open("logs/in_fields.json", "w+") as o:
    json.dump(input_field_types, o)

print("Writing out data fields to out_fields")
with open("logs/out_fields.json", "w+") as o:
    json.dump(output_field_types, o)


# Keys are input fields, values are output fields
event_field_map = {
    ("for_participating_arm_1", "cv_upload"): ("study_details_arm_1", "cv_upload"),
    ("for_participating_arm_1", "ich_upload"): ("study_details_arm_1", "ich_upload"),
    ("contracts_and_insu_arm_1", "agreements_future"): (
        "cgc_review_arm_1",
        "agreements_future",
    ),
    ("contracts_and_insu_arm_1", "agreements_other"): (
        "cgc_review_arm_1",
        "agreements_other",
    ),
}


def AddFieldRange(in_tup_prefix: tuple, out_tup_prefix: tuple, range_e: tuple):
    for i in range(range_e[0], range_e[1]):
        event_field_map[(in_tup_prefix[0], in_tup_prefix[1] + str(i))] = (
            out_tup_prefix[0],
            out_tup_prefix[1] + str(i),
        )


# AddFieldRange(
#     ("contracts_and_insu_arm_1", "agreements_future___"),
#     ("cgc_review", "agreements_future___"),
#     (0, 12),
# )

auto_match = (
    True  # whether the mapper should automatially map fields that have identical names
)


# Get data from input project
data = {
    "token": in_token,
    "content": "record",
    "action": "export",
    "format": "json",
    "type": "eav",
    "csvDelimiter": "",
    "rawOrLabel": "raw",
    "rawOrLabelHeaders": "raw",
    "exportCheckboxLabel": "false",
    "exportSurveyFields": "false",
    "exportDataAccessGroups": "false",
    "returnFormat": "json",
}
r = requests.post(
    f'https://{"testcap" if testcap else "redcap"}.florey.edu.au/api/', data=data
)
print("Data Import HTTP Status: " + str(r.status_code))


in_json = r.json()

with open("logs/input_data.json", "w+") as o:
    json.dump(in_json, o)

# print(r.json())

out_json = []


file_uploads: list[FileUploadInfo] = []

with open("logs/maplog.txt", "w+") as o:

    for field_data in r.json():
        trial_no = field_data["record"]
        field = field_data["field_name"]
        event = field_data["redcap_event_name"]

        if field in form_complete_fields:
            o.write(f"Skipping {field} (form complete)\n")
            continue
        if not field_data["value"]:
            o.write(f"Skipping {field} (input empty)\n")
            continue

        # print(f"{field} of type {input_fields[field]}")

        mapped_event_name = ""
        mapped_field_name = ""
        if (
            auto_match
            and (field in input_field_types.keys())
            and (field in output_field_types.keys())
            and (event in input_event_names)
            and (event in output_event_names)
        ):
            # Auto match means that if there exists a field/event pair in both in and out that match, they will map
            mapped_field_name = field
            mapped_event_name = event

        if (event, field) in event_field_map:
            # if the input field is in the field map, that overrides the auto match
            o.write(f"Using manual mapping for following: ")
            (mapped_event_name, mapped_field_name) = event_field_map[(event, field)]

        if mapped_field_name == "":
            o.write(
                f"Skipping {field} from event {event} with value {field_data["value"]} (no map)\n"
            )
            continue

        # We have found a match
        if mapped_field_name not in output_field_types.keys():
            o.write(
                f"Skipping {field} (couldn't find {mapped_field_name} in export project)\n"
            )
            continue

        # File maps need to be done afterwards, as the records we want to upload to don't exist yet.
        if input_field_types[field] == "file":
            file_uploads.append(
                FileUploadInfo(
                    trial_no,
                    event,
                    field,
                    field_data["redcap_repeat_instance"],
                    mapped_event_name,
                    mapped_field_name,
                )
            )
            # continue

        # Map everything else
        # print(f"match: {key} -> {mapping}")
        o.write(f"Mapping {field} to {mapped_field_name}\n")

        out_json.append(
            {
                "record": trial_no,
                "redcap_event_name": mapped_event_name,
                "redcap_repeat_instrument": field_data["redcap_repeat_instrument"],
                "redcap_repeat_instance": field_data["redcap_repeat_instance"],
                "field_name": mapped_field_name,
                "value": field_data["value"],
            }
        )


with open("logs/output_data.json", "w+") as o:
    json.dump(out_json, o)

data = {
    "token": out_token,
    "content": "record",
    "action": "import",
    "format": "json",
    "type": "eav",
    "overwriteBehavior": "normal",
    "forceAutoNumber": "new",
    "data": json.dumps(out_json),
    "returnContent": "count",
    "returnFormat": "json",
}
r = requests.post(
    f'https://{"testcap" if testcap else "redcap"}.florey.edu.au/api/', data=data
)
print("Data Export HTTP Status: " + str(r.status_code))
if str(r.status_code) != "200":
    with open("logs/error.json", "w+") as o:
        json.dump(r.json(), o)
        print("Wrote raw error to error.json")
    with open("logs/formatted_error.txt", "w+") as o:
        errortext = r.json()["error"]
        o.write(errortext)
        print("Wrote formatted error to formatted_error.txt")
    quit()
else:
    print(f"Successfully imported {r.json()["count"]} records")

print("Beginning file transfer")

for file_upload in file_uploads:
    trial_no = file_upload.trial_no
    field_name = file_upload.field
    mapped_field = file_upload.mapped_field
    event = file_upload.event
    print(f"Writing {field_name} to tmp")
    filename = redcap_data.import_file(in_token, file_upload)
    print(f"Wrote {filename} to tmp")
    print(f"Uploading {mapped_field}: {filename} to project")
    redcap_data.export_file(out_token, file_upload, filename)
    print(f"Deleting temp file {filename}")
    os.remove(f"tmp/{filename}")
