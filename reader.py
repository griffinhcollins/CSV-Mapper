import os
import requests
import numpy as np
import json
import redcap_data
from redcap_data import FileUploadInfo, get_radio_map, get_all_fields_of_type
from map_generator import generate_map


# A tool to map REDCap export CSVs to import CSVs with different instrument formats

# Input project token: 2A7FB1BE285EA4CC00BB2920F97F5865
# Output project token: 68836BC63FBEB3532CBDAD7636417A06

#   REDCAP PROJECT TOKENS
#   Current Prod Token (DANGER):                622963362CB2339A4DF099C171BA7492
#   New Setup Token (DANGER):                   F1718768DEA7197DDA9BD3A2EF862195
#
#   REDCap-created duplicate of prod:           AAF352AC73709A6AE89C45881A227FBB
#   Import Source 2:                            D9D7B7DACF84EBC9D9E720F2CA04ADE3
#
#   Test export target for prod:                A101E9985884C4423C2508B396784378
#   2nd Test Export:                            B143B1B57E6F071FC41C5D08F9226EFB
#   3rd Test Export:                            490755B7DA96CF2C7392DD9D2879238D
#   4th Test Export:                            822420F5054FD8B87965A0FC05E23552
#   Final Test Export:                          EC2F9D30E45E2ECB25B6061571E85BBE
#   Clinical Studies Registry V3 - Test Copy 2: 497E85C2071B05AD0F31C382839F3E72

"""
TODO
"""




in_token = "622963362CB2339A4DF099C171BA7492"  # the project that data will be imported FROM
out_token = "D30CC0DB1205651314EC8EF5FEE109C9"  # the project that data will be exported TO

map_filename = "Map of Variables from V2 to V3 (FINAL - 220126).csv" # the csv file containing the mapping rules

testcap = False # whether to use the Florey testcap server rather than the redcap server

# Real tokens (pointing at projects I didn't make)
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

input_events = redcap_data.get_events(in_token)
output_events = redcap_data.get_events(out_token)

input_event_names = [e["unique_event_name"] for e in input_events]
output_event_names = [e["unique_event_name"] for e in output_events]

# used to look up types, useful for radio button remapping and file transfer
input_field_types = dict(x for x in input_data_dictionary[1:, 0:4:3])
output_field_types = dict(x for x in output_data_dictionary[1:, 0:4:3])

# used to check for '<form>_complete' records
form_complete_fields = np.unique(input_data_dictionary[1:, 1] + "_complete")


# Used to ensure radio fields are mapped correctly
radio_in = get_radio_map(in_token, False)
radio_out = get_radio_map(out_token, True)



# Radio fields sometimes have the same labels applied to different IDs. e.g. if the original has "0: Yes, 1: No" and the new one has "0: No, 1: Yes"
# This goes through labels and tries to match them up where possible
def radio_map(field, mapped_field, value):

    if field not in radio_in:
        return value

    if value in radio_in[field]:
        label_data = radio_in[field][value]
    else:
        print(f"Can't map radio: no {value} from {field} in {radio_in[field]}")
        return value

    if label_data not in radio_out[mapped_field]:
        custom_pending = False
        for label in radio_out[mapped_field]:
            if "Pending" in label:
                label_data = label
                custom_pending = True
                break
        if not custom_pending:
            label = "Pending"

    new_id = radio_out[mapped_field][label_data]
    return new_id


if not os.path.exists("logs/"):
    os.makedirs("logs/")

print("Writing input data fields to in_fields")
with open("logs/in_fields.json", "w+") as logs:
    json.dump(input_field_types, logs)

print("Writing out data fields to out_fields")
with open("logs/out_fields.json", "w+") as logs:
    json.dump(output_field_types, logs)


# Keys are input fields, values are output fields
event_field_map = generate_map(map_filename)



auto_match = False  # whether the mapper should automatially map fields that have identical names


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
input_import = requests.post(
    f'https://{"testcap" if testcap else "redcap"}.florey.edu.au/api/', data=data
)
print("Data Import HTTP Status: " + str(input_import.status_code))


in_json = input_import.json()

with open("logs/input_data.json", "w+") as logs:
    json.dump(in_json, logs)

# Get data from output project to check before overwriting
data = {
    "token": out_token,
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
existing_output_data = requests.post(
    f'https://{"testcap" if testcap else "redcap"}.florey.edu.au/api/', data=data
)
print("Existing Data Import HTTP Status: " + str(input_import.status_code))

existing_output_data = existing_output_data.json()

with open("logs/existing_data.json", "w+") as logs:
    json.dump(existing_output_data, logs)

existing_trials = set()
for result in existing_output_data:
    if result["record"] not in existing_trials:
        existing_trials.add(result["record"])


file_uploads: list[FileUploadInfo] = []

print("Preparing Export...")

concat_tracking = {}
if_tracking = {}

manual_event_fields = {}

trials_to_skip = set()
trials_to_overwrite = set()

out_json = []


# Input fields that are mapped if numeric, otherwise sent to study details
enforce_numeric = ["no_sites"]
overwrite = False

with open("logs/maplog.txt", "w") as logs:
    # Go through every single field in the input project
    for field_data in in_json:
        trial_no = field_data["record"]
        if trial_no in trials_to_skip:
            continue
        if overwrite and trial_no in existing_trials:
            print(f"SKIP OVERWRITE: {trial_no} has existing data in the target project. Skipping!")
            trials_to_skip.add(trial_no)
            continue

        field = field_data["field_name"]
        event = field_data["redcap_event_name"]
        value = field_data["value"]
        
        # Used for logging
        if event in manual_event_fields.keys():
            manual_event_fields[event].append(field)
        else:
            manual_event_fields[event] = []

        # All exported records will be marked as incomplete, to be gone through later and confirmed
        if field in form_complete_fields:
            logs.write(f"Skipping {field} (form complete)\n")
            continue
        
        if value is None or value == "":
            logs.write(f"Skipping {field} (input empty)\n")
            continue

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
            logs.write(
                f"Trial {trial_no}: Using manual mapping for {field} from event {event} with value {value}\n"
            )
            repeat_instrument = event_field_map[(event, field)][0] # Get the instrument, only used for repeating events
            for mapping in event_field_map[(event, field)][1]:
                remap = True  # As of yet we don't know if this is the main mapping (in which case we want to remap if a remap is present) or a plus mapping (in which case we will toggle this off)
                manual_save = False
                
                if mapping[0] == "remap":
                    continue  # the existence of a remap does nothing on its own - it'll be used when the main mapping is parsed
                if mapping[0] == "concat" or (field in enforce_numeric and not value.isnumeric()):
                    # See if [tracking_tools_arm_1][notes_for_followup] exists yet for this trial, that's where all concat fields are sent to
                    if trial_no not in concat_tracking:
                        concat_tracking[trial_no] = len(out_json)
                        out_json.append(
                            {
                                "record": trial_no,
                                "redcap_event_name": "tracking_tools_arm_1",
                                "redcap_repeat_instrument": "",
                                "redcap_repeat_instance": "",
                                "field_name": "notes_for_followup",
                                "value": "",
                            }
                        )
                    logs.write(f"Concatenating {value} into {trial_no}'s tracking field.\n")
                    out_json[concat_tracking[trial_no]]["value"] += f"{field}: {value}\n" # Keep track of one concatenating value per trial, adding more to it if more things are added
                    continue

                if mapping[0] == "manual":
                    # Save to a folder (this should be a file)
                    file_uploads.append(
                        FileUploadInfo(
                            trial_no,
                            event,
                            field,
                            field_data["redcap_repeat_instance"],
                            "",
                            "",
                            True,
                        )
                    )
                    break
                if mapping[0] == "IF":
                    # Unpack the ifmap. For more information about where these ifmaps come from, see map_generator.py
                    ifmap = mapping[1]
                    for k in ifmap.keys():
                        ifmap_key = k
                    if type(ifmap_key) is tuple:
                        if ifmap_key[0] == 'instance': # Some IFs use the repeat instance to decide whether to map or not. They're tagged with this if they are. 
                            if int(ifmap_key[1].strip()) == field_data["redcap_repeat_instance"]:
                                logs.write(f"IFMAP repeat instrument fired!, {field_data["redcap_repeat_instance"]} == {ifmap_key[1]}\n")
                                for k in ifmap.keys():
                                    k = value
                            else:
                                logs.write(f"IFMAP repeat instrument didn't fire, {field_data["redcap_repeat_instance"]} != {ifmap_key[1]}\n")
                                continue
                        elif ifmap_key[0] == 'foreign':
                            # This tag means we need to look up a completely different event and field
                            foreign_field = ifmap_key[1]
                            # And check if it's equal to either of the given values
                            foreign_values = ifmap_key[2]
                            
                            # First find the field
                            for row in in_json:
                                if row["record"] == trial_no and row["redcap_event_name"] == foreign_field[0] + "_arm_1" and row["field_name"] == foreign_field[1]:
                                    if row["value"] in list(foreign_values):
                                        for k in ifmap.keys(): # It's a match, let the IF go through by setting the key to the value, meaning it will match and allow the IF to fire
                                            k = value
                                        logs.write(f"Foreign clause detected, {foreign_field} does match {foreign_values}, firing\n")
                                        break
                                    else:
                                        logs.write(f"Foreign clause detected, {foreign_field} does not match {foreign_values}, skipping\n")
                                        # not a match, skip
                                        # By not changing anything, the IF won't go through, as a tuple will never be the value
                                        break
                            continue
                        elif value not in ifmap_key: # A tuple of numbers with no tag means we've got a <> case, meaning we want to map if the value isn't any of the values in the tuple
                            logs.write(f"IFMAP <> clause detected, {value} not in {ifmap_key}, making valid\n")
                            for k in ifmap.keys(): # It's a match, let the IF go through by setting the key to the value, meaning it will match and allow the IF to fire
                                k = value
                        else:
                            logs.write(f"IFMAP <> clause detected, {value} IS in {ifmap_key}, skipping\n")
                            # By not changing anything, the IF won't go through, as a tuple will never be the value
                            continue
                    if value not in ifmap:
                        logs.write(
                            f"IFMAP no match: {value} not in {ifmap}, skipping with no action\n"
                        )
                        continue
                    if ifmap[value][2] == "this": # 'this' tag means instead of a set 'y' to put into the mapped event and field, we grab the value from the field and event the ifmap gives us
                        ifmap[value[2]] = value
                        logs.write(
                            f"'this' swap: 'this' swapped for {value} for [{ifmap[value][0]}][{ifmap[value][1]}]\n"
                        )
                    (mapped_event, mapped_field, value) = ifmap[value]
                    if value == "i":
                        # We need to increment [application][oseas_sites_num_2]
                        # First, check if it exists already
                        if trial_no not in if_tracking:
                            if_tracking[trial_no] = len(out_json)
                            out_json.append(
                                {
                                    "record": trial_no,
                                    "redcap_event_name": "governance_applica_arm_1",
                                    "redcap_repeat_instrument": "application",
                                    "redcap_repeat_instance": "",
                                    "field_name": "oseas_sites_num_2",
                                    "value": 0,
                                }
                            )
                        logs.write(
                            f"IF CLAUSE: Incrementing [application][oseas_sites_num_2] by 1 for {trial_no} from {event} {field}\n"
                        )
                        out_json[if_tracking[trial_no]]["value"] += 1
                    else:
                        # set based on the value
                        out_json.append(
                            {
                                "record": trial_no,
                                "redcap_event_name": mapped_event,
                                "redcap_repeat_instrument": (
                                    ""
                                    if field_data["redcap_repeat_instrument"] == ""
                                    else repeat_instrument
                                ),
                                "redcap_repeat_instance": field_data["redcap_repeat_instance"],
                                "field_name": mapped_field,
                                "value": radio_map(field, mapped_field, value),
                            }
                        )
                        logs.write(f"IF CLAUSE: setting {mapped_event} {mapped_field} to {value}\n")
                    continue

                # Regular or plus mapping
                if len(mapping) == 2:
                    # regular mapping
                    (mapped_event_name, mapped_field_name) = mapping

                    logs.write(
                        f"Trial {trial_no}: Mapping {field} to {mapped_field_name} in event {mapped_event_name} with value {value}\n"
                    )

                elif len(mapping) == 3:
                    (mapped_event_name, mapped_field_name) = (mapping[0], mapping[1])
                    value = mapping[2]
                    # plus mapping
                    remap = False
                    logs.write(
                        f"Trial {trial_no}: Mapping {field} to {mapped_field_name} in event {mapped_event_name} with value {mapping[2]} (PLUS MAPPING)\n"
                    )

                else:
                    # something's wrong
                    logs.write(f"ERROR: weird mapping: {mapping}\n")
                    continue

                if mapped_field_name == "":
                    logs.write(f"Skipping (no map)\n")
                    continue

                # We have found a match
                if mapped_field_name not in output_field_types.keys():
                    logs.write(
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
                # check if there's a remap if this is the main mapping
                if remap:
                    for remapcheck in event_field_map[(event, field)][1]:
                        if remapcheck[0] == "remap":
                            # Change the value
                            mapped = False
                            for relation in remapcheck[1]:
                                if str.startswith(relation, field_data["value"]):
                                    logs.write(
                                        f"Replacement mapping: swapping {value} for {relation[2:]}\n"
                                    )
                                    if relation[2:] == "concat":
                                        if trial_no not in concat_tracking:
                                            concat_tracking[trial_no] = len(out_json)
                                            out_json.append(
                                                {
                                                    "record": trial_no,
                                                    "redcap_event_name": "tracking_tools_arm_1",
                                                    "redcap_repeat_instrument": "",
                                                    "redcap_repeat_instance": "",
                                                    "field_name": "notes_for_followup",
                                                    "value": "",
                                                }
                                            )
                                        logs.write(f"Concatenating {value} into {trial_no}'s tracking field\n")
                                        out_json[concat_tracking[trial_no]]["value"] += f"{field}: {value}\n"
                                        break
                                    value = relation[2:]
                                    mapped = True
                                    break
                            if not mapped:
                                logs.write(
                                    f"Skipping unexpected replacement mapping input of {value}\n"
                                )
                                value = "NOTUSED"
                                break
                if value == "NOTUSED":
                    continue
                out_json.append(
                    {
                        "record": trial_no,
                        "redcap_event_name": mapped_event_name,
                        "redcap_repeat_instrument": repeat_instrument,
                        "redcap_repeat_instance": field_data["redcap_repeat_instance"],
                        "field_name": mapped_field_name,
                        "value": radio_map(field, mapped_field_name, value),
                    }
                )
        else:
            logs.write(f"Couldn't find {(event, field)}\n")

print(f"Dumping field data to event_field_map.json")
with open("logs/event_field_map.json", "w+") as logs:
    json.dump(manual_event_fields, logs)
with open("logs/output_data.json", "w+") as logs:
    json.dump(out_json, logs)

yn = input("Begin upload of non-file data? y/n: ")

while yn.lower() != "y" and yn.lower() != "n":
    yn = input("Please enter y for yes or n for no: ")

if yn.lower() == "y":
    print("Uploading...")
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

    input_import = requests.post(
        f'https://{"testcap" if testcap else "redcap"}.florey.edu.au/api/', data=data
    )
    print("Data Export HTTP Status: " + str(input_import.status_code))
    if str(input_import.status_code) != "200":
        with open("logs/error.json", "w+") as logs:
            json.dump(input_import.json(), logs)
            print("Wrote raw error to error.json")
        with open("logs/formatted_error.txt", "w+") as logs:
            errortext = input_import.json()["error"]
            logs.write(errortext)
            print("Wrote formatted error to formatted_error.txt")
        quit()
    else:
        print(f"Successfully imported {input_import.json()["count"]} records")

yn = input("Begin upload of file data? y/n: ")

while yn.lower() != "y" and yn.lower() != "n":
    yn = input("Please enter y for yes or n for no: ")

if yn.lower() == "n":
    quit()

print("Beginning file transfer")

files_already_uploaded = []

use_backup = False

with open("logs/file_logs.txt", "r") as f:
    files_already_uploaded = f.readlines()

with open("logs/file_logs.txt", "w") as f:
    for file_upload in file_uploads:
        filehash = file_upload.hash()
        if use_backup and filehash in files_already_uploaded:
            continue
        print(filehash)
        trial_no = file_upload.trial_no
        field_name = file_upload.field
        mapped_field = file_upload.mapped_field
        event = file_upload.event
        print(f"Downloading {field_name}...")
        filepath = redcap_data.import_file(in_token, file_upload)
        print(f"Wrote {field_name} to {filepath}")
        if not file_upload.manual_save:
            print(f"Uploading {mapped_field}: {filepath} to project")
            redcap_data.export_file(out_token, file_upload, filepath)
            print(f"Deleting temp file {filepath}")
            os.remove(filepath)
        f.write(filehash + "\n")

