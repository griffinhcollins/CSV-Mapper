import csv
import numpy as np
import re
import json

# Using a csv created by Sarah, generates a map from one REDCap project's fields to another's
def generate_map(map_file_name):
    rows = []
    with open(map_file_name) as f:
        reader = csv.reader(f, dialect="excel")
        for row in reader:
            rows.append(row)

    rows = np.array(rows[7:])  # Skip headers

    v2_event_lookup = {  # Convert instruments to arms
        "study_details": "study_details_arm_1",
        "study_intervention": "study_details_arm_1",
        "study_registration": "study_details_arm_1",
        "study_contracts": "contracts_and_insu_arm_1",
        "agreements": "contracts_and_insu_arm_1",
        "data_storage_and_archive": "data_storage_and_a_arm_1",
        "cgc_review": "cgc_review_arm_1",
        "annual_reporting": "annual_reporting_arm_1",
        "annual_safety_reports": "annual_reporting_arm_1",
        "governance_application": "for_participating_arm_1",
        "governance_approval": "for_participating_arm_1",
        "amendment_applications": "for_participating_arm_1",
    }

    v3_event_lookup = {  # Convert instruments to arms
        "eoi": "eoi_arm_1",
        "eoi_clinic_rooms": "eoi_arm_1",
        "mapping_meeting": "eoi_arm_1",
        "application": "governance_applica_arm_1",
        "approval": "governance_applica_arm_1",
        "annual_safety_reports": "postapproval_arm_1",
        "governance_amendments": "postapproval_arm_1",
        "agreements": "tracking_tools_arm_1",
        "team_credentials": "tracking_tools_arm_1",
        "study_status": "tracking_tools_arm_1",
    }

    instrument_fixers = (
        {  # amendment applications is the old repeat instrument, governance is what it matches to
            "amendment_applications": "governance_amendments"
        }
    )

    # used to skip rows that aren't working yet so they don't throw errors and the rest can be tested. empty means everything is expected to work
    wip = []

    map = {}
    with open("logs/map_gen_logs.txt", "w+") as l:
        for row in rows:
            if row[2] == row[3] == "" or row[2] == "NOT USED" or row[3] in wip:
                continue

            mappings = []
            if len(row) > 4 and row[4] != "": # column 4 is only used when additional operations need to be made, separated by //
                cases = str.split(row[4], "//")
                for case in cases:
                    case = str.strip(case)
                    if str.startswith(case, "Plus"):
                        # Do the actual mapping from columns 0-3, then also do this
                        if_i = case.find("IF") # IF clauses are all various mutations of "if this row is x, set another field to y"
                        if if_i != -1:
                            clause = str.strip(case[if_i:])
                            groups = parse_if(clause)
                            x = groups[0]
                            mapped_event = v3_event_lookup[groups[1]]
                            mapped_field = groups[2]
                            y = groups[3]
                            ifmap = {}
                            ifmap[x] = (mapped_event, mapped_field, y)
                            mappings.append(("IF", ifmap))
                        else:
                            # Regex designed specifically for Sarah's syntax
                            plusdata = re.findall(r".*\[(.*)\]\[(.*)\((\d)\)\] = '(\d)'", row[4])
                            if len(plusdata) > 0:
                                data = plusdata[0]
                                mappings.append(
                                    (v3_event_lookup[data[0]], data[1], data[2])
                                )
                            else:
                                plusdata = re.findall(r".*\[(.*)\]\[(.*)\] = '(\d)'", row[4])[0]
                                mappings.append(
                                    (v3_event_lookup[plusdata[0]], plusdata[1], plusdata[2])
                                )
                    elif str.startswith(case, "Responses changed:"):
                        # Unpack the syntax from the csv into a python list, then send it off with a "remap" tag so the reader knows what to do with it
                        casemap = case[18:].replace("'", "").replace(" ", "").split(",")
                        mappings.append(("remap", casemap))
                    else:
                        print(f"Not mapped: |{case}|")

            if row[2] in v3_event_lookup.keys():
                # Regular mapping, each row has its own mappings, ie when the reader is executing, it will look up each mapping for the input fields (rows 0 and 1) then copy them to the mapped fields (rows 2 and 3)
                mappings.append((f"{v3_event_lookup[row[2]]}", str(row[3])))
            else:
                # Special cases (concat, manual and IF)
                if str.startswith(row[2], "CONCATENATE"):
                    mappings.append(("concat", "")) # Used for fields that are too complex to automate, they are all concatenated in [tracking_tools_arm_1][notes_for_followup]
                elif str.startswith(row[2], "FOR MANUAL ALLOCATION"):
                    mappings.append(("manual", "")) # Used for files that aren't to be mapped anywhere. They will instead be saved in ./saved_files
                elif str.startswith(row[2], "IF"):
                    # Do something else depending on the value
                    # mapping[0] is "IF", mapping[1] is the map
                    clauses = row[2].split(";")
                    ifmap = (
                        {}
                    )  # ifmap will be of the form ifmap[x] = (mapped_event, mapped_field, y) which means "if [mapped_event][mapped_field] == x, [mapped_event][mapped_field] = y". In the case of a radio map, check for [mapped_event][x] == 1
                    for clause in clauses:
                        clause = str.strip(clause)
                        groups = parse_if(clause)
                        x = groups[0]
                        mapped_event = v3_event_lookup[groups[1]]
                        mapped_field = groups[2]
                        y = groups[3]
                        ifmap[x] = (mapped_event, mapped_field, y)
                    mappings.append(("IF", ifmap))

                else: # Something has gone wrong, write it to logs
                    l.write(str(row))
                    continue

            # Bonus mappings that apply to all studies, every study has a trial_no row so this will be added to each study exactly once
            if row[1] == "trial_no":
                mappings.append((f"{v3_event_lookup["eoi"]}", "florey_yn", "1"))
                mappings.append((f"{v3_event_lookup["eoi"]}", "ethics_yn", "1"))
                mappings.append((f"{v3_event_lookup["eoi"]}", "research_andor_clinical", "1"))
                mappings.append((f"{v3_event_lookup["eoi"]}", "staff", "1"))

            map[(v2_event_lookup[row[0]], str(row[1]))] = (
                instrument_fixers[row[0]] if row[0] in instrument_fixers else row[0],
                mappings,
            )

    return map


def parse_if(clause):
    # print(f"parsing |{clause}|")
    # Go through potential cases
    match = re.match(r"IF = '(.)', Increase \[(.*)\]\[(.*)\]", clause)
    if match is not None:
        # case 1: format is "IF = 'x'", INCREMENT
        # Group 0: x, Group 1: mapped event, Group 2: mapped field, append group 3 to be "i"
        groups = list(match.groups())
        groups.append(
            "i"
        )  # This tells the reader that rather than setting [mapped_event][mapped_field] to a new value, just increment it by 1
        return groups

    match = re.match(r"IF \[.*\((.)\)\] = '1', \[(.*)\]\[(.*)\((.)\)\]", clause)
    if match is not None:
        # case 2: format is "IF [field(x)] = '1'", SET
        # Group 0: x, Group 1: mapped event, Group 2: mapped field, Group 3: Value to set
        groups = match.groups()
        return groups

    match = re.match(r"IF = '(.)', \[(.*)\]\[(.*)\] = '(.*)'", clause)
    if match is not None:
        # case 3: format is "IF = 'x'", SET
        # Group 0: x, Group 1: mapped event, Group 2: mapped field, Group 3: Value to set
        groups = match.groups()
        return groups

    match = re.match(r"IF <> '(\d)' OR '(\d)': \[(.*)\]\[(.*)\] = '(.*)'", clause)
    if match is not None:
        # case 4: format is "IF <> 'x1' or 'x2'":, SET
        # Group 0: x1, Group 1: x2, Group 2: mapped event, Group 3: mapped field, Group 4: Value to set
        groups = match.groups()
        return ((groups[0], groups[1]), groups[2], groups[3], groups[4])
    
    match = re.match(r"IF = '(\d)': \[(.*)\]\[(.*)\] = '(.*)'", clause)
    if match is not None:
        # case 5: format is "IF = 'x1':, SET
        # Group 0: x, Group 1: mapped event, Group 2: mapped field, Group 3: Value to set
        groups = match.groups()
        return groups

    match = re.match(r"IF \[current-instance\] = '(\d)', \[(.*)\]\[(.*)\] = '(.*)'", clause)
    if match is not None:
        # case 6: format is "IF [current-instance] = 'x', SET"
        # Group 0: ("instance", x), Group 1: mapped event, Group 2: mapped field, Group 3: Value to set
        groups = list(match.groups())
        groups[0] = ("instance", groups[0])
        return groups
    
    match = re.match(r"IF \[(.*)\]\[(.*)\] = '(\d)' OR '(\d)', \[(.*)\]\[(.*)\]", clause)
    if match is not None:
        # case 7: if [event1][field1] = 'j or k', [event2][field2] = x
        # Group 0: event1, Group 1: field1, Group 2: j, Group 3: k, Group 4: event2, Group 5: field2
        groups = match.groups()
        return (("foreign", (groups[0], groups[1]), (groups[2], groups[3])), groups[4], groups[5], "this")

    if match is None:
        raise ValueError(f"{clause} has no matching regex")


if __name__ == "__main__":
    print(generate_map("Map of Variables from V2 to V3 (FINAL - 300126).csv"))


