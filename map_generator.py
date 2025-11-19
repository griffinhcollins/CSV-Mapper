import csv
import numpy as np
import re
import json


def get_basic_map():
    rows = []
    with open("Map of Variables from V2 to V3 v4.csv") as f:
        reader = csv.reader(f, dialect="excel")
        for row in reader:
            rows.append(row)

    rows = np.array(rows[7:])

    
    v2_event_lookup = {
        "study_details" : "study_details_arm_1",
        "study_intervention" : "study_details_arm_1",
        "study_registration" : "study_details_arm_1",
        "study_contracts" : "contracts_and_insu_arm_1",
        "agreements" : "contracts_and_insu_arm_1",
        "data_storage_and_archive" : "data_storage_and_a_arm_1",
        "cgc_review" : "cgc_review_arm_1",
        "annual_reporting" : "annual_reporting_arm_1",
        "annual_safety_reports" : "annual_reporting_arm_1",
        "governance_application" : "for_participating_arm_1",
        "governance_approval" : "for_participating_arm_1",
        "amendment_applications" : "for_participating_arm_1",
    }
    
    v3_event_lookup = {
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
    
    instrument_fixers = { # amendment applications is the old repeat instrument, governance is what it matches to
        "amendment_applications" : "governance_amendments"
    }

    # wip = ["investproduct_gmp", "check_insurance", "full_au_sites_num"]
    wip = []

    map = {}
    with open("logs/map_gen_logs.txt", "w+") as l:
        for row in rows:
            if row[2] == row[3] == "" or row[2] == "NOT USED" or row[3] in wip:
                continue

            mappings = []
            if len(row) > 4 and row[4] != "":
                # Special cases (remap and plus)
                cases = str.split(row[4], "//")
                for case in cases:
                    case = str.strip(case)
                    if str.startswith(case, "Plus:"):
                        # Do something else as well
                        plusdata = re.findall(".*\[(.*)\]\[(.*)\] = '(\d)'", row[4])[0]
                        mappings.append((v3_event_lookup[plusdata[0]], plusdata[1], plusdata[2]))
                    elif str.startswith(case, "Responses changed:"):
                        casemap = case[18:].replace("'", "").replace(" ", "").split(",")
                        mappings.append(("remap", casemap))
                    else:
                        print(f"Not mapped: {case}")

            if row[2] in v3_event_lookup.keys():
                # Regular mapping
                mappings.append((f"{v3_event_lookup[row[2]]}", str(row[3])))
            else:
                # Special cases (concat, manual and IF)
                if str.startswith(row[2], "CONCATENATE"):
                    mappings.append(("concat", ""))
                elif str.startswith(row[2], "FOR MANUAL ALLOCATION"):
                    mappings.append(("manual", ""))
                elif str.startswith(row[2], "IF"):
                    # Do something else depending on the value
                    # mapping[0] is IF, mapping[1] is the map
                    clauses = row[2].split(";")
                    ifmap = {}
                    for clause in clauses:
                        clause = str.strip(clause)
                        if clause[3] == "=":
                            # case 1: format is "IF = 'x'", INCREMENT
                            # Group 0: x, Group 1: event, Group 2: field
                            groups = re.match(
                                r"IF = '(.)', Increase \[(.*)\]\[(.*)\]", clause
                            ).groups()
                            y = "i"

                        else:
                            # case 2: format is "IF [field(x)] = '1'", SET
                            # Group 0: x, Group 1: event, Group 2: field, Group 3: Value to set
                            groups = re.match(
                                r"IF \[.*\((.)\)\] = '1', \[(.*)\]\[(.*)\((.)\)\]", clause
                            ).groups()
                            y = groups[3]
                        x = groups[0]
                        mapped_event = v3_event_lookup[groups[1]]
                        mapped_field = groups[2]
                        ifmap[x] = (mapped_event, mapped_field, y)
                    mappings.append(("IF", ifmap))

                else:
                    l.write(str(row))
                    continue
            
            
            # Bonus mappings that apply to all studies
            if row[1] == "trial_no":
                mappings.append((f"{v3_event_lookup["eoi"]}", "florey_yn", "1"))
                mappings.append((f"{v3_event_lookup["eoi"]}", "ethics_yn", "1"))
                
                
            map[(v2_event_lookup[row[0]], str(row[1]))] = (instrument_fixers[row[0]] if row[0] in instrument_fixers else row[0], mappings)

    return map


if __name__ == "__main__":
    print(get_basic_map())
