import requests
import numpy as np
import xml.etree.ElementTree as ET
import os
import csv


class FileUploadInfo:
    def __init__(
        self, study_id, event, field, repeat_index, mapped_event, mapped_field, manual_save=False
    ) -> None:
        self.trial_no = study_id
        self.event = event
        self.field = field
        self.repeat_index = repeat_index
        self.mapped_event = mapped_event
        self.mapped_field = mapped_field
        self.manual_save = manual_save
        pass
    
    def hash(self):
        return "-".join([str(a) for a in [self.trial_no, self.event, self.field, self.repeat_index]])


def project_xml(token, testcap=False):

    data = {
        "token": token,
        "content": "project_xml",
        "format": "json",
        "returnMetadataOnly": "false",
        "exportFiles": "false",
        "exportSurveyFields": "false",
        "exportDataAccessGroups": "false",
        "returnFormat": "json",
    }
    r = requests.post(f'https://{"testcap" if testcap else "redcap"}.florey.edu.au/api/', data=data)
    print("XML HTTP Status: " + str(r.status_code))
    return ET.fromstring(r.text)


def get_events(token, testcap=False):
    data = {
        "token": token,
        "content": "event",
        "format": "json",
        "returnFormat": "json",
    }
    r = requests.post(f"https://{"testcap" if testcap else "redcap"}.florey.edu.au/api/", data=data)
    print("Events HTTP Status: " + str(r.status_code))
    return r.json()


def get_dictionary(token, testcap=False):
    reformat = False
    data = {
        "token": token,
        "content": "metadata",
        "format": "csv",
        "returnFormat": "json",
    }
    r = requests.post(f'https://{"testcap" if testcap else "redcap"}.florey.edu.au/api/', data=data)
    print("Dictionary HTTP Status: " + str(r.status_code))

    csv_raw = []

    for row in r.text.split("\n")[:-1]:
        csv_raw.append(row)

    array = []
    for read_row in csv.reader(csv_raw):
        array.append(read_row)

    return np.array(array)


# Downloads the requested file into tmp and returns its filename
def import_file(token: str, filedata: FileUploadInfo, testcap=False):
    data = {
        "token": token,
        "content": "file",
        "action": "export",
        "record": filedata.trial_no,
        "field": filedata.field,
        "event": filedata.event,
        "repeat_instance": filedata.repeat_index,
        "returnFormat": "json",
    }
    r = requests.post(f'https://{"testcap" if testcap else "redcap"}.florey.edu.au/api/', data=data)

    # We need to do another import just to get the filename, as it isn't included in eav data
    filename_request = {
        "token": token,
        "content": "record",
        "action": "export",
        "format": "json",
        "type": "flat",
        "records[0]": filedata.trial_no,
        "fields[0]": filedata.field,
        "forms[0]": "study_details",
        "forms[1]": "agreements",
        "events[0]": filedata.event,
        "rawOrLabel": "raw",
        "rawOrLabelHeaders": "raw",
        "exportCheckboxLabel": "false",
        "exportSurveyFields": "false",
        "exportDataAccessGroups": "false",
        "returnFormat": "json",
    }
    filename = ""
    filename_response = requests.post("https://redcap.florey.edu.au/api/", data=filename_request)
    print("Filename Request HTTP Status: " + str(filename_response.status_code))
    r_index = filedata.repeat_index
    if r_index != "":
        r_index = int(r_index)
    for j in filename_response.json():
        if j["redcap_repeat_instance"] == r_index:
            print("Found correct instance")
            filename = j[filedata.field]
            break
    if filename == "":
        print(filedata.repeat_index)
        print(filename_response.json())
        raise Exception("Filename error")

    # print(f"Filename: {filename}")
    if filedata.manual_save:
        path = f"saved_files/{filedata.trial_no}/{filedata.event}/{filedata.field}/"
    else:
        path = "tmp/"
    if not os.path.exists(path):
        os.makedirs(path)
    with open(path + filename, "wb+") as f:
        f.write(r.content)
        f.close()
    return path + filename


def export_file(token, filedata: FileUploadInfo, filepath, testcap=False):
    data = {
        "token": token,
        "content": "file",
        "action": "import",
        "record": filedata.trial_no,
        "field": filedata.mapped_field,
        "event": filedata.mapped_event,
        "repeat_instance": filedata.repeat_index,
        "returnFormat": "json",
    }
    with open(filepath, "rb") as file_obj:
        r = requests.post(
            f'https://{"testcap" if testcap else "redcap"}.florey.edu.au/api/',
            data=data,
            files={"file": file_obj},
        )
        file_obj.close()
    print("Export HTTP Status: " + str(r.status_code))
    if r.status_code != 200:
        with open("logs/file_upload_errors.txt", "a+") as e:
            e.write(str(data))
            e.write(str(r.json()))
            e.write("\n")
            print("Wrote error to file_upload_errors.txt")
    return r


def get_all_fields_of_type(token, field_type):
    all_fields = get_dictionary(token)
    field_types = dict(x for x in all_fields[1:, 0:4:3])
    matching_fields = []
    for k in field_types:
        if field_types[k] == field_type:
            matching_fields.append(k)
    return matching_fields


def get_radio_map(token, label_key):
    radio_fields = get_all_fields_of_type(token, "radio")

    data = {
        "token": token,
        "content": "metadata",
        "format": "json",
        "returnFormat": "json",
        "fields": radio_fields,
    }
    r = requests.post("https://redcap.florey.edu.au/api/", data=data)
    print("HTTP Status: " + str(r.status_code))
    j = r.json()
    radio_map = {}
    for field in j:
        choices = field["select_choices_or_calculations"]
        if "Yes" in choices:
            id_map = {}
            for label in choices.split("|"):
                label = label.strip()
                if label_key:
                    id_map[label[3:]] = label[0]
                else:
                    id_map[label[0]] = label[3:]
            radio_map[field["field_name"]] = id_map
    return radio_map


def get_users(token):
    data = {"token": token, "content": "user", "format": "json", "returnFormat": "json"}
    r = requests.post("https://redcap.florey.edu.au/api/", data=data)
    return r.json()


def import_users(token, users):
    data = {
        "token": token,
        "content": "user",
        "format": "json",
        "data": users,
        "returnFormat": "json",
    }
    print(data)
    r = requests.post("https://redcap.florey.edu.au/api/", data=data)
    print("HTTP Status: " + str(r.status_code))
    print(r.json())


def get_instruments(token):
    data = {"token": token, "content": "instrument", "format": "json", "returnFormat": "json"}
    r = requests.post("https://redcap.florey.edu.au/api/", data=data)
    print("HTTP Status: " + str(r.status_code))
    return r.json()


def get_dags(token):
    data = {"token": token, "content": "dag", "format": "json", "returnFormat": "json"}
    r = requests.post("https://redcap.florey.edu.au/api/", data=data)
    print("HTTP Status: " + str(r.status_code))
    return r.json()

def import_dags(token, dags):
    data = {
    'token': token,
    'content': 'dag',
    'action': 'import',
    'format': 'json',
    'data': dags,
    'returnFormat': 'json'
    }
    r = requests.post('https://redcap.florey.edu.au/api/',data=data)
    print('HTTP Status: ' + str(r.status_code))
    return r.json()


if __name__ == "__main__":
    rows = []
    with open("Map of Variables from V2 to V3 v4.csv") as f:
        reader = csv.reader(f, dialect="excel")
        for row in reader:
            rows.append(row)

    rows = np.array(rows[7:])

    in_project = get_radio_map("AAF352AC73709A6AE89C45881A227FBB", False)
    out_project = get_radio_map("490755B7DA96CF2C7392DD9D2879238D", False)

    for row in rows:
        if row[1] in in_project and row[2] != "NOT USED" and row[3] in out_project:
            print(
                f"{row[1]} labels: {in_project[row[1]]} map to {row[3]} labels: {out_project[row[3]]} "
            )

    # project_xml('2A7FB1BE285EA4CC00BB2920F97F5865')
