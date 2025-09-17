import requests
import numpy as np
import xml.etree.ElementTree as ET
import os
import csv


class FileUploadInfo:
    def __init__(
        self, study_id, event, field, repeat_index, mapped_event, mapped_field
    ) -> None:
        self.trial_no = study_id
        self.event = event
        self.field = field
        self.repeat_index = repeat_index
        self.mapped_event = mapped_event
        self.mapped_field = mapped_field
        pass


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
    r = requests.post(
        f'https://{"testcap" if testcap else "redcap"}.florey.edu.au/api/', data=data
    )
    print("XML HTTP Status: " + str(r.status_code))
    return ET.fromstring(r.text)


def get_events(token, testcap=False):
    data = {
        "token": token,
        "content": "event",
        "format": "json",
        "returnFormat": "json",
    }
    r = requests.post(
        f"https://{"testcap" if testcap else "redcap"}.florey.edu.au/api/", data=data
    )
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
    r = requests.post(
        f'https://{"testcap" if testcap else "redcap"}.florey.edu.au/api/', data=data
    )
    print("Dictionary HTTP Status: " + str(r.status_code))

    csv_raw = []

    for row in r.text.split("\n")[:-1]:
        csv_raw.append(row)

    array = []
    for read_row in csv.reader(csv_raw):
        array.append(read_row)

    return np.array(array)

    # return


# Downloads the requested file into tmp and returns its filename
def import_file(token, filedata: FileUploadInfo, testcap=False):
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
    r = requests.post(
        f'https://{"testcap" if testcap else "redcap"}.florey.edu.au/api/', data=data
    )

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
    filename_response = requests.post(
        "https://redcap.florey.edu.au/api/", data=filename_request
    )
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

    print("Import HTTP Status: " + str(r.status_code))
    # print(f"Filename: {filename}")
    if not os.path.exists("tmp/"):
        os.makedirs("tmp/")
    with open(f"tmp/{filename}", "wb+") as f:
        f.write(r.content)
        f.close()
    return filename


def export_file(token, filedata: FileUploadInfo, filename, testcap=False):
    data = {
        "token": token,
        "content": "file",
        "action": "import",
        "record": filedata.trial_no,
        "field": filedata.field,
        "event": filedata.event,
        "repeat_instance": filedata.repeat_index,
        "returnFormat": "json",
    }
    with open(f"tmp/{filename}", "rb") as file_obj:
        r = requests.post(
            f'https://{"testcap" if testcap else "redcap"}.florey.edu.au/api/',
            data=data,
            files={"file": file_obj},
        )
        file_obj.close()
    print("Export HTTP Status: " + str(r.status_code))
    return r


if __name__ == "__main__":
    print(get_dictionary("2A7FB1BE285EA4CC00BB2920F97F5865"))
    # project_xml('2A7FB1BE285EA4CC00BB2920F97F5865')
