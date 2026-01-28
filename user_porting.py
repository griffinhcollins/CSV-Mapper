import redcap_data


export_token = "622963362CB2339A4DF099C171BA7492"
import_token = "BC97BB08739A5CD4A5544CB0A48E68FC"


def port_users(from_token, to_token):
    users = redcap_data.get_users(from_token)
    valid_forms = [i["instrument_name"] for i in redcap_data.get_instruments(to_token)]
    for u in users:
        new_forms = {}
        for form in u["forms"].keys():
            if form in valid_forms:
                new_forms[form] = u["forms"][form]
        u["forms"] = new_forms
        u["forms_export"] = new_forms

    fixed_users = str(users).replace("'", '"')

    redcap_data.import_users(to_token, fixed_users)


def port_dag(from_token, to_token):
    dags = redcap_data.get_dags(from_token)
    for dag in dags:
        dag["unique_group_name"] = ""
    dags = str(dags).replace("'", '"')
    print(dags)
    print(redcap_data.import_dags(to_token, dags))


# port_dag(export_token, import_token)
port_users(export_token, import_token)