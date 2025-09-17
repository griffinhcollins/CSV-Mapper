import redcap_data


aep_upload = redcap_data.FileUploadInfo(
    "3960-6",
    "contracts_and_insu_arm_1",
    "agree_file",
    3,
    "contracts_and_insu_arm_1",
    "agree_file",
)


print(
    redcap_data.export_file(
        "B143B1B57E6F071FC41C5D08F9226EFB",
        aep_upload,
        "AEP_VCGS Services Agreement CMA and data provision_clean.docx",
    )
)
