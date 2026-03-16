# --- DSR extraction ---
dsr_data = all_jsons.get("dsr")

if dsr_data:
    pp_name = dsr_data.get("Project_Proponent_Name", "Not provided")
    serial_list = dsr_data.get("Corresponding_Serial_Number", [])

    # handle list case
    if isinstance(serial_list, list) and serial_list:
        serial_no = ", ".join(map(str, serial_list))
    else:
        serial_no = "Not provided"

    line4_dsr = (
        f"The DSR Approval page has been submitted. "
        f"The Project Proponent Name '{pp_name}' is listed in the DSR at Serial Number {serial_no}."
    )
else:
    line4_dsr = "The DSR Approval page and corresponding entry number were not provided in the submitted documents."
