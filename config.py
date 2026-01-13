PARTNER_CONFIG = {
    "ACME": {
        "file_path": "data/acme.txt",
        "delimiter": "|",
        "partner_code": "ACME",
        "column_mapping": {
            "MBI": "external_id",
            "FNAME": "first_name",
            "LNAME": "last_name",
            "DOB": "dob",
            "EMAIL": "email",
            "PHONE": "phone"
        }
    },
    "BETTERCARE": {
        "file_path": "data/bettercare.csv",
        "delimiter": ",",
        "partner_code": "BETTERCARE",
        "column_mapping": {
            "subscriber_id": "external_id",
            "first_name": "first_name",
            "last_name": "last_name",
            "date_of_birth": "dob",
            "email": "email",
            "phone": "phone"
        }
    }
}
