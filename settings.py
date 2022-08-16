# project = "test"
project = "dev"

if project == "test":

    # export GOOGLE_APPLICATION_CREDENTIALS="/home/yuong/work/pyproj/Keys/test_local_access.json"
    project_id = "testing-33c79"
    firebase_database = "testing-33c79"
    fx_api_key = "test_alpha_vantage_api"
    firestore_api_key = "test_firebase_db"
    google_sheets_api_key = "test_googlesheets"
    schedule_function_key = "test_schedule_function_key"
    firebase_auth_api_key = "test_firebase_auth"

else:

    # export GOOGLE_APPLICATION_CREDENTIALS="/home/yuong/work/pyproj/Keys/blockmacro_local_access.json"
    project_id = "blockmacro-7b611"
    firebase_database = "blockmacro-7b611"
    fx_api_key = "blockmacro_alpha_vantage_api"
    firestore_api_key = "blockmacro_firebase_db"
    google_sheets_api_key = "blockmacro_googlesheets"
    schedule_function_key = "blockmacro_schedule_function_key"
    firebase_auth_api_key = "blockmacro_firebase_auth"