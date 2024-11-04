## Contains configuration settings that can be changed manually between dag runs.

dag_configs = {
    "ppd_csv_url_complete" : "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv",
    "ppd_csv_url_month" : 'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv',
    "epc_file_path" : "/data/epc-files.pkl",
    "epc_file_test" : "/data/epc-files-test.pkl",
    "write_mode_complete" : "overwrite",
    "write_mode_month" : "append"
}