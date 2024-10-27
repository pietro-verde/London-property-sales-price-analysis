## Contains configuration settings that can be changed manually between dag runs.

dag_configs = {
    "csv_url" : "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv",
    # "csv_url" : 'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv',
    "write_mode" : "overwrite",
    "epc_file_path" : "/data/epc-files.pkl"
}