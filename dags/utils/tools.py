
def get_key_arguments(args: dict)-> list:
    """
    Get the key arguments from the given arguments.
    """
    # Filter out the keys that are not in the list
    keys_to_clean = [x for x in args]
    return keys_to_clean





if __name__ == "__main__":
    # Example usage
    # 
    args = {
    "dag_id": "aws_glue_paquet_to_json",
    "job_name": "test pyspark",
    "script_location": "s3://aws-glue-assets-123456789101-us-west-1/scripts/test.py",
    "iam_role_name": "GlueExecutionRole",
    "region": "us-west-1",
    "script_args": {
        "--TempDir": "s3://aws-glue-assets-123456789101-us-west-1/temp/",
        "--job-bookmark-option": "job-bookmark-enable",
        "--input_path": "s3://aws-glue-assets-123456789101-us-west-1/input/",
        "--output_path": "s3://aws-glue-assets-123456789101-us-west-1/output/",
        "--enable-metrics": "",
        "--enable-continuous-cloudwatch-log": "",
        "--enable-s3-parquet-optimized-writer": ""  
    },
    "schedule_interval": "@daily",
    "start_date": "2024-04-05",
    "catchup": False,
    "email": ["testman@gmail.com", "tester@gmail.com"],
    "owner": "Diego",
    "description": "This DAG processes parquet files to json format using AWS Glue."
}
    keys_to_clean = get_key_arguments(args["script_args"])
    # Example of how to use the function
    print(keys_to_clean)  # Output: ['key1', 'key2']