import boto3
import urllib.parse
import json

glue = boto3.client("glue")

def lambda_handler(event, context):
    print(json.dumps(event, indent=2))

    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]

        key = urllib.parse.unquote_plus(
            record["s3"]["object"]["key"]
        )

        print(f"File uploaded: s3://{bucket}/{key}")

        response = glue.start_job_run(
            JobName="zomato-glue-etl-job",
            Arguments={
                "--RAW_BUCKET": bucket,
                "--RAW_KEY": key,
                "--OUTPUT_PATH": "s3://zomato-dataeng-processed/curated/"
            }
        )

        print(response)

    return {
        "statusCode": 200,
        "body": "Glue Job Triggered"
    }