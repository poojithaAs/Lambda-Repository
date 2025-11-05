import boto3, json, os, requests

def lambda_handler(event, context):
    secret_name = os.environ["DATABRICKS_SECRET_NAME"]
    region = os.environ["AWS_REGION"]

    sm = boto3.client("secretsmanager", region_name=region)
    secret_value = sm.get_secret_value(SecretId=secret_name)
    creds = json.loads(secret_value["SecretString"])

    databricks_url = creds["url"]
    token = creds["token"]
    job_id = creds["job_id"]

    headers = {"Authorization": f"Bearer {token}"}
    body = {"job_id": job_id}
    response = requests.post(f"{databricks_url}/api/2.1/jobs/run-now", json=body, headers=headers)

    print(f"Databricks Response: {response.status_code} - {response.text}")
    return {"statusCode": response.status_code, "body": response.text}
