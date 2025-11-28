import boto3
import json
import os
s3 = boto3.client("s3")
def read_control(default_ts="1972-01-01T00:00:00Z"):
    try:
        print(os.environ.get("AWS_ACCESS_KEY_ID"))
        
        obj = s3.get_object(Bucket="glue-practice-31052025", Key="staging/control/control_f.json")
        payload = json.loads(obj["Body"].read().decode("utf-8"))
        print("last_updated_ts from json: ", payload.get("last_updated_ts", default_ts))
        return payload.get("last_updated_ts", default_ts)
    except s3.exceptions.NoSuchKey:
        return default_ts

def main():
    last_updated_ts = read_control()
    print("Control file read successfully with last_updated_ts: ", last_updated_ts) 

if __name__ == "__main__":
    main()