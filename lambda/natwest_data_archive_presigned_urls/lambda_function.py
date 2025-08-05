import boto3
import os
from botocore.exceptions import ClientError
import json
from urllib.parse import unquote

def lambda_handler(event, context):
    """
    Generates a pre-signed URL to download a file from S3.

    This function is triggered by an API Gateway request. The S3 object key
    is passed as a query string parameter named 'key'.
    """

    print(f"Received event: {json.dumps(event)}")

    try:
        s3_key = event['queryStringParameters']['key']
        s3_key = unquote(s3_key)
    except (KeyError, TypeError):
        return {
            'statusCode': 400,
            'body': json.dumps({'error': "Missing or invalid 'key' query string parameter."})
        }

    bucket_name = os.environ.get('S3_BUCKET_NAME', 'natwest-data-archive-vault')
    expiration = 300
    
    # --- FIX: Explicitly define the S3 bucket's region ---
    # This prevents any ambiguity about where the S3 client should look for the bucket.
    s3_region = 'eu-west-1' 
    
    print(f"Attempting to generate URL for bucket: {repr(bucket_name)} in region: {s3_region}")
    print(f"Attempting to generate URL for key: {repr(s3_key)}")

    # Create an S3 client, specifying the region
    s3_client = boto3.client('s3', region_name=s3_region)

    try:
        response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket_name,
                                                            'Key': s3_key},
                                                    ExpiresIn=expiration)
    except ClientError as e:
        print(f"Error generating pre-signed URL: {e}")
        if e.response['Error']['Code'] == 'NoSuchKey':
             return {
                'statusCode': 404,
                'body': json.dumps({'error': 'The specified file key does not exist in the bucket.'})
            }
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Could not generate the download link.'})
        }

    return {
        'statusCode': 302,
        'headers': {
            'Location': response
        }
    }
