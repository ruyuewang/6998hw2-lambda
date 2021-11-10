import json
import boto3
import requests 
import requests_aws4auth
import datetime
import chardet
import idna
import urllib3
import certifi

bot = boto3.client('s3')

def lambda_handler(event, context):
    # test cfm
    s3_info = event['Records'][0]['s3']
    bucket_name = s3_info['bucket']['name']
    key_name = s3_info['object']['key']
    
    labels = getRekLabel(bucket_name, key_name)
    print("---------labels from Reko -------",labels)
    parsed_photo = parsePhoto(bucket_name, key_name, labels)
    print("---------this is parsed photo-------",parsed_photo)
    reply = uploadToES(parsed_photo)
    print("---------this is reply from ES-------", reply)
    return {
        'statusCode': 200,
        'body': json.dumps('Successfully indexed!')
    }

# function to generate label for current photo 
def getRekLabel(bucket_name, key_name):
    boto = boto3.session.Session('','',region_name='us-east-1')
    rekognition = boto.client('rekognition')
    response = rekognition.detect_labels(
        Image = {
            'S3Object' : {
                'Bucket' : bucket_name,
                'Name' : key_name
            }
        },
        MaxLabels=10
    )
    labels=[]
    for res in response['Labels']:
        labels.append(res['Name'])
    
    # Get S3 metadata using headObject() method
    headObject = bot.head_object(Bucket=bucket_name, Key=key_name)
    # assert 'Metadata' in headObject, "Oops! Forget to add metadata(custom field) while uploading imgs."
    print(headObject)
    metaData = headObject['Metadata']
    if len(metaData) !=0:
        custom_labels = metaData['customlabels'].split(',')
        labels.extend(custom_labels)
    return labels
    
# function to parse current photo in order to store in ES
def parsePhoto(bucket_name, key_name, labels):
    parsed_photo = {
        'objectKey': key_name,
        'bucket': bucket_name,
        'createdTimeStamp': datetime.datetime.now().strftime("%y-%m-%d %H:%M:%S"),
        'labels': labels
    }
    return parsed_photo
    
# function to connect to ES and upload photo 
def uploadToES(parsed_photo):
    endpoint = 'https://search-photos-qxhwqporwm5u6sqzzema4xdt6a.us-east-1.es.amazonaws.com/photos/photo'
    headers = { "Content-Type": "application/json" }
    response = requests.post(endpoint, auth=("photoalbum", "Photoalbum123!"), json=parsed_photo, headers=headers)
    return response 
    
