import json
import boto3 
import requests
import idna
import requests_aws4auth
import chardet
import urllib3
import certifi

def lambda_handler(event, context):
    # test 
    query = event['queryStringParameters']['q']
    labels = getLabels(query)
    print("----------labels is----------", labels)
    photo_paths = getPhotoPaths(labels)
    return{
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Headers': 'Content-Type, Access-Control-Allow-Headers, Authorization, X-Requested-With,x-api-key',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
        },
        'body': json.dumps(photo_paths)
    }

# function to get labels 
def getLabels(query):
    lex = boto3.client('lex-runtime')
    response = lex.post_text(
        botName='SearchBot',  
        botAlias='Test',
        userId="string",           
        inputText=query
    )
    
    labels=[]
    print("--------lambda response", response)
    if 'slots' not in response:
        print('No photo collection for query {}'.format(query))
    else:
        slot_val = response['slots']
        for key,value in slot_val.items():
            if value!=None:
                labels.append(value)
    return labels
    
# function to get all photo paths  
def getPhotoPaths(labels):
    photo_paths=[]
    for l in labels:
        host = 'https://search-photos-qxhwqporwm5u6sqzzema4xdt6a.us-east-1.es.amazonaws.com/photos'
        path = host + '/_search?q=labels:'+ l 
        headers = { "Content-Type": "application/json" }
        response = requests.get(path, headers=headers, auth=('photoalbum', 'Photoalbum123!'))
        response = response.json()
        for res in response['hits']['hits']:
            img_bucket = res['_source']['bucket']
            img_key_name = res['_source']['objectKey']
            
            # need to check the link format 
            img_url = 'https://s3.amazonaws.com/' + str(img_bucket) + '/' + str(img_key_name)
            if img_url not in photo_paths:
                photo_paths.append(img_url)
    return photo_paths
  
        
        
        
        
