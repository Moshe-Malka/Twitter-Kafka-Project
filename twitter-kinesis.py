from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream
import os, json, base64
import boto3
from botocore.exceptions import ClientError

session = boto3.session.Session()
secretsmanager_client = session.client( service_name='secretsmanager', region_name=os.environ['REGION'] )

class MyStreamListener(StreamListener):
    def __init__(self):
        self.kinesis_client = boto3.client('kinesis')
        print('[*] Starting Stream...')

    def on_data(self, data):
        self.put_record(json.loads(data))
        return True
    
    def put_record(self, data):
        try:
            response = self.kinesis_client.put_record(
                StreamName='twitter-stream',
                Data=json.dumps(data),
                PartitionKey=data['id_str']
            )
        except Exception as e:
            print(e)

    def on_error(self, status_code):
        print(f"[*] Error : {status_code}")
        if status_code == 420:
            print("[*] Disconnecting Stream...")
            #returning False in on_error disconnects the stream
            return False
        else:
            print("[*] Reconnecting Stream...")
            return True
   
def get_secret(secret_name):
    try:
        secret_value = secretsmanager_client.get_secret_value( SecretId=secret_name )
        return json.loads(secret_value['SecretString'])
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e

def set_twitter_auth(creds):
    try:
        auth = OAuthHandler(creds.get('API_KEY', None), creds.get('API_SECRET_KEY', None))
        auth.set_access_token(creds.get('ACCESS_TOKEN', None), creds.get('ACCESS_TOKEN_SECRET', None))
        return auth
    except Exception as e:
        print(e)

if __name__ == '__main__':
    # Auth
    # aws secretsmanager put-secret-value --secret-id twitter-secrets --secret-string file://mycreds.json
    credentials = get_secret('twitter-secrets')
    auth = set_twitter_auth(credentials)
    
    # Stream initialization
    try:
        stream = Stream(auth, MyStreamListener())
        stream.filter(track=['trump'])
    except KeyboardInterrupt:
        print(f"\n[*] Keyboard Interrupt")
        stream.disconnect()
