from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream

API_KEY = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
API_SECRET_KEY = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
ACCESS_TOKEN = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
ACCESS_TOKEN_SECRET = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'

class MyKafkaStreamListener(StreamListener):
    def on_data(self, data):
        print(data)
        return True

    def on_error(self, status_code):
        print(f"Error : {status_code}")

if __name__ == '__main__':
    auth = OAuthHandler(API_KEY, API_SECRET_KEY)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    stream = Stream(auth, MyKafkaStreamListener())
    stream.filter(track=['covid'])
