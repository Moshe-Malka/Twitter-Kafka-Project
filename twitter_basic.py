from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import credentials
from pykafka import KafkaClient

class MyKafkaStreamListener(StreamListener):
    def on_data(self, data):
        print(data)
        client = KafkaClient('127.0.0.1:9092')
        topic = client.topics['twitter_tweets']
        producer = topic.get_sync_producer()
        producer.produce(data.encode('ascii'))
        return True

    def on_error(self, status_code):
        print(f"Error : {status_code}")

if __name__ == '__main__':
    auth = OAuthHandler(credentials.API_KEY, credentials.API_SECRET_KEY)
    auth.set_access_token(credentials.ACCESS_TOKEN, credentials.ACCESS_TOKEN_SECRET)
    
    stream = Stream(auth, MyKafkaStreamListener())
    stream.filter(track=['covid'])
