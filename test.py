from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import credentials, config
import json
from pprint import pprint
from pykafka import KafkaClient

class MyKafkaStreamListener(StreamListener):
    def __init__(self):
        self.client = KafkaClient(config.KAFKA_HOST)
        self.topic = self.client.topics[config.TOPIC]
        self.producer = self.topic.get_sync_producer()
        print('[*] Starting Stream...')

    def filter_data(self, data):
        json_data = json.loads(data)
        user_data = json_data.get('user', {})
        return {
            'created_at' : json_data.get('created_at', None),
            'id' : json_data.get('id', None),
            'truncated' : json_data.get('truncated', None),
            'text' : json_data.get('text', None),
            'source' : json_data.get('source', None),
            'coordinates' : json_data.get('coordinates', None),
            'place' : json_data.get('place', None),
            'retweet_count' : json_data.get('retweet_count', None),
            'user_name' : user_data.get('name', None),
            'user_joined_twitter' : user_data.get('created_at', None),
            'user_friends_count' : user_data.get('friends_count', None),
            'user_followers_count' : user_data.get('followers_count', None),
            'user_statuses_count' : user_data.get('statuses_count', None),
            'user_location' : user_data.get('location', None),
            'user_timezone' : user_data.get('time_zone', None),
            'user_screen_name' : user_data.get('screen_name', None),
            'user_description' : user_data.get('description', None),
            'user_verified' : user_data.get('verified', None),
            'user_url' : user_data.get('url', None),
            'user_language' : user_data.get('lang', None),
            'user_profile_image' : user_data.get('profile_image_url', None)
        }

    def on_data(self, data):
        # pprint(json.loads(data))
        filtered_data = self.filter_data(data)
        message = json.dumps(filtered_data).encode('ascii')
        self.producer.produce(message)
        return True
    
    def on_error(self, status_code):
        print(f"[*] Error : {status_code}")
        if status_code == 420:
            print("[*] Disconnecting Stream...")
            #returning False in on_error disconnects the stream
            return False
        else:
            print("[*] Reconnecting Stream...")
            return True

if __name__ == '__main__':
    # Auth
    auth = OAuthHandler(credentials.API_KEY, credentials.API_SECRET_KEY)
    auth.set_access_token(credentials.ACCESS_TOKEN, credentials.ACCESS_TOKEN_SECRET)
    # Stream initialization
    try:
        stream = Stream(auth, MyKafkaStreamListener(), tweet_mode='extended')
        stream.filter(track=config.TWEETS_TO_TRACK)
    except KeyboardInterrupt:
        print(f"\n[*] Keyboard Interrupt")
        stream.disconnect()
