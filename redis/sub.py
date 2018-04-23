from settings import redis_object
import sys

if __name__ == '__main__':
    channel = "tweet_prediction"

    pubsub = redis_object.pubsub()
    pubsub.subscribe(channel)

    print ('Listening to {channel}'.format(**locals()))

    while True:
        for item in pubsub.listen():
            if item['type'] == 'message':
                print (item['data'].decode('UTF-8'))