from settings import redis_object
import sys

if __name__ == '__main__':
    channel = "tweet_prediction"

    print ('Welcome to {channel}'.format(**locals()))

    while True:
        message = input('Enter a message: ')

        message_body = '{"text": "'+message+'", "sentiment": "+ve"}'
            

        if message.lower() == 'exit':
            break

        message = '{message_body}'.format(**locals()).encode('UTF-8')

        redis_object.publish(channel, message)
