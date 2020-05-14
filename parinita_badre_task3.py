import tweepy
from collections import Counter
import random

consumer_api_key = 'CyzpyfN0puWOOHB8Y5klYWWUn'
consumer_api_key_secret = 'x0UCZpmDLqPWlM0aMvLTCnF0X8lviIEhCqDDRZvBOBFqqAv9T0'

access_token = '1145209298974232576-NbPL93SOTGVfbhSGsZ1EF6ZVN9BqNF'
access_token_secret = 'jYM8EOJ4LZ2hLnnKfBCWNxoibWeyJb2WHFoHMrKz7GBOb'

sequence_number = 0
size = 150
tags_counter = Counter()
tweet_tags = []


class TwitterStreamListener(tweepy.StreamListener):
    def on_status(self, tweet):
        global sequence_number, size, tweet_tags
        hashtag_list = tweet.entities['hashtags']
        hashtag_text = []
        for i in hashtag_list:
            hashtag_text.append(i['text'])
        if (len(hashtag_list) != 0):
            sequence_number += 1

            if sequence_number <= size:
                tweet_tags.append(hashtag_text)
                tags_counter.update(hashtag_text)

            else:
                probability = random.randint(0, sequence_number)
                if probability < size:
                    index_removed = random.randint(0, size - 1)
                    tags_remove = tweet_tags[index_removed]

                    for tag in tags_remove:
                        tags_counter[tag] -=1
                    tweet_tags[index_removed] = hashtag_text
                    tags_counter.update(hashtag_text)

            reverse_tags_counter = dict()
            for tag, freq in tags_counter.items():
                if freq in reverse_tags_counter:
                    reverse_tags_counter[freq].append(tag)
                else:
                    reverse_tags_counter[freq] = [tag]
            sorted_frequent_tags = sorted(reverse_tags_counter.keys(), reverse=True)
            print("The number of tweets with tags from beginning:", str(sequence_number))

            if len(sorted_frequent_tags) < 5:
                for i in range(len(sorted_frequent_tags)):
                    count = sorted_frequent_tags[i]
                    tag_list = sorted(reverse_tags_counter[count])
                    for tag in tag_list:
                        print(tag + " : " + str(count))
            else:
                for i in range(0, 5):
                    count = sorted_frequent_tags[i]
                    tag_list = sorted(reverse_tags_counter[count])
                    for tag in tag_list:
                        print(tag + " : " + str(count))
            print("")


auth = tweepy.OAuthHandler(consumer_api_key, consumer_api_key_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

stream_listener = tweepy.Stream(auth=api.auth, listener=TwitterStreamListener())
tweets = stream_listener.filter(track="#")
