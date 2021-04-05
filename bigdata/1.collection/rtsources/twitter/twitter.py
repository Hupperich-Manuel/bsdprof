import logging
import json
import argparse
import tweepy

class TwitterStreamListener(tweepy.StreamListener):

  def on_status(self, status):
    print(status.text)

# main
logging.basicConfig(level=logging.DEBUG)
parser = argparse.ArgumentParser()
parser.add_argument("consumerKey", help="Twitter App's consumer key")
parser.add_argument("consumerSecret", help="Twitter App's consumer secret")
parser.add_argument("accessToken", help="Twitter App's access token")
parser.add_argument("accessTokenSecret", help="Twitter App's access token secret")
args = parser.parse_args()
logging.debug("main: Starting streaming connection with Twitter via the following keys: (%s,%s)" % 
              (args.consumerKey, args.consumerSecret))

# 1. Authentication against Twitter
auth = tweepy.OAuthHandler(args.consumerKey, args.consumerSecret)
auth.set_access_token(args.accessToken, args.accessTokenSecret)

api = tweepy.API(auth, wait_on_rate_limit=True,
                 wait_on_rate_limit_notify=True)

# 2. 
#tsl = TwitterStreamListener()
#myStream = tweepy.Stream(auth = api.auth, listener=tsl)
#myStream.filter(track=['python'])
#myStream.sample()
tweets_listener = TwitterStreamListener(api)
stream = tweepy.Stream(api.auth, tweets_listener)
#stream.filter(track=["Python", "Django", "Tweepy"], languages=["en"])
stream.sample(languages=["es"])
