import tweepy
import keys as k

auth=tweepy.OAuthHandler(k.consumer_API_key,k.consumer_API_secret_key)
auth.set_access_token(k.Access_token,k.Access_token_secret)

api=tweepy.API(auth)
i=0
public_tweets=api.home_timeline(tweet_mode='extended')
some_user=api.get_user('@elonmusk')
print(some_user.screen_name)
print(some_user.followers_count)
print("")
for tweet in public_tweets:
    print(tweet.full_text)
    i+=1
    if i==10:
        break