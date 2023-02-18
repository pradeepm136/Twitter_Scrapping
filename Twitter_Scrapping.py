# importing libraries and packages
import snscrape.modules.twitter as sntwitter
import pandas as pd
import streamlit as st
from pymongo import MongoClient
import json
import base64


# Using TwitterSearchScraper to scrape data and append tweets to list
def scraping_tweets(hashtag, start_date, end_date, tweet_limit):
    tweet_list = []
    for i, tweet in enumerate(
        sntwitter.TwitterSearchScraper(f'{hashtag} since:{start_date} until:{end_date}').get_items()):  # declare a username or search keyword
        data = [tweet.date, tweet.id, tweet.content, tweet.user.username, tweet.likeCount, tweet.retweetCount, tweet.url,
         tweet.replyCount, tweet.lang, tweet.source]
        
        tweet_list.append(data)
        if i == tweet_limit:
            break

    return tweet_list

def create_df(tweets_list): #Creating dataframe
    tweet_data = pd.DataFrame(tweets_list,
        columns=['Date', 'Tweet ID', 'Tweet Content', 'Tweet Username', 'Like Count', 'Retweet Count', 'URL',
                            'Reply Count', 'Language', 'Source'])
    return tweet_data


# Using streamlit for GUI
st.title("Scrape the Tweets")

# Get input from user for username/hashtag or search keywords
hashtag = st.text_input("Enter the Username/Keyword to search:")
start_date = st.date_input("Select start date:", key="start_date")
end_date = st.date_input("Select end date:", key="end_date")
tweet_limit = st.number_input("Enter the number of tweet you need:", key="limit")


# Scrape tweets

if st.button("Scrape Tweets"):
    tweet = scraping_tweets(hashtag, start_date, end_date, tweet_limit)
    tweet_data = create_df(tweet)
    st.dataframe(tweet_data)

    # Download as csv

    st.write("Saving dataframe as csv")
    csv = tweet_data.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="tweet_data.csv">Download CSV File</a>'
    st.markdown(href, unsafe_allow_html=True)

    # Download as JSON

    st.write("Saving dataframe as json")
    json_string = tweet_data.to_json(indent=2)
    b64 = base64.b64encode(json_string.encode()).decode()
    href = f'<a href="data:file/json;base64,{b64}" download="tweet_data.json">Download json File</a>'
    st.markdown(href, unsafe_allow_html=True)

    # Upload to mongoDB
    st.cache_data(ttl=600)
if st.button("Upload to MongoDB"):
    tweet = scraping_tweets(hashtag, start_date, end_date, tweet_limit)
    tweet_data = create_df(tweet)

    client = MongoClient('mongodb://localhost:27017/')
    db = client["Twitter_db_streamlit"]
    collection = db['tweet']
    tweet_data_json = json.loads(tweet_data.to_json(orient='records'))
    collection.insert_many(tweet_data_json)
    st.success('Uploaded to MongoDB')
