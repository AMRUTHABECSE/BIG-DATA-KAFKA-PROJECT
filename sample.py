from flask import Flask, render_template, jsonify
from collections import Counter
import random
import threading
import time
import praw
from textblob import TextBlob
import requests

app = Flask(__name__)

# Simulated tweet sentiment counts
sentiment_counts = Counter({"positive": 0, "negative": 0, "neutral": 0})

# Reddit sentiment counts
reddit_sentiment_counts = Counter({"positive": 0, "negative": 0, "neutral": 0})

# News sentiment counts
news_sentiment_counts = Counter({"positive": 0, "negative": 0, "neutral": 0})

# --- Simulated Tweets ---
tweets = [
    ("I absolutely LOVE this service! Best thing ever!", "positive"),
    ("This is the WORST experience I've ever had!", "negative"),
    ("It's okay, nothing special.", "neutral"),
    ("Fantastic experience from start to finish! 10/10!", "positive"),
    ("Complete waste of time and money! Awful!", "negative"),
    ("Does what it says, I guess.", "neutral"),
    ("Horrible customer support! Unacceptable!", "negative"),
    ("Perfect solution for my needs! Outstanding!", "positive"),
    ("Using this daily now—can't live without it!", "positive"),
    ("Support ghosted me after the issue. Trash!", "negative"),
    ("Feels okay to use, but not amazing.", "neutral"),
    ("This just made my day better! Incredible work!", "positive")
]

def simulate_stream():
    while True:
        tweet, sentiment = random.choice(tweets)
        sentiment_counts[sentiment] += 1
        print(f"[Tweet] {tweet} -> {sentiment}")
        time.sleep(2)

# --- Reddit API Setup ---
reddit = praw.Reddit(
    client_id="C_2l75-htxMd-IQvc6R7BQ",
    client_secret="4g0g7OUt0bbrvOZykAYvNk36sFV3Bw",
    user_agent="SentimentAnalysisApp"
)

def analyze_sentiment(text):
    analysis = TextBlob(text)
    score = analysis.sentiment.polarity
    if score > 0.1:
        return "positive"
    elif score < -0.1:
        return "negative"
    else:
        return "neutral"

def reddit_stream():
    while True:
        try:
            subreddit = reddit.subreddit("Python")
            for post in subreddit.hot(limit=5):
                sentiment = analyze_sentiment(post.title)
                reddit_sentiment_counts[sentiment] += 1
                print(f"[Reddit] {post.title} -> {sentiment}")
            time.sleep(10)
        except Exception as e:
            print(f"[Reddit Error] {e}")
            time.sleep(30)

# --- NewsAPI Integration ---
news_api_key = '87f78437b6ce42fb883d26fe56af04d6'
news_url = 'https://newsapi.org/v2/top-headlines'

def fetch_news():
    params = {
        'apiKey': news_api_key,
        'country': 'us',  # You can change this to any country you want
        'pageSize': 5      # Get top 5 headlines
    }
    try:
        response = requests.get(news_url, params=params)
        articles = response.json().get('articles', [])
        return articles
    except Exception as e:
        print(f"[NewsAPI Error] {e}")
        return []

def news_stream():
    while True:
        try:
            articles = fetch_news()
            for article in articles:
                title = article['title']
                sentiment = analyze_sentiment(title)
                news_sentiment_counts[sentiment] += 1
                print(f"[NewsAPI] {title} -> {sentiment}")
            time.sleep(10)
        except Exception as e:
            print(f"[NewsAPI Error] {e}")
            time.sleep(30)

# --- Flask Routes ---
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/data')
def data():
    return jsonify(sentiment_counts)

@app.route('/reddit-data')
def reddit_data():
    return jsonify(reddit_sentiment_counts)

@app.route('/news-data')
def news_data():
    return jsonify(news_sentiment_counts)

# --- Run Flask App ---
if __name__ == '__main__':
    threading.Thread(target=simulate_stream, daemon=True).start()
    threading.Thread(target=reddit_stream, daemon=True).start()
    threading.Thread(target=news_stream, daemon=True).start()
    app.run(debug=True)
