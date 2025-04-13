from flask import Flask, render_template, jsonify
from collections import Counter
import random
import threading
import time
import praw
from textblob import TextBlob
import requests
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dashboard.log'),
        logging.StreamHandler()
    ]
)

# Sentiment counts
sentiment_counts = Counter({"positive": 0, "negative": 0, "neutral": 0})
reddit_sentiment_counts = Counter({"positive": 0, "negative": 0, "neutral": 0})
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
    logging.info("Starting Twitter simulation stream")
    while True:
        try:
            tweet, sentiment = random.choice(tweets)
            sentiment_counts[sentiment] += 1
            logging.info(f"[Tweet] {tweet} -> {sentiment} (Counts: {dict(sentiment_counts)})")
            time.sleep(2)
        except Exception as e:
            logging.error(f"[Tweet Stream Error] {e}")
            time.sleep(5)

# --- Reddit API Setup ---
reddit = praw.Reddit(
    client_id="C_2l75-htxMd-IQvc6R7BQ",
    client_secret="4g0g7OUt0bbrvOZykAYvNk36sFV3Bw",
    user_agent="SentimentAnalysisApp"
)

# --- NewsAPI Setup ---
news_api_key = '87f78437b6ce42fb883d26fe56af04d6'
news_url = 'https://newsapi.org/v2/top-headlines'

def analyze_sentiment(text):
    try:
        analysis = TextBlob(text)
        score = analysis.sentiment.polarity
        if score > 0.1:
            return "positive"
        elif score < -0.1:
            return "negative"
        else:
            return "neutral"
    except Exception as e:
        logging.error(f"[Sentiment Analysis Error] {e}")
        return "neutral"

def reddit_stream():
    logging.info("Starting Reddit stream")
    while True:
        try:
            subreddit = reddit.subreddit("Python")
            for post in subreddit.hot(limit=5):
                sentiment = analyze_sentiment(post.title)
                reddit_sentiment_counts[sentiment] += 1
                logging.info(f"[Reddit] {post.title} -> {sentiment} (Counts: {dict(reddit_sentiment_counts)})")
            time.sleep(10)
        except Exception as e:
            logging.error(f"[Reddit Error] {e}")
            time.sleep(30)

def fetch_news():
    params = {
        'apiKey': news_api_key,
        'country': 'us',
        'pageSize': 5
    }
    try:
        response = requests.get(news_url, params=params)
        response.raise_for_status()
        articles = response.json().get('articles', [])
        logging.info(f"[NewsAPI] Fetched {len(articles)} articles")
        return articles
    except Exception as e:
        logging.error(f"[NewsAPI Error] {e}")
        return []

def news_stream():
    logging.info("Starting NewsAPI stream")
    while True:
        try:
            articles = fetch_news()
            for article in articles:
                title = article.get('title', '')
                if title:
                    sentiment = analyze_sentiment(title)
                    news_sentiment_counts[sentiment] += 1
                    logging.info(f"[NewsAPI] {title} -> {sentiment} (Counts: {dict(news_sentiment_counts)})")
            time.sleep(10)
        except Exception as e:
            logging.error(f"[NewsAPI Error] {e}")
            time.sleep(30)

# --- Flask Routes ---
@app.route('/')
def index():
    logging.info("Serving index.html")
    return render_template('index.html')

@app.route('/data')
def data():
    logging.info(f"Serving Twitter data: {dict(sentiment_counts)}")
    return jsonify(dict(sentiment_counts))

@app.route('/reddit-data')
def reddit_data():
    logging.info(f"Serving Reddit data: {dict(reddit_sentiment_counts)}")
    return jsonify(dict(reddit_sentiment_counts))

@app.route('/news-data')
def news_data():
    logging.info(f"Serving NewsAPI data: {dict(news_sentiment_counts)}")
    return jsonify(dict(news_sentiment_counts))

# --- Run Flask App ---
if __name__ == '__main__':
    logging.info("Starting Flask app")
    threading.Thread(target=simulate_stream, daemon=True).start()
    threading.Thread(target=reddit_stream, daemon=True).start()
    threading.Thread(target=news_stream, daemon=True).start()
    app.run(debug=True, use_reloader=False)