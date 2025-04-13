import praw
from textblob import TextBlob

# Reddit API credentials
reddit = praw.Reddit(
    client_id="C_2l75-htxMd-IQvc6R7BQ",
    client_secret="4g0g7OUt0bbrvOZykAYvNk36sFV3Bw",
    user_agent="SentimentAnalysisApp"
)

# Choose a subreddit and get posts
subreddit = reddit.subreddit("Python")
for post in subreddit.hot(limit=10):
    print(f"📝 Title: {post.title}")
    blob = TextBlob(post.title)
    sentiment = blob.sentiment.polarity
    print(f"📊 Sentiment Score: {sentiment}")
    print("😊 Positive" if sentiment > 0 else "😐 Neutral" if sentiment == 0 else "😠 Negative")
    print("-" * 40)
