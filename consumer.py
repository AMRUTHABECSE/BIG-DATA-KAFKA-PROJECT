from kafka import KafkaConsumer
from textblob import TextBlob
import requests
import logging
import time
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('consumer.log'),
        logging.StreamHandler()
    ]
)

def get_sentiment(text):
    try:
        polarity = TextBlob(text).sentiment.polarity
        if polarity > 0.2:  # More strict positive threshold
            return "positive"
        elif polarity < -0.2:  # More strict negative threshold
            return "negative"
        return "neutral"
    except Exception as e:
        logging.error(f"Error analyzing sentiment: {str(e)}")
        return "neutral"

def create_consumer():
    retries = 3
    for i in range(retries):
        try:
            return KafkaConsumer(
                'tweets',
                bootstrap_servers=['localhost:9092'],
                auto_offset_reset='earliest',
                consumer_timeout_ms=10000,
                group_id='sentiment-group',
                enable_auto_commit=True
            )
        except Exception as e:
            logging.error(f"Attempt {i+1} failed to connect to Kafka: {str(e)}")
            if i == retries - 1:
                raise
            time.sleep(2)

def update_flask(sentiment_count):
    retries = 3
    for i in range(retries):
        try:
            response = requests.post(
                'http://localhost:5000/update',
                json=sentiment_count,
                timeout=5
            )
            response.raise_for_status()
            return True
        except requests.exceptions.RequestException as e:
            logging.warning(f"Attempt {i+1} failed to update Flask: {str(e)}")
            if i == retries - 1:
                return False
            time.sleep(2)

def main():
    sentiment_count = {"positive": 0, "neutral": 0, "negative": 0}
    
    try:
        consumer = create_consumer()
        logging.info("Consumer started successfully")
        
        for msg in consumer:
            try:
                tweet = msg.value.decode('utf-8')
                sentiment = get_sentiment(tweet)
                sentiment_count[sentiment] += 1
                
                logging.info(f"{tweet[:50]}... → {sentiment.upper()}")
                
                # Update Flask every 3 messages for better performance
                if sum(sentiment_count.values()) % 3 == 0:
                    if not update_flask(sentiment_count):
                        logging.error("Failed to update Flask after multiple attempts")
                
            except UnicodeDecodeError:
                logging.warning("Received message with invalid encoding")
            except Exception as e:
                logging.error(f"Error processing message: {str(e)}")
                
    except KeyboardInterrupt:
        logging.info("Shutting down consumer...")
    except Exception as e:
        logging.critical(f"Fatal error in consumer: {str(e)}")
    finally:
        if 'consumer' in locals():
            consumer.close()

if __name__ == '__main__':
    main()