from kafka import KafkaProducer
import time
import random
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('producer.log'),
        logging.StreamHandler()
    ]
)

tweets = [
    # Positive tweets (40%)
    "I absolutely LOVE this service! Best thing ever!",
    "This product changed my life completely! Amazing!",
    "Fantastic experience from start to finish! 10/10!",
    "The team is incredible and so helpful! Wonderful!",
    "Couldn't be happier with the results! Stunning!",
    "This exceeded all my expectations! Brilliant!",
    "Perfect solution for my needs! Outstanding!",
    "The quality is exceptional! Highly recommend!",
    "I'm thrilled with this purchase! Phenomenal!",
    "This is exactly what I needed! Marvelous!",
    "Absolutely perfect in every way! Spectacular!",
    "The best decision I've made all year! Superb!",
    "Flawless execution and delivery! Impressive!",
    "Worth every penny and more! Exceptional!",
    "I can't believe how good this is! Magnificent!",
    
    # Negative tweets (40%)
    "This is the WORST experience I've ever had!",
    "Terrible service! Never using this again!",
    "Complete waste of time and money! Awful!",
    "I regret buying this product! Disgusting!",
    "Horrible customer support! Unacceptable!",
    "This product is a complete scam! Fraud!",
    "Broken from day one! Disgraceful!",
    "Worst decision I've ever made! Appalling!",
    "Total garbage! Don't waste your money!",
    "I want my money back! Atrocious service!",
    "Absolutely dreadful experience! Shocking!",
    "This company should be ashamed! Pathetic!",
    "The quality is shockingly bad! Revolting!",
    "Never been so disappointed! Abysmal!",
    "Stay far away from this! Deplorable!",
    
    # Neutral tweets (20%)
    "It's okay, nothing special.",
    "Does what it says, I guess.",
    "Average experience overall.",
    "Not bad, but not great either.",
    "It works, but could be better.",
    "Meets basic expectations.",
    "Standard functionality.",
    "Neither impressed nor disappointed.",
    "Basic features work fine.",
    "It's alright for the price."
]

def create_producer():
    retries = 3
    for i in range(retries):
        try:
            return KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                retries=3,
                reconnect_backoff_ms=1000
            )
        except Exception as e:
            logging.error(f"Attempt {i+1} failed to connect to Kafka: {str(e)}")
            if i == retries - 1:
                raise
            time.sleep(2)

def main():
    try:
        producer = create_producer()
        logging.info("Producer started successfully")
        
        while True:
            try:
                tweet = random.choice(tweets)
                future = producer.send(
                    'tweets',
                    value=tweet.encode('utf-8'),
                    timestamp_ms=int(time.time() * 1000)
                )
                
                future.get(timeout=10)
                logging.info(f"Sent: {tweet}")
                
                time.sleep(random.uniform(0.1, 0.5))  # Faster production
                
            except KeyboardInterrupt:
                logging.info("Shutting down producer...")
                producer.close()
                break
                
            except Exception as e:
                logging.error(f"Error sending message: {str(e)}")
                time.sleep(2)
                
    except Exception as e:
        logging.critical(f"Fatal error in producer: {str(e)}")

if __name__ == '__main__':
    main()