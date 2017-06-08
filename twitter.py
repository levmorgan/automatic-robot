from time import sleep
import tweepy
from MySQLdb import DataError

import markov
from hidden_utils import connect_db, get_cfg

# import nltk
# from nltk import word_tokenize

def get_api():
    cfg = get_cfg()
    auth = tweepy.OAuthHandler(cfg['consumer_key'], cfg['consumer_secret'])
    auth.set_access_token(cfg['access_token'], cfg['access_token_secret'])
    return tweepy.API(auth)

def get_ascii(s):
    return ''.join([c for c in s if ord(c) < 128])

def is_ascii(s):
    return all(ord(c) < 128 for c in s)

#override tweepy.StreamListener to add logic to on_status
class BigramListener(tweepy.StreamListener):
    def on_status(self, status):
        text = status.text
        # print(word_tokenize(text.lower()))
        # print(text)

        ascii_chars = get_ascii(text)
        if len(ascii_chars) > (len(text)/2):
            text = ascii_chars
            print(text)
            db = connect_db()
            try:
                cur = db.cursor()
                cur.execute("INSERT INTO messages (message) VALUES (%s);", (text,))
                db.commit()
                markov.train_markov(text)
            except DataError:
                print "Couldn't handle "+ascii_chars
            cur.close()
            db.close()

if __name__ == '__main__':
    api = get_api()

    bigramListener = BigramListener()
    stream = tweepy.Stream(auth = api.auth, listener=bigramListener)
    stream.filter(locations=(-122.75,36.8,-121.75,37.8,-74,40,-73,41), async = True)
    # stream.filter(locations=(-122.75,36.8,-121.75,37.8), async = True)
    # stream.filter(locations=(-165.,30.,-70.,70.), async = True)
    hours = 12
    print("Sleeping "+str(hours)+" hours!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    sleep(60*60*12)
    stream.disconnect()
    print("Stream off!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
