from time import sleep
import tweepy
from MySQLdb import DataError

import markov
from hidden_utils import connect_db, get_cfg

from threading import Thread
from queue import Queue, Empty
from datetime import datetime

# import nltk
# from nltk import word_tokenize

def get_api():
    cfg = get_cfg()
    auth = tweepy.OAuthHandler(cfg['consumer_key'], cfg['consumer_secret'])
    auth.set_access_token(cfg['access_token'], cfg['access_token_secret'])
    return tweepy.API(auth, wait_on_rate_limit=True)

def get_ascii(s):
    return str(''.join([c for c in s if ord(c) < 128]))

def is_ascii(s):
    return all(ord(c) < 128 for c in s)

#override tweepy.StreamListener to add logic to on_status
class BigramListener(tweepy.StreamListener):
    def on_status(self, status):
        if not hasattr(self, 'q'):
            self.q = Queue(maxsize=1000000)
        text = status.text
        # print(word_tokenize(text.lower()))
        # print(text)

        ascii_chars = get_ascii(text)
        if len(ascii_chars) > (len(text)/2):
            text = ascii_chars
            # print(text)
            """
            db = self.db
            try:
                text_tuple = (text, )
                cur = db.cursor()
                cur.execute("INSERT INTO messages (message) VALUES (%s)", text_tuple)
                db.commit()
                markov.train_markov(text)
            except DataError:
                print "Couldn't handle "+ascii_chars
            cur.close()
            """
        self.q.put(text)

class InsertThread(Thread):
    def __init__(self, q):
        self.db = connect_db()
        self.q = q
        super(InsertThread, self).__init__()

if __name__ == '__main__':
    api = get_api()

    bigramListener = BigramListener()
    stream = tweepy.Stream(auth = api.auth, listener=bigramListener)
    # stream.filter(locations=(-122.75,36.8,-121.75,37.8,-74,40,-73,41), async = True)
    # stream.filter(locations=(-122.75,36.8,-121.75,37.8), async = True)
    stream.filter(locations=(-165.,30.,-70.,70.), async = True)
    minutes = 60*4
    def killer():
        sleep(minutes*60)
        stream.disconnect()
        print(datetime.now().time())
        print("Disconnected stream!")

    killerThread = Thread(target = killer)
    killerThread.start()
    db = connect_db()
    print(datetime.now().time())
    print("Working for %i minutes"%(minutes))
    # for _ in xrange(minutes):
        #sleep(1)
        #print("Got %s tweets in the last minute (or so), adding to db"%(
        #       str(bigramListener.q.qsize())))
    cur = db.cursor()

    # Wait for some tweets to build up
    sleep(10)
    counter = 0
    while True:
        try:
            tweet = bigramListener.q.get_nowait()
            ascii_chars = get_ascii(tweet)
            if len(ascii_chars) > (len(tweet)/2):
                tweet = ascii_chars
                if(len(tweet) < 160):
                    cur.execute("INSERT INTO messages (message) VALUES (%s)", (tweet, ))
                    markov.train_markov(tweet, db)
            bigramListener.q.task_done()
            counter = counter + 1
            if (counter % 1000) == 0:
                print(datetime.now().time())
                print("Processed %i tweets!"%(counter))
        except Empty:
            break
        except Exception as e:
            print("Exception:",)
            print(e)
            print("Continuing...")

    db.commit()
    cur.close()
    db.close()
    
    print(datetime.now().time())
    print("Done!")
