import tweepy
from time import sleep

import markov
# import nltk
# from nltk import word_tokenize

def get_api():
    cfg = get_cfg()
    auth = tweepy.OAuthHandler(cfg['consumer_key'], cfg['consumer_secret'])
    auth.set_access_token(cfg['access_token'], cfg['access_token_secret'])
    return tweepy.API(auth)

def get_cfg():
    cfg = {
        "consumer_key"        : "ESQKkz6AJhuUMkRwu3HQK7CZ2",
        "consumer_secret"     : "Kg66kJ3oS2oVKAZCwQz8pf4DzLZT5JEFdP175GEmmplNwewdXj",
        "access_token"        : "799711173738254336-814wQlIdMAhYJJecrD7hI9nTic2Paps",
        "access_token_secret" : "DPY0iSrx8uiCBkTCE99cefO3DtjjclGMFNtqMCn9oDKLV"
    }
    return cfg

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

        if is_ascii(text):
            print(text)
            db = markov.connect_db()
            cur = db.cursor()
            cur.execute("INSERT INTO messages (message) VALUES (%s);", (text,))
            db.commit()
            cur.close()
            db.close()
            markov.train_markov(text)

if __name__ == False: #'__main__':
    api = get_api()

    bigramListener = BigramListener()
    stream = tweepy.Stream(auth = api.auth, listener=bigramListener)
    stream.filter(locations=(-122.75,36.8,-121.75,37.8,-74,40,-73,41), async = True)
    # stream.filter(locations=(-122.75,36.8,-121.75,37.8), async = True)
    # stream.filter(locations=(-165.,30.,-70.,70.), async = True)
    print("Sleeping an hour!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    sleep(60*60*6)
    stream.disconnect()
    print("Stream off!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

