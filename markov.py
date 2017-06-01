import nltk
from nltk import word_tokenize
from nltk.util import ngrams
from collections import Counter
import MySQLdb

def connect_db():
    db = MySQLdb.Connect(
            host = "localhost",
            user = "pi",
            passwd = "s0sw00d!",
            db = "twitter"
    )
    return db

def train_markov(text):
    tokens = word_tokenize(text)
    bigrams = ngrams(tokens, 2)
    counts = Counter(bigrams)
    data = [(key[0], key[1], counts[key], counts[key])
            for key in counts.keys()]

    db = connect_db()
    cur = db.cursor()
    cur.executemany(
        """
        INSERT INTO transitions (first_word, second_word, times_seen)
        VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE times_seen=times_seen+%s;
        """,
        data
    )
    db.commit()
    cur.close()
    db.close()
