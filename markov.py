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

def score_text(text):
    tokens = word_tokenize(text)
    bigrams = ngrams(tokens, 2)
    scores = [score_bigram(bigram)
              for bigram in bigrams]
    score = sum(scores)

    if score == length(bigrams):
        return 0
    else:
        return score/length(bigrams)

def score_bigram(bigram):
    db = connect_db()
    cur = db.cursor()

    res = cur.execute(
        'SELECT SUM(times_seen) FROM transitions WHERE first_word = %s', bigram[0])
    total_count = cur.fetchall()[0][0]
    res = cur.execute(
        'SELECT times_seen FROM transitions WHERE first_word = %s AND second_word = %s', bigram)
    bigram_count = cur.fetchall()[0][0]
    cur.close()
    db.close()

    return(1 - (bigram_count/total_count))

