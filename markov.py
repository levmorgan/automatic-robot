# import nltk
# from nltk import word_tokenize
from nltk.tokenize import TweetTokenizer
from nltk.util import ngrams
from collections import Counter
from decimal import Decimal
import MySQLdb

tknzr = TweetTokenizer(strip_handles=True, reduce_len=True)

def connect_db():
    db = MySQLdb.Connect(
            host = "localhost",
            user = "pi",
            passwd = "s0sw00d!",
            db = "twitter"
    )
    return db

def train_markov(text):
    global tknzr
    text = text.lower()
    tokens = tknzr.tokenize(text)
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

def score_db():
    db = connect_db()
    cur = db.cursor()

    cur.execute("SELECT * FROM messages;")
    rows = cur.fetchall()
    scores_and_text = [(score_text(row[0]), row[0]) for row in rows]

    cur.executemany("UPDATE messages SET score = %s WHERE message = %s;", scores_and_text)

    db.commit()
    cur.close()
    db.close()


def score_text(text):
    global tknzr
    text = text.lower()
    tokens = tknzr.tokenize(text)
    bigrams = ngrams(tokens, 2)
    bigrams = list(bigrams)
    scores = [score_bigram(bigram)
              for bigram in bigrams]
    score = sum(scores)

    if score == len(bigrams):
        return 0
    else:
        return score/Decimal(len(bigrams)+1)

def score_bigram(bigram):
    db = connect_db()
    cur = db.cursor()
    try:
        res = cur.execute(
            'SELECT SUM(times_seen) FROM transitions WHERE first_word = %s', bigram[0])
        total_count = cur.fetchall()[0][0]
        res = cur.execute(
            'SELECT times_seen FROM transitions WHERE first_word = %s AND second_word = %s', bigram)
        bigram_count = cur.fetchall()[0][0]
    except IndexError as e:
        return 0
    db.close()

    return(1 - (bigram_count/total_count))

if __name__ == "__main__":
    score_db()
