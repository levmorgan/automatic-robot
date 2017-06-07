print("Loading markov.py")
import re
from collections import Counter
from decimal import Decimal
import re

from nltk.tokenize import TweetTokenizer
from nltk.util import ngrams
from MySQLdb import DataError

from hidden_utils import connect_db

from utils import memoize
from datetime import datetime


print(datetime.now().time())
print("markov.py loaded")
tknzr = TweetTokenizer(strip_handles=True, reduce_len=True)
db = connect_db()

def train_db():
    global db
    cur = db.cursor()
    cur.execute('SELECT message FROM messages;')
    rows = cur.fetchall()
    [train_markov(row[0]) for row in rows]

def train_markov(text):
    global db
    global tknzr
    bigrams = get_bigrams(text)
    counts = Counter(bigrams)
    data = [(key[0], key[1], counts[key], counts[key])
            for key in counts.keys()]

    cur = db.cursor()
    try:
        cur.executemany(
            """
            INSERT INTO transitions_1 (first_word, second_word, times_seen)
            VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE times_seen=times_seen+%s;
            """,
            data
        )
        db.commit()
    except DataError as e:
        print(e)
    cur.close()

def get_bigrams(text):
    text = text.lower()
    tokens = ( token for token in tknzr.tokenize(text)
              if token_ok(token) )
    bigrams = ngrams(tokens, 2)
    return bigrams

def token_ok(token):
    return (len(token) > 1) or (re.match("\w", token))

def score_db():
    global db
    cur = db.cursor()

    print(datetime.now().time())
    print("Fetching messages...")
    cur.execute("SELECT * FROM messages ORDER BY message;")
    rows = cur.fetchall()

    print(datetime.now().time())
    print("Messages fetched")

    print(datetime.now().time())
    print("Scoring messages...")
    scorer = Scorer(db)
    scores_and_text = [(scorer.score_text(row[0]), row[0]) for row in rows]

    print(datetime.now().time())
    print("Messages scored")

    print(datetime.now().time())
    print("Writing scores...")
    cur.executemany("UPDATE messages SET score_1 = %s WHERE message = %s;", scores_and_text)

    print(datetime.now().time())
    print("Scores written")

    db.commit()
    cur.close()


class Scorer(object):
    def __init__(self, db):
        self.db = db
        self.bigram_counts = self.load_bigram_counts()
        self.first_word_counts = self.load_first_word_counts()
        self.tknzr = TweetTokenizer(strip_handles=True, reduce_len=True)

    def score_text(self, text):
        tknzr = self.tknzr
        bigrams = get_bigrams(text)
        bigrams = list(bigrams)
        scores = [self.score_bigram(bigram)
                  for bigram in bigrams]
        score = sum(scores)

        if score == len(bigrams):
            return 0
        else:
            return score/Decimal(len(bigrams)+1)

    def score_bigram(self, bigram):
        db = self.db
        cur = db.cursor()
        try:
            total_count = self.get_first_word_count(bigram[0])
            bigram_count = self.get_bigram_count(bigram)
        except IndexError as e:
            return 0

        cur.close()
        return(1 - (bigram_count/total_count))

    def get_first_word_count(self, first_word):
        try:
            return self.first_word_counts[first_word]
        # TODO: Figure out why this happens
        except KeyError:
            return 1

    def get_bigram_count(self, bigram):
        try:
            return self.bigram_counts[bigram]
        # TODO: Figure out why this happens
        except KeyError:
            return 1

    def load_bigram_counts(self):
        db = self.db
        cur = db.cursor()
        res = cur.execute("SELECT first_word, second_word, times_seen FROM transitions_1;")
        rows = self.filter_rows(cur.fetchall(), 3)
        count_dict = {(row[0], row[1]): row[2] for row in rows if len(row) == 3}
        cur.close()
        return count_dict

    def filter_rows(self, rows, desired_len):
        good_rows = []
        for row in rows:
            if len(row) == desired_len:
                good_rows.append(row)
            else:
                print("Bad row: "+str(row))
        return good_rows

    def load_first_word_counts(self):
        db = self.db
        cur = db.cursor()
        res = cur.execute(
            'SELECT first_word, SUM(times_seen) FROM transitions_1 GROUP BY first_word;')
        rows = self.filter_rows(cur.fetchall(), 2)
        count_dict = {row[0]: row[1] for row in rows}
        cur.close()
        return count_dict

if __name__ == "__main__":
    print(datetime.now().time())
    print("Starting score_db")
    score_db()
