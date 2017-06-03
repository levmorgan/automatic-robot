from collections import Counter
from decimal import Decimal

from nltk.tokenize import TweetTokenizer
from nltk.util import ngrams

from hidden_utils import connect_db

from utils import memoize


tknzr = TweetTokenizer(strip_handles=True, reduce_len=True)
db = connect_db()

def train_markov(text):
    global db
    global tknzr
    text = text.lower()
    tokens = tknzr.tokenize(text)
    bigrams = ngrams(tokens, 2)
    counts = Counter(bigrams)
    data = [(key[0], key[1], counts[key], counts[key])
            for key in counts.keys()]

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

def score_db():
    global db
    cur = db.cursor()

    cur.execute("SELECT * FROM messages ORDER BY message LIMIT 10;")
    rows = cur.fetchall()
    # TODO: Thread this
    scores_and_text = [(score_text(row[0]), row[0]) for row in rows]

    cur.executemany("UPDATE messages SET score = %s WHERE message = %s;", scores_and_text)

    db.commit()
    cur.close()


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
    global db
    cur = db.cursor()
    try:
        total_count = get_first_word_count(bigram[0])
        res = cur.execute(
            'SELECT times_seen FROM transitions WHERE first_word = %s AND second_word = %s', bigram)
        bigram_count = cur.fetchall()[0][0]
    except IndexError as e:
        return 0

    cur.close()
    return(1 - (bigram_count/total_count))

@memoize
def get_first_word_count(first_word):
    global db
    cur = db.cursor()
    res = cur.execute(
        'SELECT SUM(times_seen) FROM transitions WHERE first_word = %s', (first_word, ))
    total_count = cur.fetchall()[0][0]
    cur.close()
    return(total_count)


if __name__ == "__main__":
    score_db()
