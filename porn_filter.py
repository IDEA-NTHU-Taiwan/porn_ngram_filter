from nltk.util import ngrams
import string
import csv
import ujson

WORDLIST_PATH = "wordlists/"
OUTPUT_PATH = "output/"
PUNCTUATION = list(string.punctuation)

def read_csv_file(filename, path):
    """Accepts a file name and loads it as a list.
    Args:
        filename   (str): Filename to be loaded.
        path       (str): Directory path to use.

    Returns:
        list: List of strings.
    """

    try:
        with open(path + filename + ".csv", "r") as entry:
            reader = csv.reader(entry)
            temp = list(reader)
            # flatten to 1D, it gets loaded as 2D array
            result = [x for sublist in temp for x in sublist]
    except IOError as ex:
        print("I/O error({0}): {1}".format(ex.errno, ex.strerror))
    else:
        entry.close()
        return result


def write_csv_file(filename, path, result):
    """Writes the result to csv with the given filename.

    Args:
        filename   (str): Filename to write to.
        path       (str): Directory path to use.
    """

    output = open(path + filename + ".csv", "w")
    writer = csv.writer(output, quoting=csv.QUOTE_ALL, lineterminator="\n")
    for val in result:
        writer.writerow([val])
    # Print one a single row
    # writer.writerow(result)


def read_json_file(filename, path):
    """Accepts a file name and loads it as a json object.

    Args:
        filename   (str): Filename to be loaded.
        path       (str): Directory path to use.

    Returns:
        obj: json object
    """

    result = []
    try:
        with open(path + filename + ".json", "r") as entry:
            result = ujson.load(entry)
    except IOError as ex:
        print("I/O error({0}): {1}".format(ex.errno, ex.strerror))
    else:
        entry.close()
        return result


def write_json_file(filename, path, result):
    """Writes the result to json with the given filename.

    Args:
        filename   (str): Filename to write to.
        path       (str): Directory path to use.
    """

    with open(path + filename + ".json", "w+") as json_file:
        ujson.dump(result, json_file)
    json_file.close()


def create_ngrams(text_list, length):
    """ Create ngrams of the specified length from a string of text
    Args:
        text_list   (list): Pre-tokenized text to process.
        length      (int):  Length of ngrams to create.
    """

    clean_tokens = [token for token in text_list if token not in PUNCTUATION]
    return [" ".join(i for i in ngram) for ngram in ngrams(clean_tokens, length)]


def do_create_ngram_collections(text, args):
    """ Create and return ngram collections and set intersections.
    Text must be lowercased if required.
    Args:
        text   (str): Text to process.
        args      (list): Can contains the following:
            0: porn_black_list (set): List of porn keywords.
            1: hs_keywords (set) HS corpus.
            2: black_list  (set) Custom words to filter on.
    """

    porn_black_list = args[0]
    if args[1]:
        hs_keywords = args[1]
    else:
        hs_keywords = None
    if args[2]:
        black_list = args[2]
    else:
        black_list = None

    tokens = text.split(" ")
    unigrams = create_ngrams(tokens, 1)
    bigrams = create_ngrams(tokens, 2)
    trigrams = create_ngrams(tokens, 3)
    quadgrams = create_ngrams(tokens, 4)

    # Customize this as required, some unigrams are
    # too generic so I exclude them
    xgrams = bigrams + trigrams + quadgrams
    unigrams = set(unigrams)
    xgrams = set(xgrams)

    # Set operations are faster than list iterations.
    # Here we perform a best effort series of filters
    # to ensure we only get tweets we want.
    # unigram_intersect = set(porn_black_list).intersection(unigrams)
    xgrams_intersect = porn_black_list.intersection(xgrams)

    if hs_keywords:
        hs_keywords_intersect = hs_keywords.intersection(unigrams)
    else:
        hs_keywords_intersect = None
    
    if black_list:
        black_list_intersect = black_list.intersection(unigrams)
    else:
        black_list_intersect = None
    
    return [None, xgrams_intersect, hs_keywords_intersect, black_list_intersect]


def select_porn_tweets(text_list, porn_black_list):
    """ Select tweets that contain ngrams related to porn.
    Use this to produce a list of tweets spefically related to porn.

    Args:
        text_list   (list): Raw text to process.
        porn_black_list (set): List of porn keywords and ngrams.
    """

    progress = 0
    staging = []
    for document in text_list:
        progress = progress + 1
        set_intersects = do_create_ngram_collections(
            document.lower(), [porn_black_list, None, None])

        ngrams_intersect = set_intersects[1]
        if ngrams_intersect:
            staging.append(document)
        else:
         #   No intersection, skip entry
            pass

    write_csv_file('porn_related_tweets', OUTPUT_PATH, staging)


def filter_porn_tweets(text_list, porn_black_list):
    """ Filter tweets that contain ngrams related to porn.
    Use this to produce a list of tweets that do not relate to porn.

    Args:
        text_list   (list): Raw text to process.
        porn_black_list (set): List of porn keywords and ngrams.
    """

    progress = 0
    staging = []
    # Keep track of how often we match an ngram in our blacklist
    porn_black_list_counts = dict.fromkeys(porn_black_list, 0)

    for document in text_list:
        progress = progress + 1
        set_intersects = do_create_ngram_collections(
            document.lower(), [porn_black_list, None, None])

        ngrams_intersect = set_intersects[1]
        if not ngrams_intersect:
            staging.append(document)
        else:
            for token in ngrams_intersect:
                porn_black_list_counts[token] += 1

    write_csv_file('porn_filtered_tweets', OUTPUT_PATH, staging)
    write_json_file(
        'porn_ngram_hits', OUTPUT_PATH, porn_black_list_counts)

def main():
    """Run operations"""
    test_list = ["asian lesbians hello world", "sexy naked women are great", "this is a test"]
    
    # This is a list of porn related tokens and ngrams that I compiled
    # If I recall I got most of them from the following
    # https://pastebin.com/gpHmA8X5

    PORN_BLACK_LIST = set(read_csv_file(
        "porn_blacklist", WORDLIST_PATH))

    # I built this list from the top k users that had tweets matching the ngrams in
    # the raw PORN_BLACK_LIST, I can't remember what I set k to but I think it
    # was 1000. There are a few other lists that you can try as well
    porn_trigrams_top_k_users = set(read_csv_file(
        "porn_trigrams_top_k_users", WORDLIST_PATH))
        
    select_porn_tweets(test_list, PORN_BLACK_LIST)
    filter_porn_tweets(test_list, PORN_BLACK_LIST)
if __name__ == "__main__":
    main()