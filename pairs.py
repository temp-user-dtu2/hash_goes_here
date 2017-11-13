import sqlite3

# Constants
num_cmt_to_fetch = 500
num_max_results = 10

subreddit_authors = {}

with sqlite3.connect("reddit.db", check_same_thread=False) as conn:
    comments_query =  "SELECT author_id, subreddit_id FROM comments LIMIT 50000"
    comment_cur = conn.cursor()
    comment_cur.execute(comments_query)
    pairs = comment_cur.fetchmany(num_cmt_to_fetch)
    while pairs:
        for i in range(len(pairs)):
            if pairs[i][1] in subreddit_authors:
                subreddit_authors[pairs[i][1]].add(int(pairs[i][0]))
            else:
                subreddit_authors[pairs[i][1]] = set([int(pairs[i][0])])
        pairs = comment_cur.fetchmany(num_cmt_to_fetch)

subreddit_authors = list(subreddit_authors.items())

subreddit_pairs = []

for i in range(len(subreddit_authors)):
    for j in range(i + 1, len(subreddit_authors)):
        num_common_authors = len(subreddit_authors[i][1].intersection(subreddit_authors[j][1]))
        if num_common_authors > 1:
            subreddit_pairs.append((subreddit_authors[i][0], subreddit_authors[j][0], num_common_authors))

with sqlite3.connect("reddit.db") as conn:
    for reddit_one, reddit_two, num_authors in (sorted(subreddit_pairs, key=lambda tup: tup[2]))[-num_max_results:]:
        subreddit_query =  "SELECT name FROM subreddits WHERE id='" + str(reddit_one) +"'"
        name_one = (conn.execute(subreddit_query).fetchone())[0]
        subreddit_query =  "SELECT name FROM subreddits WHERE id='" + str(reddit_two) +"'"
        name_two = (conn.execute(subreddit_query).fetchone())[0]
        print(name_one, name_two, reddit_one, reddit_two, num_authors)