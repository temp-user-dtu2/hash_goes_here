import sqlite3
import sys

# Constants
num_cmt_to_fetch = 500
num_max_results = 10

subreddit_t3s = {}
parent_posts = {}

with sqlite3.connect("reddit.db", check_same_thread=False) as conn:
    comments_query =  "SELECT id, parent_id, subreddit_id FROM comments"
    comment_cur = conn.cursor()
    comment_cur.execute(comments_query)
    posts = comment_cur.fetchmany(num_cmt_to_fetch)
    while posts:
        for i in range(len(posts)):
            if posts[i][1][:3] == "t3_":
                if posts[i][2] in subreddit_t3s:
                    subreddit_t3s[posts[i][2]].append(posts[i][1])
                else:
                    subreddit_t3s[posts[i][2]] = list([posts[i][1]])
            if posts[i][1] in parent_posts:
                parent_posts[posts[i][1]].append(posts[i][0])
            else:
                parent_posts[posts[i][1]] = list([posts[i][0]])
        posts = comment_cur.fetchmany(num_cmt_to_fetch)

leaderboard = [("", -1)] * num_max_results

def calculate_max_depth(parent):
    depth = -1.0
    current_children = parent_posts[parent]
    while len(current_children) != 0:
        new_children = []
        for child in current_children:
            if child in parent_posts:
                new_children.extend(parent_posts[child])
        current_children = new_children
        depth += 1.0
    return depth


for subreddit, top_level_cmts in subreddit_t3s.items():
    # calculate average max depth for each reddit
    sum = 0.0
    for comment in top_level_cmts:
        sum += calculate_max_depth(comment)

    result = (subreddit, sum / len(top_level_cmts))
    min_index = 0
    min_value = leaderboard[min_index][1]
    for i in range(1, num_max_results):
        if min_value > leaderboard[i][1]:
            min_value = leaderboard[i][1]
            min_index = i
    if result[1] > min_value:
        leaderboard[min_index] = result

with sqlite3.connect("reddit.db", check_same_thread=False) as conn:
    for position in sorted(leaderboard, key=lambda tup: tup[1])[-num_max_results:]:
        subreddit_query =  "SELECT name FROM subreddits WHERE id='" + str(position[0]) +"'"
        name = (conn.execute(subreddit_query).fetchone())[0]
        print((name, position[0], position[1]))
