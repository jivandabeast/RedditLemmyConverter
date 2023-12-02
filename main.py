"""
Reddit -> Lemmy Post Converter. Copies Reddit posts over to a Lemmy community.
Copyright (C) 2023  Jivan RamjiSingh

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""

import requests
import logging
import yaml
import json
import psycopg2
import time
import sqlite3
from pythorhead import Lemmy
from alive_progress import alive_bar

logging.basicConfig(filename='output/out.log', level=logging.ERROR, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def load_yaml(file: str):
    """Load yaml file
    file -> string location of the file to load
    """

    with open(file, 'r') as f:
        data = yaml.safe_load(f)
    return data

def lemmy_setup(config: dict):
    """Initialize and login to Lemmy
    config -> config.yml dictionary 
    """

    lemmy = Lemmy(config['lemmy']['url'])
    lemmy.log_in(config['credentials']['lemmy_user'], config['credentials']['lemmy_pass'])
    return lemmy

def load_db(file: str):
    """Load sqlite3 db connection
    file -> string location of the sqlite3 db
    """

    # Create the DB connection and open cursor
    db = sqlite3.connect(file)
    sq_cursor = db.cursor()

    # Check if the posts table is created, if not make it
    try:
        sq_cursor.execute("SELECT * FROM posts")
        logging.debug(f"{file} table 'posts' exists")
    except sqlite3.OperationalError:
        sq_cursor.execute("CREATE TABLE posts(reddit_post_id text, lemmy_post_id text, post_score integer, post_hash text)")
        db.commit()
        logging.debug(f"{file} table 'posts' created")
    
    # Check if the comments table is created, if not make it
    try:
        sq_cursor.execute("SELECT * FROM comments")
        logging.debug(f"{file} table 'comments' exists")
    except sqlite3.OperationalError:
        sq_cursor.execute("CREATE TABLE comments(reddit_comment_id text, lemmy_comment_id text, reddit_post_id text, lemmy_post_id text, comment_score integer, comment_hash text)")
        db.commit()
        logging.debug(f"{file} table 'comments' created")
    
    sq_cursor.close()
    return db

def pg_setup(config: dict):
    """Initialize and connect to postgres
    config -> config.yml dictionary
    """

    pg = psycopg2.connect(
        database = config['lemmy']['pg_db'],
        host = config['lemmy']['pg_host'],
        port = config['lemmy']['pg_port'],
        user = config['credentials']['pg_user'],
        password = config['credentials']['pg_pass']
    )
    return pg

def get_json(endpoint: str):
    """Conduct the Reddit API request for a post
    endpoint -> the api endpoint to hit
    """

    headers = {
        'User-agent': 'RLC 0.1'
    }
    url = f"https://reddit.com{endpoint}.json?limit=10000"
    response = requests.get(url, headers=headers)
    return response.json()

def get_frontpage(subreddit: str):
    """Get the API response for a subreddit frontpage
    subreddit -> subreddit to hit (i.e. /r/tifu/)
    """

    headers = {
        'User-agent': 'RLC 0.1'
    }
    url = f"https://reddit.com{subreddit}.json"
    print(url)
    response = requests.get(url, headers=headers)
    return response.json()

def check_dupe(db: sqlite3.Connection, post: dict = {}, comment: dict = {}):
    """Checks sqlite3 db for a duplicate post/comment based on reddit post/comment id
    Only post OR comment should be passed, not both at once
    db -> sqlite3 db connection
    post -> post data dict (defaults empty/false)
    comment -> comment data dict (defaults empty/false)
    """

    db_cursor = db.cursor()
    row_list = []
    if (post):
        db_data = db_cursor.execute(f"SELECT reddit_post_id, lemmy_post_id, post_score FROM posts WHERE reddit_post_id='{post['id']}'").fetchall()
        for row in db_data or []:
            db_dict = {
                'reddit_post_id': row[0],
                'lemmy_post_id': row[1],
                'post_score': row[2]
            }
            row_list.append(db_dict)
    elif (comment):
        db_data = db_cursor.execute(f"SELECT reddit_post_id, lemmy_post_id, lemmy_comment_id, reddit_comment_id, comment_score FROM comments WHERE reddit_post_id='{comment['reddit_post_id']}' AND reddit_comment_id='{comment['reddit_comment_id']}'").fetchall()
        for row in db_data or []:
            db_dict = {
                'reddit_post_id': row[0],
                'lemmy_post_id': row[1],
                'lemmy_comment_id': row[2],
                'reddit_comment_id': row[3],
                'comment_score': row[4]
            }
            row_list.append(db_dict)
    else:
        logging.error(f"check_dupe called but no data was passed.")
    
    db_cursor.close()

    if (len(row_list) > 1):
        logging.error(f"More than one result returned: {row_list}")
        return row_list[0]
    elif (len(row_list) == 1):
        return row_list[0]
    else:
        return []

def save_entry(db: sqlite3.Connection, comment_data: dict = {}, post_data: dict = {}, comment: dict = {}, post: dict = {}):
    """Save post/comment information to sqlite3
    db -> sqlite3 db connection
    comment_data -> comment data from Lemmy
    post_data -> post data from Lemmy
    comment -> comment information parsed from Reddit API
    post -> post information parsed from Reddit API
    """

    db_cursor = db.cursor()
    if (comment_data and comment):
        db_cursor.execute(f"""
        INSERT INTO comments (reddit_comment_id, lemmy_comment_id, reddit_post_id, lemmy_post_id, comment_score)
                          VALUES ('{comment['reddit_comment_id']}', '{comment['lemmy_comment_id']}', '{comment['reddit_post_id']}', '{comment['lemmy_post_id']}', {comment['comment_score']})
        """)
        pass
    elif (post_data and post):
        db_cursor.execute(f"""
        INSERT INTO posts (reddit_post_id, lemmy_post_id, post_score)
                          VALUES ('{post['id']}', '{post_data['post_view']['post']['id']}', {post['score']})
        """)
    else: 
        logging.error('save_entry called incorrectly')

    db.commit()
    db_cursor.close()

def fix_comment_score(pg: psycopg2.extensions.connection, comment_data: dict, item: dict):
    """Edit comment data in postgres to match score on Reddit
    pg -> postgres db connection
    comment_data -> comment data returned by pythorhead on creation
    item -> comment data from Reddit API
    """

    score = item['data']['score']
    if score == 1:
        # If the score is 1, then it doesn't need to be updated on Lemmy
        # By default all comments on Lemmy have a score of 1
        logging.info(f"Comment {comment_data['comment_view']['comment']['id']} has a score of 1, skipping")
        return
    else:
        query = f"UPDATE comment_aggregates SET score = {score} WHERE comment_id = {comment_data['comment_view']['comment']['id']};"

        pg_cursor = pg.cursor()
        pg_cursor.execute(query)

        pg.commit()
        pg_cursor.close()

        logging.info(f"Fixed score for comment {comment_data['comment_view']['comment']['id']} to {score}")

def fix_post_score(pg: psycopg2.extensions.connection, post_data: dict, post: dict):
    """Edit post data in postgres to match score on Reddit
    pg -> postgres db connection
    post_data -> post data returned by pythorhead on creation
    post -> post data from Reddit API
    """

    score = post['score']
    if score == 1:
        # If the score is 1, then it doesn't need to be updated on Lemmy
        # By default all comments on Lemmy have a score of 1
        logging.info(f"Post {post_data['post_view']['post']['id']} has a score of 1, skipping")
    else:
        query = f"UPDATE post_aggregates SET score = {score} WHERE post_id = {post_data['post_view']['post']['id']};"

        pg_cursor = pg.cursor()
        pg_cursor.execute(query)

        pg.commit()
        pg_cursor.close()

        logging.info(f"Fixed score for post {post_data['post_view']['post']['id']} to {score}")
    pass

def parse_comments(pg: psycopg2.extensions.connection, lemmy: Lemmy, post_data: dict, data: dict, post: dict, db: sqlite3.Connection, parent_comment: dict = {}):
    """Parse through comments and create them on the Lemmy post
    pg -> postgres db connection
    lemmy -> Lemmy instance connection
    post_data -> post data returned by pythorhead on creation
    data -> comment data returned by Reddit API
    post -> post information parsed from Reddit API
    parent_comment -> parent comment information, if applicable (defaults empty/false)
    """

    for item in data['data']['children']:
        # Iterate over all the comments
        if (item['kind'] == 'more'):
            # This comment type is for unloaded comments, should be ignored
            pass
        else:
            comment = {
                'reddit_post_id': post['id'],
                'reddit_comment_id': item['data']['id'],
                'lemmy_post_id': post_data['post_view']['post']['id'],
                'comment_score': item['data']['score']
            }

            comment_data = {}
            if (check_dupe(db, comment=comment)):
                logging.info(f"Duplicate comment found: {comment['reddit_comment_id']}")
            else:
                comment_made = False

                # Copy over the comment, if it has a parent comment then the context should be preserved
                if (parent_comment):
                    try:
                        comment_data = lemmy.comment.create(int(post_data['post_view']['post']['id']), item['data']['body'], parent_id=parent_comment['comment_view']['comment']['id'])
                        comment['lemmy_comment_id'] = comment_data['comment_view']['comment']['id']
                        comment_made = True
                    except:
                        logging.error(f"Could not push child comment {item['data']['id']}")
                        with open(f"output/errors/{item['data']['id']}.json", 'w') as f:
                            f.write(json.dumps(item, indent=4))
                else:
                    try:
                        comment_data = lemmy.comment.create(int(post_data['post_view']['post']['id']), item['data']['body'])
                        comment['lemmy_comment_id'] = comment_data['comment_view']['comment']['id']
                        comment_made = True
                    except:
                        logging.error(f"Could not push comment {item['data']['id']}")
                        with open(f"output/errors/{item['data']['id']}.json", 'w') as f:
                            f.write(json.dumps(item, indent=4))

                if (comment_made):
                    # Copy over the score of the comment
                    try:
                        fix_comment_score(pg, comment_data, item)
                    except:
                        logging.error(f"Could not fix comment score {item['data']['id']}")
                        try:
                            with open(f"output/errors/{item['data']['id']}_score.json", 'w') as f:
                                f.write(json.dumps(comment_data, indent=4))
                            with open(f"output/errors/{item['data']['id']}.json", 'w') as f:
                                f.write(json.dumps(item, indent=4))
                        except:
                            logging.critical(f"Comment has no comment_data {item['data']['id']}")
                    
                    # Save the comment information to sqlite3
                    try:
                        save_entry(db, comment_data=comment_data, comment=comment)
                    except:
                        logging.error(f"Could not save comment information to sqlite3")
                        try:
                            with open(f"output/errors/{comment['lemmy_comment_id']}.json", 'w') as f:
                                f.write(json.dumps(comment, indent=4))
                        except:
                            logging.critical(f"Could not save comment error data")

            # Loop through any replies to the comment to follow comment chains
            if (item['data']['replies'] != ""):
                try:
                    parse_comments(pg, lemmy, post_data, item['data']['replies'], post, db, comment_data)
                except:
                    logging.error(f"Could not handle comment reply {item['data']['id']}")
                    try:
                        with open(f"output/errors/{item['data']['id']}_score.json", 'w') as f:
                            f.write(json.dumps(comment_data, indent=4))
                        with open(f"output/errors/{item['data']['id']}.json", 'w') as f:
                            f.write(json.dumps(item, indent=4))
                    except:
                        logging.critical(f"Comment has no comment_data {item['data']['id']}")

    return True

def load_example_data():
    """Load example data for testing purposes
    """

    with open('output/example.json', 'r') as f:
        data = json.load(f)
    return data

def copy_post(lemmy: Lemmy, pg: psycopg2.extensions.connection, permalink: str, db: sqlite3.Connection, comments: bool = True):
    """Copy post from Reddit to Lemmy
    lemmy -> Lemmy instance connection
    pg -> postgres db connection
    permalink -> Reddit post permalink (i.e. /r/tifu/comments/xxx/xxx)
    comments -> whether to copy comments, defaults True
    """
    
    # Receive Reddit API response and process it
    data = get_json(permalink) 
    post = {
        'title': data[0]['data']['children'][0]['data']['title'],
        'url': data[0]['data']['children'][0]['data']['url'],
        'body': data[0]['data']['children'][0]['data']['selftext'],
        'creator_id': data[0]['data']['children'][0]['data']['author'],
        'subreddit': data[0]['data']['children'][0]['data']['subreddit'],
        'nsfw': data[0]['data']['children'][0]['data']['over_18'],
        'score': data[0]['data']['children'][0]['data']['score'],
        'id': data[0]['data']['children'][0]['data']['id']
    }

    # Find the ID of matching community name on Lemmy
    post['community_id'] = lemmy.community.get(name=post['subreddit'])['community_view']['community']['id']

    # While loop is here to handle rate limits or other reasons for post copy failure
    # Will attempt 5 times before skipping
    attempts = 0
    post_made = False
    post_data = check_dupe(db, post=post)
    if (post_data):
        logging.info(f"Duplicate post found: {post['title']}")
        post_data = {
            'post_view': {
                'post': {
                    "id": post_data['lemmy_post_id']
                }
            }
        }
    else:
        while True:
            try:
                post_data = lemmy.post.create(post['community_id'], post['title'], post['url'], f"{post['body']} \n\n Originally Posted on r/{post['subreddit']} by u/{post['creator_id']}", post['nsfw'])
                fix_post_score(pg, post_data, post)
                post_made = True
                break
            except:
                if (attempts == 5):
                    logging.critical(f'Post could not be copied, skipping')
                    logging.critical(post)
                    post_made = False
                    break
                logging.warning(f'Something went wrong, retrying {attempts}')
                time.sleep(30)
                attempts += 1 
    
    if (post_made):
        logging.info(f"Post Created: {post['title']}")
        save_entry(db, post_data=post_data, post=post)

    # Copy over comments if requested
    if (comments):
        comment_data = parse_comments(pg, lemmy, post_data, data[1], post, db)

def main():
    """Main function
    Gets the data from Reddit 
    Sorts through what should and should not go to Lemmy
    """

    # Load data and initialize connections
    config = load_yaml('config.yml')
    lemmy = lemmy_setup(config)
    pg = pg_setup(config)
    db = load_db("rlc.db")
    
    # Loop through subreddits where we want comments
    for sub in config['subreddits']:
        posts = get_frontpage(f'/r/{sub}/')
        try:
            with alive_bar(len(posts['data']['children'])) as bar:
                for post in posts['data']['children']:
                    if (post['data']['stickied'] == True):
                        # Skip sticky posts
                        pass
                    else:
                        start = time.time()
                        copy_post(lemmy, pg, post['data']['permalink'], db)
                        end = time.time()

                        # Want to make sure we stay under the Reddit rate limit
                        # 10 per minute
                        # Lemmy request times make this unnecessary, but is a precaution
                        if ((end - start) < 10):
                            time.sleep(10 - (end - start))
                    bar()
        except:
            try:
                if (posts['reason'] == 'banned'):
                    # Some subs would fail because they have been banned
                    # Log the error and keep moving
                    logging.error(f"{sub} has been banned")
            except:
                with open(f'output/errors/{sub}.json', 'w') as f:
                    f.write(json.dumps(posts, indent=4))
        
        time.sleep(120)
    
    # Loop through subreddits where we only want pictures/links
    for sub in config['po_subreddits']:
        posts = get_frontpage(f'/r/{sub}/')
        try:
            with alive_bar(len(posts['data']['children'])) as bar:
                for post in posts['data']['children']:
                    if (post['data']['stickied'] == True):
                        # Skip sticky posts
                        pass
                    else:
                        start = time.time()
                        copy_post(lemmy, pg, post['data']['permalink'], db, False)
                        end = time.time()

                        # Want to make sure we stay under the Reddit rate limit
                        # 10 per minute
                        # Lemmy request times make this unnecessary, but is a precaution
                        if ((end - start) < 10):
                            time.sleep(10 - (end - start))
                    bar()
        except:
            try:
                if (posts['reason'] == 'banned'):
                    # Some subs would fail because they have been banned
                    # Log the error and keep moving
                    logging.error(f"{sub} has been banned")
            except:
                with open(f'output/errors/{sub}.json', 'w') as f:
                    f.write(json.dumps(posts, indent=4))

        time.sleep(120)

    db.commit()
    db.close()
    pg.close()
        
if __name__ == '__main__':
    main()