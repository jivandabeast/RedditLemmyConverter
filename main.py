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
from pythorhead import Lemmy

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

def fix_comment_score(pg: psycopg2.connection, comment_data: dict, item: dict):
    """Edit comment data in postgres to match score on Reddit
    pg -> postgres db connection
    comment_data -> comment data returned by pythorhead on creation
    item -> comment data from Reddit API
    """

    score = item['data']['score']
    if score == 1:
        logging.info(f"Comment {comment_data['comment_view']['comment']['id']} has a score of 1, skipping")
        return
    else:
        query = f"UPDATE comment_aggregates SET score = {score} WHERE comment_id = {comment_data['comment_view']['comment']['id']};"

        pg_cursor = pg.cursor()
        pg_cursor.execute(query)

        pg.commit()
        pg_cursor.close()

        logging.info(f"Fixed score for comment {comment_data['comment_view']['comment']['id']} to {score}")

def fix_post_score(pg: psycopg2.connection, post_data: dict, post: dict):
    """Edit post data in postgres to match score on Reddit
    pg -> postgres db connection
    post_data -> post data returned by pythorhead on creation
    post -> post data from Reddit API
    """

    score = post['score']
    if score == 1:
        logging.info(f"Post {post_data['post_view']['post']['id']} has a score of 1, skipping")
    else:
        query = f"UPDATE post_aggregates SET score = {score} WHERE post_id = {post_data['post_view']['post']['id']};"

        pg_cursor = pg.cursor()
        pg_cursor.execute(query)

        pg.commit()
        pg_cursor.close()

        logging.info(f"Fixed score for post {post_data['post_view']['post']['id']} to {score}")
    pass

def parse_comments(pg: psycopg2.connection, lemmy: Lemmy, post_data: dict, data: dict, parent_comment: dict = {}):
    """Parse through comments and create them on the Lemmy post
    pg -> postgres db connection
    lemmy -> Lemmy instance connection
    post_data -> post data returned by pythorhead on creation
    data -> comment data returned by Reddit API
    parent_comment -> parent comment information, if applicable (defaults empty/false)
    """
    
    for item in data['data']['children']:
        if (item['kind'] == 'more'):
            pass
        else:
            if (parent_comment):
                try:
                    comment_data = lemmy.comment.create(post_data['post_view']['post']['id'], item['data']['body'], parent_id=parent_comment['comment_view']['comment']['id'])
                except:
                    logging.error(f"Could not push child comment {item['data']['id']}")
                    with open(f"output/errors/{item['data']['id']}.json", 'w') as f:
                        f.write(json.dumps(item, indent=4))
            else:
                try:
                    comment_data = lemmy.comment.create(post_data['post_view']['post']['id'], item['data']['body'])
                except:
                    logging.error(f"Could not push comment {item['data']['id']}")
                    with open(f"output/errors/{item['data']['id']}.json", 'w') as f:
                        f.write(json.dumps(item, indent=4))

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
            
            if (item['data']['replies'] != ""):
                try:
                    parse_comments(pg, lemmy, post_data, item['data']['replies'], comment_data)
                except:
                    logging.error(f"Could not fix comment score {item['data']['id']}")
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

def copy_post(lemmy: Lemmy, pg: psycopg2.connection, permalink: str, comments: bool = True):
    """Copy post from Reddit to Lemmy
    lemmy -> Lemmy instance connection
    pg -> postgres db connection
    permalink -> Reddit post permalink (i.e. /r/tifu/comments/xxx/xxx)
    comments -> whether to copy comments, defaults True
    """
    
    data = get_json(permalink)
    
    post = {
        'title': data[0]['data']['children'][0]['data']['title'],
        'url': data[0]['data']['children'][0]['data']['url'],
        'body': data[0]['data']['children'][0]['data']['selftext'],
        'creator_id': data[0]['data']['children'][0]['data']['author'],
        'subreddit': data[0]['data']['children'][0]['data']['subreddit'],
        'nsfw': data[0]['data']['children'][0]['data']['over_18'],
        'score': data[0]['data']['children'][0]['data']['score']
    }

    post['community_id'] = lemmy.community.get(name=post['subreddit'])['community_view']['community']['id']

    attempts = 0
    post_made = False
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
        if (comments):
            comment_data = parse_comments(pg, lemmy, post_data, data[1])

def main():
    """Main function
    Gets the data from Reddit 
    Sorts through what should and should not go to Lemmy
    """

    config = load_yaml('config.yml')
    lemmy = lemmy_setup(config)
    pg = pg_setup(config)
    
    for sub in config['subreddits']:
        posts = get_frontpage(f'/r/{sub}/')
        try:
            for post in posts['data']['children']:
                if (post['data']['stickied'] == True):
                    pass
                else:
                    start = time.time()
                    copy_post(lemmy, pg, post['data']['permalink'])
                    end = time.time()
                    if ((end - start) < 10):
                        time.sleep(10 - (end - start))
        except:
            try:
                if (posts['reason'] == 'banned'):
                    logging.error(f"{sub} has been banned")
            except:
                with open(f'output/errors/{sub}.json', 'w') as f:
                    f.write(json.dumps(posts, indent=4))
        
        time.sleep(120)
    
    for sub in config['po_subreddits']:
        posts = get_frontpage(f'/r/{sub}/')
        try:
            for post in posts['data']['children']:
                if (post['data']['stickied'] == True):
                    pass
                else:
                    start = time.time()
                    copy_post(lemmy, pg, post['data']['permalink'], False)
                    end = time.time()
                    if ((end - start) < 10):
                        time.sleep(10 - (end - start))
        except:
            try:
                if (posts['reason'] == 'banned'):
                    logging.error(f"{sub} has been banned")
            except:
                with open(f'output/errors/{sub}.json', 'w') as f:
                    f.write(json.dumps(posts, indent=4))

        time.sleep(120)

    pg.close()
        

if __name__ == '__main__':
    main()