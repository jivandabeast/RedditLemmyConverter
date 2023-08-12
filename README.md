# Reddit -> Post Converter
A Python script to pull posts from Reddit and upload them to Lemmy. (With or without comments)

# Limitations
This is not the kind of program you can run on any instance. There are certain aspects that require direct access to the database, which you would not have on a public instance. You also need to be concerned about instance rate limiting, which could pose an issue with copying over the posts. 

If you still wish to run this on a public instance, you can remove the portions of the script which alter the post data in Postgres and everything should still work well.

Additionally, this script only gets the frontpage of each subreddit (meaning the first 25 posts in "hot").

For simplicity, the script searches the lemmy instance for a community that matches the subreddit name.

# Setup
1. Setup a Python virtual environment, and install the `requirements.txt`
2. Create the `config.yml`, using `config.yml.sample` as your basis
3. Run the script
    - The script takes a really long time to run per subreddit. I believe this is a limitation of the `pythorhead` Lemmy API library. This is something I hope to fix in a future version

# TODO
- Handle duplication
- Comment attribution
- Faster?
- Code documentation