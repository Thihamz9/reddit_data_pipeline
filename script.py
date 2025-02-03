import praw
import config
import pandas as pd

from sql_script import sql_script

# Authenticate with Reddit API
reddit = praw.Reddit(
    client_id=config.CLIENT_ID,
    client_secret=config.CLIENT_SECRET,
    user_agent=config.USER_AGENT
)

db_script= sql_script(
    db_host = config.DB_HOST,
    db_name = config.DB_NAME,
    db_password = config.DB_PASSWORD,
    db_port = config.DB_PORT,
    db_user = config.DB_USER
)

# Test connection by fetching subreddit details
subreddit = reddit.subreddit("paranormal")
print(f"Subreddit name: {subreddit.display_name}")
print(f"Subreddit description: {subreddit.public_description}")

# Function to fetch posts from a subreddit
def fetch_reddit_posts(subreddit_name, post_limit=50):
    """Fetch top posts from the given subreddit"""
    reddit = praw.Reddit(
        client_id=config.CLIENT_ID,
        client_secret=config.CLIENT_SECRET,
        user_agent=config.USER_AGENT
    )

    subreddit = reddit.subreddit(subreddit_name)
    posts = []

    for post in subreddit.top(limit=post_limit):
        posts.append({
            "id": post.id,
            "title": post.title,
            "score": post.score,
            "num_comments": post.num_comments,
            "created_utc": post.created_utc,
            "url": post.url
        })

    return pd.DataFrame(posts)

def main():
    df = fetch_reddit_posts("paranormal", 200)
    db_script.insert_table(df,"paranormal")
    print(df)


if __name__=="__main__":
    main()