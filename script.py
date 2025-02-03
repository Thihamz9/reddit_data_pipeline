import praw
import config

# Authenticate with Reddit API
reddit = praw.Reddit(
    client_id=config.CLIENT_ID,
    client_secret=config.CLIENT_SECRET,
    user_agent=config.USER_AGENT
)

# Test connection by fetching subreddit details
subreddit = reddit.subreddit("paranormal")
print(f"Subreddit name: {subreddit.display_name}")
print(f"Subreddit description: {subreddit.public_description}")
