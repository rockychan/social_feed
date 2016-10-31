import json
import os

SKYGEAR_APP_NAME = os.getenv('APP_NAME', 'my_skygear_app')
SOCIAL_FEED_TABLE_PREFIX = 'skygear_social_feed'
SOCIAL_FEED_RECORD_TYPES = json.loads(
    os.getenv('SKYGEAR_SOCIAL_FEED_RECORD_TYPES', '[]')
)
SOCIAL_FEED_FANOUT_POLICY_JSON_STR = os.getenv(
    'SKYGEAR_SOCIAL_FEED_FANOUT_POLICY',
    '{"friends": true, "following": true}'
)
SOCIAL_FEED_FANOUT_POLICY = json.loads(SOCIAL_FEED_FANOUT_POLICY_JSON_STR)

DB_NAME = 'app_' + SKYGEAR_APP_NAME
