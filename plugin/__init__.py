import json
import os
from skygear import (
    op,
)
from skygear.container import SkygearContainer
from skygear.options import options
from skygear.utils import db
from .audit import (
    register_update_index_if_fanout_policy_change,
)
from .record import (
    register_query_my_friends_records,
    register_query_my_followees_records,
    register_after_save_add_record_to_index_for_friends,
    register_after_save_add_record_to_index_for_followers,
)
from .relation import (
    register_create_index_for_friends,
    register_create_index_for_followee,
    register_remove_index_for_friends,
    register_remove_index_for_followees,
    register_reindex_for_friends,
    register_reindex_for_followees,
)
from .table_name import (
    name_for_followings_relation_index,
    name_for_friends_relation_index
)
from .user import (
    register_set_enable_fanout_to_relation,
    register_get_user_fanout_policy,
)

SKYGEAR_APP_NAME = os.getenv('APP_NAME', 'my_skygear_app')
SOCIAL_FEED_TABLE_PREFIX = 'skygear_social_feed'
SOCIAL_FEED_RECORD_TYPES = json.loads(
    os.getenv('SKYGEAR_SOCIAL_FEED_RECORD_TYPES', '[]')
)


def create_table_for_social_feed(container, record_type):
    table_name_for_following_relation = name_for_followings_relation_index(
        prefix=SOCIAL_FEED_TABLE_PREFIX,
        record_type=record_type
    )
    container.send_action(
        'schema:create',
        {
            'record_types': {
                table_name_for_following_relation: {
                    'fields': [
                        {
                            'name': 'left_id',
                            'type': 'ref(user)',
                        },
                        {
                            'name': 'right_id',
                            'type': 'ref(user)',
                        },
                        {
                            'name': 'record_ref',
                            'type': 'ref({0})'.format(record_type),
                        },
                    ]
                }
            }
        }
    )

    table_name_for_friends_relation = name_for_friends_relation_index(
        prefix=SOCIAL_FEED_TABLE_PREFIX,
        record_type=record_type
    )
    container.send_action(
        'schema:create',
        {
            'record_types': {
                table_name_for_friends_relation: {
                    'fields': [
                        {
                            'name': 'left_id',
                            'type': 'ref(user)',
                        },
                        {
                            'name': 'right_id',
                            'type': 'ref(user)',
                        },
                        {
                            'name': 'record_ref',
                            'type': 'ref({0})'.format(record_type),
                        },
                    ]
                }
            }
        }
    )


@op('social-feed-init')
def social_feed_init():
    container = SkygearContainer(api_key=options.masterkey)

    container.send_action(
        'schema:create',
        {
            'record_types': {
                'user': {
                    'fields': [
                        {
                            'name': 'name',
                            'type': 'string',
                        },
                        {
                            'name': 'social_feed_fanout_policy',
                            'type': 'json',
                        },
                        {
                            'name': 'social_feed_fanout_policy_is_dirty',
                            'type': 'boolean',
                        },
                    ]
                }
            }
        }
    )

    for record_type in SOCIAL_FEED_RECORD_TYPES:
        create_table_for_social_feed(container, record_type)

    with db.conn() as conn:
        sql = 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp"'
        conn.execute(sql)


for record_type in SOCIAL_FEED_RECORD_TYPES:
    register_after_save_add_record_to_index_for_friends(record_type)
    register_after_save_add_record_to_index_for_followers(record_type)

register_create_index_for_friends()
register_create_index_for_followee()

register_query_my_friends_records()
register_query_my_followees_records()

register_remove_index_for_friends()
register_remove_index_for_followees()

register_reindex_for_friends()
register_reindex_for_followees()

register_set_enable_fanout_to_relation()
register_get_user_fanout_policy()

register_update_index_if_fanout_policy_change()
