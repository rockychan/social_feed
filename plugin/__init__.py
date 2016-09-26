import json
import os
from skygear import op
from skygear.container import SkygearContainer
from skygear.options import options

SOCIAL_FEED_TABLE_PREFIX = 'skygear_social_feed'
SOCIAL_FEED_RECORD_TYPES = json.loads(
    os.getenv('SKYGEAR_SOCIAL_FEED_RECORD_TYPES', '[]')
)

def create_table_for_social_feed(container, record_type):
    table_name_format = '{prefix}_{relation}_{record_type}'

    table_name_for_following_relation = table_name_format.format(
        prefix=SOCIAL_FEED_TABLE_PREFIX,
        relation='following',
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

    table_name_for_friends_relation = table_name_format.format(
        prefix=SOCIAL_FEED_TABLE_PREFIX,
        relation='friends',
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
                    ]
                }
            }
        }
    )

    for record_type in SOCIAL_FEED_RECORD_TYPES:
        create_table_for_social_feed(container, record_type);
