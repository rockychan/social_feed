import json
import os
import skygear
from skygear import op
from skygear.container import SkygearContainer
from skygear.options import options
from skygear.utils import db
import sqlalchemy as sa
import uuid

SKYGEAR_APP_NAME = os.getenv('APP_NAME', 'my_skygear_app')
SOCIAL_FEED_TABLE_PREFIX = 'skygear_social_feed'
SOCIAL_FEED_RECORD_TYPES = json.loads(
    os.getenv('SKYGEAR_SOCIAL_FEED_RECORD_TYPES', '[]')
)

DB_NAME = 'app_' + SKYGEAR_APP_NAME


def table_name_for_relation_index(prefix, relation, record_type):
    table_name_format = '{prefix}_{relation}_{record_type}'
    return table_name_format.format(
        prefix=prefix,
        relation=relation,
        record_type=record_type
    )


def create_table_for_social_feed(container, record_type):
    table_name_for_following_relation = table_name_for_relation_index(
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

    table_name_for_friends_relation = table_name_for_relation_index(
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


@op('social_feed:create_index', user_required=True)
def social_feed_create_index(maybe_my_friends):
    if len(maybe_my_friends) <= 0:
        return

    with db.conn() as conn:
        my_user_id = skygear.utils.context.current_user_id()
        maybe_my_friend_ids = [user['user_id'] for user in maybe_my_friends]
        maybe_my_friend_ids_tuple = tuple(maybe_my_friend_ids)

        sql = sa.text('''
            SELECT f1.right_id as id
            FROM {db_name}._friend f1
            JOIN {db_name}._friend f2
            ON f1.right_id = f2.left_id
            WHERE f1.left_id = :my_user_id
            AND f2.right_id = :my_user_id
            AND f1.right_id IN :maybe_my_friend_ids
        '''.format(db_name=DB_NAME))
        results = conn.execute(
            sql,
            my_user_id=my_user_id,
            maybe_my_friend_ids=maybe_my_friend_ids_tuple
        )
        my_friend_ids = [user.id for user in results]
        if len(my_friend_ids) <= 0:
            return
        my_friend_ids_tuple = tuple(my_friend_ids)

        for record_type in SOCIAL_FEED_RECORD_TYPES:
            table_name = table_name_for_relation_index(
                prefix=SOCIAL_FEED_TABLE_PREFIX,
                relation='friends',
                record_type=record_type
            )

            create_index_sql = sa.text('''
                INSERT INTO {db_name}.{table_name} (
                    _id,
                    _database_id,
                    _owner_id,
                    _created_at,
                    _created_by,
                    _updated_at,
                    _updated_by,
                    _access,
                    left_id,
                    right_id,
                    record_ref
                )
                SELECT
                    :uuid as _id,
                    '' as _database_id,
                    :my_user_id as _owner_id,
                    current_timestamp as _created_at,
                    :my_user_id as _created_by,
                    current_timestamp as _updated_at,
                    :my_user_id as _updated_by,
                    '[]'::jsonb as _access,
                    :my_user_id as left_id,
                    _owner_id as right_id,
                    _id as record_ref
                FROM {db_name}.{record_type}
                WHERE _owner_id in :my_friend_ids
                AND NOT EXISTS (
                    SELECT *
                    FROM {db_name}.{table_name}
                    WHERE left_id=:my_user_id
                    AND right_id IN :my_friend_ids
                )
            '''.format(
                db_name=DB_NAME,
                table_name=table_name,
                record_type=record_type
            ))
            conn.execute(
                create_index_sql,
                uuid=uuid.uuid4(),
                my_user_id=my_user_id,
                my_friend_ids=my_friend_ids_tuple
            )
