import json
import os
import skygear
from skygear import (
    after_save,
    op,
)
from skygear.container import SkygearContainer
from skygear.options import options
from skygear.utils import db
import sqlalchemy as sa

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

    with db.conn() as conn:
        sql = 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp"'
        conn.execute(sql)


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

            create_my_friends_records_index_sql = sa.text('''
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
                    uuid_generate_v4() as _id,
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
                FROM {db_name}.{record_type} record_table
                WHERE _owner_id in :my_friend_ids
                AND NOT EXISTS (
                    SELECT *
                    FROM {db_name}.{table_name}
                    WHERE left_id=:my_user_id
                    AND right_id IN (record_table._owner_id)
                    AND record_ref IN (record_table._id)
                )
            '''.format(
                db_name=DB_NAME,
                table_name=table_name,
                record_type=record_type
            ))
            conn.execute(
                create_my_friends_records_index_sql,
                my_user_id=my_user_id,
                my_friend_ids=my_friend_ids_tuple
            )

            create_friends_to_my_records_index_sql = sa.text('''
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
                    uuid_generate_v4() as _id,
                    '' as _database_id,
                    u.id as _owner_id,
                    current_timestamp as _created_at,
                    u.id as _created_by,
                    current_timestamp as _updated_at,
                    u.id as _updated_by,
                    '[]'::jsonb as _access,
                    u.id as left_id,
                    :my_user_id as right_id,
                    record_table._id as record_ref
                FROM {db_name}.{record_type} record_table,
                     {db_name}._user u
                WHERE record_table._owner_id = :my_user_id
                AND u.id in :my_friend_ids
                AND NOT EXISTS (
                    SELECT *
                    FROM {db_name}.{table_name}
                    WHERE right_id = :my_user_id
                    AND left_id IN :my_friend_ids
                    AND record_ref IN (record_table._id)
                )
            '''.format(
                db_name=DB_NAME,
                table_name=table_name,
                record_type=record_type
            ))
            conn.execute(
                create_friends_to_my_records_index_sql,
                my_user_id=my_user_id,
                my_friend_ids=my_friend_ids_tuple
            )


@op('social_feed:query_my_friends_records', user_required=True)
def social_feed_query_my_friends_records(serializedSkygearQuery):
    with db.conn() as conn:
        query_record_type = serializedSkygearQuery['record_type']
        table_name = table_name_for_relation_index(
            prefix=SOCIAL_FEED_TABLE_PREFIX,
            relation='friends',
            record_type=query_record_type
        )
        my_user_id = skygear.utils.context.current_user_id()
        get_my_friends_records_ids_sql = sa.text('''
            SELECT record_ref as id
            FROM {db_name}.{table_name}
            WHERE left_id = :my_user_id
        '''.format(db_name=DB_NAME, table_name=table_name))
        results = conn.execute(
            get_my_friends_records_ids_sql,
            my_user_id=my_user_id
        )

        records_ids = [record.id for record in results]

        if 'predicate' in serializedSkygearQuery:
            pass
            original_predicate = serializedSkygearQuery['predicate']
            serializedSkygearQuery['predicate'] = [
                'and',
                [
                    'in',
                    {
                        '$type': 'keypath',
                        '$val': '_id'
                    },
                    records_ids
                ],
                original_predicate
            ]
        else:
            serializedSkygearQuery['predicate'] = [
                'in',
                {
                    '$type': 'keypath',
                    '$val': '_id'
                },
                records_ids
            ]

        container = SkygearContainer(api_key=options.apikey)
        return container.send_action(
            'record:query',
            serializedSkygearQuery
        )


def add_record_to_index(record_type):
    def after_save_add_record_to_index(record, original_record, db):
        if original_record is not None:
            return

        record_id = record.id.key
        record_owner_id = record.owner_id

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
                uuid_generate_v4() as _id,
                '' as _database_id,
                f1.left_id as _owner_id,
                current_timestamp as _created_at,
                f1.left_id as _created_by,
                current_timestamp as _updated_at,
                f1.left_id as _created_by,
                '[]'::jsonb as _access,
                f1.left_id as left_id,
                :record_owner_id as right_id,
                :record_id as record_ref
            FROM {db_name}._friend f1
            JOIN {db_name}._friend f2
            ON f1.right_id = f2.left_id
            WHERE f1.right_id = :record_owner_id
            AND f2.left_id = :record_owner_id
            AND f1.left_id = f2.right_id
        '''.format(
            db_name=DB_NAME,
            table_name=table_name,
            record_type=record_type
        ))

        db.execute(
            create_index_sql,
            record_owner_id=record_owner_id,
            record_id=record_id
        )

    return after_save_add_record_to_index

for record_type in SOCIAL_FEED_RECORD_TYPES:
    after_save_add_record_to_index = add_record_to_index(record_type)
    after_save(record_type, async=True)(after_save_add_record_to_index)
