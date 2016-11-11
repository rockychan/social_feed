import skygear
from skygear import (
    after_save,
    op,
)
from skygear.container import SkygearContainer
from skygear.options import options
from skygear.utils import db
import sqlalchemy as sa

from .options import (
    DB_NAME,
    SOCIAL_FEED_TABLE_PREFIX,
    SOCIAL_FEED_FANOUT_POLICY,
)

from .table_name import (
    name_for_followings_relation_index,
    name_for_friends_relation_index
)

from .user import (
    should_record_be_indexed,
)


def register_query_my_friends_records():
    @op('social_feed:query_my_friends_records', user_required=True)
    def social_feed_query_my_friends_records(serializedSkygearQuery):
        with db.conn() as conn:
            query_record_type = serializedSkygearQuery['record_type']
            table_name = name_for_friends_relation_index(
                prefix=SOCIAL_FEED_TABLE_PREFIX,
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


def register_query_my_followees_records():
    @op('social_feed:query_my_followees_records', user_required=True)
    def query_my_followees_records(serializedSkygearQuery):
        with db.conn() as conn:
            query_record_type = serializedSkygearQuery['record_type']
            table_name = name_for_followings_relation_index(
                prefix=SOCIAL_FEED_TABLE_PREFIX,
                record_type=query_record_type
            )
            my_user_id = skygear.utils.context.current_user_id()
            get_my_followees_records_ids_sql = sa.text('''
                SELECT record_ref as id
                FROM {db_name}.{table_name}
                WHERE left_id = :my_user_id
            '''.format(db_name=DB_NAME, table_name=table_name))
            results = conn.execute(
                get_my_followees_records_ids_sql,
                my_user_id=my_user_id
            )

            records_ids = [record.id for record in results]

            if 'predicate' in serializedSkygearQuery:
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


def register_after_save_add_record_to_index_for_friends(record_type):
    @after_save(record_type, async=True)
    def after_save_add_record_to_index_for_friends(record, original_record,
                                                   db):
        if original_record is not None:
            return

        record_id = record.id.key
        record_owner_id = record.owner_id

        should_index = should_record_be_indexed(
            DB_NAME,
            SOCIAL_FEED_FANOUT_POLICY,
            db,
            user_id=record_owner_id,
            relation='friends'
        )
        if not should_index:
            return

        table_name = name_for_friends_relation_index(
            prefix=SOCIAL_FEED_TABLE_PREFIX,
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

    return after_save_add_record_to_index_for_friends


def register_after_save_add_record_to_index_for_followers(record_type):
    @after_save(record_type, async=True)
    def after_save_add_record_to_index_for_followers(record, original_record,
                                                     db):
        if original_record is not None:
            return

        record_id = record.id.key
        record_owner_id = record.owner_id

        should_index = should_record_be_indexed(
            DB_NAME,
            SOCIAL_FEED_FANOUT_POLICY,
            db,
            user_id=record_owner_id,
            relation='following'
        )
        if not should_index:
            return

        table_name = name_for_followings_relation_index(
            prefix=SOCIAL_FEED_TABLE_PREFIX,
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
                f.left_id as _owner_id,
                current_timestamp as _created_at,
                f.left_id as _created_by,
                current_timestamp as _updated_at,
                f.left_id as _created_by,
                '[]'::jsonb as _access,
                f.left_id as left_id,
                :record_owner_id as right_id,
                :record_id as record_ref
            FROM {db_name}._follow f
            WHERE f.right_id = :record_owner_id
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
