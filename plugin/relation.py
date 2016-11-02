import skygear
from skygear import (
    op,
)
from skygear.utils import db
import sqlalchemy as sa

from .options import (
    DB_NAME,
    SOCIAL_FEED_FANOUT_POLICY_JSON_STR,
    SOCIAL_FEED_RECORD_TYPES,
    SOCIAL_FEED_TABLE_PREFIX,
)

from .table_name import (
    name_for_followings_relation_index,
    name_for_friends_relation_index
)

from .user import (
    should_record_be_indexed,
)

DIRECTION_MUTUAL = 'mutual'
DIRECTION_INWARD = 'inward'
DIRECTION_OUTWARD = 'outward'

RELATION_TABLE_MAP = {
    'friends': '_friend',
    'following': '_follow',
}


def register_create_index_for_friends():
    @op('social_feed:create_index_for_friends', user_required=True)
    def social_feed_create_index_for_friends(maybe_my_friends):
        if len(maybe_my_friends) <= 0:
            return

        with db.conn() as conn:
            my_user_id = skygear.utils.context.current_user_id()
            maybe_my_friend_ids = [
                user['user_id'] for user in maybe_my_friends
            ]
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

            should_fanout_my_records = should_record_be_indexed(
                DB_NAME,
                SOCIAL_FEED_RECORD_TYPES,
                conn,
                my_user_id,
                'friends'
            )

            for record_type in SOCIAL_FEED_RECORD_TYPES:
                table_name = name_for_friends_relation_index(
                    prefix=SOCIAL_FEED_TABLE_PREFIX,
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
                        record_table._owner_id as right_id,
                        record_table._id as record_ref
                    FROM {db_name}.{record_type} record_table
                    JOIN {db_name}.user user_table
                    ON (
                        record_table._owner_id = user_table._id
                        AND COALESCE(
                                user_table.social_feed_fanout_policy,
                                '{default_fanout_policy}'::jsonb
                            ) @> '{req_fanout_policy}'::jsonb
                    )
                    WHERE record_table._owner_id in :my_friend_ids
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
                    record_type=record_type,
                    default_fanout_policy=SOCIAL_FEED_FANOUT_POLICY_JSON_STR,
                    req_fanout_policy='{"friends": true}'
                ))
                conn.execute(
                    create_my_friends_records_index_sql,
                    my_user_id=my_user_id,
                    my_friend_ids=my_friend_ids_tuple
                )

                if should_fanout_my_records:
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


def register_create_index_for_followee():
    @op('social_feed:create_index_for_followees', user_required=True)
    def create_index_for_followee(followees):
        if len(followees) <= 0:
            return

        with db.conn() as conn:
            my_user_id = skygear.utils.context.current_user_id()
            my_followees_ids = [followee['user_id'] for followee in followees]
            my_followees_ids_tuple = tuple(my_followees_ids)

            for record_type in SOCIAL_FEED_RECORD_TYPES:
                table_name = name_for_followings_relation_index(
                    prefix=SOCIAL_FEED_TABLE_PREFIX,
                    record_type=record_type
                )

                create_my_followees_records_index_sql = sa.text('''
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
                        record_table._owner_id as right_id,
                        record_table._id as record_ref
                    FROM {db_name}.{record_type} record_table
                    JOIN {db_name}.user user_table
                    ON (
                        record_table._owner_id = user_table._id
                        AND COALESCE(
                                user_table.social_feed_fanout_policy,
                                '{default_fanout_policy}'::jsonb
                            ) @> '{req_fanout_policy}'::jsonb
                    )
                    WHERE record_table._owner_id in :my_followees_ids
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
                    record_type=record_type,
                    default_fanout_policy=SOCIAL_FEED_FANOUT_POLICY_JSON_STR,
                    req_fanout_policy='{"following": true}'
                ))
                conn.execute(
                    create_my_followees_records_index_sql,
                    my_user_id=my_user_id,
                    my_followees_ids=my_followees_ids_tuple
                )


def register_remove_index_for_friends():
    @op('social_feed:remove_index_for_friends', user_required=True)
    def remove_index_for_friends(friends):
        if len(friends) <= 0:
            return

        with db.conn() as conn:
            my_user_id = skygear.utils.context.current_user_id()
            my_friends_ids = [friend['user_id'] for friend in friends]
            my_friends_ids_tuple = tuple(my_friends_ids)

            for record_type in SOCIAL_FEED_RECORD_TYPES:
                table_name = name_for_friends_relation_index(
                    prefix=SOCIAL_FEED_TABLE_PREFIX,
                    record_type=record_type
                )

                remove_my_friends_records_sql = sa.text('''
                    DELETE from {db_name}.{table_name}
                    WHERE (
                        left_id = :my_user_id
                        AND right_id in :my_friends_ids
                    )
                    OR (right_id = :my_user_id AND left_id in :my_friends_ids)
                '''.format(db_name=DB_NAME, table_name=table_name))
                conn.execute(
                    remove_my_friends_records_sql,
                    my_user_id=my_user_id,
                    my_friends_ids=my_friends_ids_tuple
                )


def register_remove_index_for_followees():
    @op('social_feed:remove_index_for_followees', user_required=True)
    def remove_index_for_followees(followees):
        if len(followees) <= 0:
            return

        with db.conn() as conn:
            my_user_id = skygear.utils.context.current_user_id()
            my_followees_ids = [followee['user_id'] for followee in followees]
            my_followees_ids_tuple = tuple(my_followees_ids)

            for record_type in SOCIAL_FEED_RECORD_TYPES:
                table_name = name_for_followings_relation_index(
                    prefix=SOCIAL_FEED_TABLE_PREFIX,
                    record_type=record_type
                )

                remove_my_friends_records_sql = sa.text('''
                    DELETE from {db_name}.{table_name}
                    WHERE left_id = :my_user_id
                    AND right_id in :my_followees_ids
                '''.format(db_name=DB_NAME, table_name=table_name))
                conn.execute(
                    remove_my_friends_records_sql,
                    my_user_id=my_user_id,
                    my_followees_ids=my_followees_ids_tuple
                )


def register_reindex_for_friends():
    @op('social_feed:reindex_for_friends', user_required=True)
    def reindex_for_friends():
        with db.conn() as conn:
            my_user_id = skygear.utils.context.current_user_id()

            for record_type in SOCIAL_FEED_RECORD_TYPES:
                table_name = name_for_friends_relation_index(
                    prefix=SOCIAL_FEED_TABLE_PREFIX,
                    record_type=record_type
                )

                remove_current_index_sql = sa.text('''
                    DELETE FROM {db_name}.{table_name}
                    WHERE left_id = :my_user_id
                '''.format(db_name=DB_NAME, table_name=table_name))
                conn.execute(
                    remove_current_index_sql,
                    my_user_id=my_user_id
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
                    WHERE _owner_id in (
                        SELECT f1.right_id as id
                        FROM {db_name}._friend f1
                        JOIN {db_name}._friend f2
                        ON f1.right_id = f2.left_id
                        WHERE f1.left_id = :my_user_id
                        AND f2.right_id = :my_user_id
                    )
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
                )


def register_reindex_for_followees():
    @op('social_feed:reindex_for_followees', user_required=True)
    def reindex_for_followees():
        with db.conn() as conn:
            my_user_id = skygear.utils.context.current_user_id()

            for record_type in SOCIAL_FEED_RECORD_TYPES:
                table_name = name_for_followings_relation_index(
                    prefix=SOCIAL_FEED_TABLE_PREFIX,
                    record_type=record_type
                )

                remove_current_index_sql = sa.text('''
                    DELETE FROM {db_name}.{table_name}
                    WHERE left_id = :my_user_id
                '''.format(db_name=DB_NAME, table_name=table_name))
                conn.execute(
                    remove_current_index_sql,
                    my_user_id=my_user_id
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
                    WHERE _owner_id in (
                        SELECT f.right_id as id
                        FROM {db_name}._follow f
                        WHERE f.left_id = :my_user_id
                    )
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
                )
