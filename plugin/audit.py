import json

from skygear import (
    every,
)
from skygear.utils import db
import sqlalchemy as sa

from .options import (
    DB_NAME,
    SOCIAL_FEED_FANOUT_POLICY_JSON_STR,
    SOCIAL_FEED_RECORD_TYPES,
    SOCIAL_FEED_TABLE_PREFIX,
)
from .relation import (
    DIRECTION_MUTUAL,
    DIRECTION_OUTWARD,
    RELATION_TABLE_MAP,
)
from .table_name import (
    name_for_relation_index,
)


def remove_relation_index_if_fanout_policy_change_to_false(conn, relation,
                                                           record_type):
    table_name = name_for_relation_index(
        prefix=SOCIAL_FEED_TABLE_PREFIX,
        relation=relation,
        record_type=record_type
    )
    relation_fanout_policy = {
        relation: False
    }
    remove_feed_index_sql = sa.text('''
        DELETE FROM {db_name}.{table_name} feed_table
        WHERE feed_table.right_id IN (
            SELECT _id
            FROM {db_name}.user
            WHERE social_feed_fanout_policy_is_dirty IS TRUE
            AND COALESCE(
                social_feed_fanout_policy,
                '{default_fanout_policy}' ::jsonb
            ) @> '{relation_fanout_policy}'::jsonb
        )
    '''.format(
        db_name=DB_NAME,
        table_name=table_name,
        default_fanout_policy=SOCIAL_FEED_FANOUT_POLICY_JSON_STR,
        relation_fanout_policy=json.dumps(relation_fanout_policy)
    ))
    conn.execute(remove_feed_index_sql)


def reindex_mutual_relation_index_if_fanout_policy_change_to_true(conn,
                                                                  relation,
                                                                  record_type):
    table_name = name_for_relation_index(
        prefix=SOCIAL_FEED_TABLE_PREFIX,
        relation=relation,
        record_type=record_type
    )
    relation_table = RELATION_TABLE_MAP[relation]
    relation_fanout_policy = {
        relation: True
    }
    reindex_feed_sql = sa.text('''
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
            uuid_generate_v4()::text as _id,
            '' as _database_id,
            t.left_id as _owner_id,
            current_timestamp as _created_at,
            t.left_id as created_by,
            current_timestamp as _updated_at,
            t.left_id as _updated_by,
            '[]'::jsonb as _access,
            t.left_id as left_id,
            t.right_id as right_id,
            t.record_ref as record_ref
        FROM (
            SELECT
                f1.left_id as left_id,
                f1.right_id as right_id,
                record_table._id as record_ref
            FROM {db_name}.{relation_table} f1
            JOIN {db_name}.user user_table
            ON (
                user_table._id = f1.right_id
                AND user_table.social_feed_fanout_policy_is_dirty = TRUE
                AND COALESCE(
                    social_feed_fanout_policy,
                    '{default_fanout_policy}'::jsonb
                ) @> '{relation_fanout_policy}'::jsonb
            )
            JOIN {db_name}.{relation_table} f2
            ON f1.left_id = f2.right_id AND f1.right_id = f2.left_id
            JOIN {db_name}.{record_type} record_table
            ON record_table._owner_id = f1.right_id
            EXCEPT
            SELECT
                left_id,
                right_id,
                record_ref
            FROM {db_name}.{table_name}
        ) AS t
    '''.format(
        db_name=DB_NAME,
        table_name=table_name,
        relation_table=relation_table,
        record_type=record_type,
        default_fanout_policy=SOCIAL_FEED_FANOUT_POLICY_JSON_STR,
        relation_fanout_policy=json.dumps(relation_fanout_policy)
    ))
    conn.execute(reindex_feed_sql)


def reindex_outward_relation_index_if_fanout_policy_change_to_true(
        conn,
        relation,
        record_type):
    table_name = name_for_relation_index(
        prefix=SOCIAL_FEED_TABLE_PREFIX,
        relation=relation,
        record_type=record_type
    )
    relation_table = RELATION_TABLE_MAP[relation]
    relation_fanout_policy = {
        relation: True
    }
    reindex_feed_sql = sa.text('''
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
            uuid_generate_v4()::text as _id,
            '' as _database_id,
            t.left_id as _owner_id,
            current_timestamp as _created_at,
            t.left_id as created_by,
            current_timestamp as _updated_at,
            t.left_id as _updated_by,
            '[]'::jsonb as _access,
            t.left_id as left_id,
            t.right_id as right_id,
            t.record_ref as record_ref
        FROM (
            SELECT
                f1.left_id as left_id,
                f1.right_id as right_id,
                record_table._id as record_ref
            FROM {db_name}.{relation_table} f1
            JOIN {db_name}.user user_table
            ON (
                user_table._id = f1.right_id
                AND user_table.social_feed_fanout_policy_is_dirty = TRUE
                AND COALESCE(
                    social_feed_fanout_policy,
                    '{default_fanout_policy}'::jsonb
                ) @> '{relation_fanout_policy}'::jsonb
            )
            JOIN {db_name}.{record_type} record_table
            ON record_table._owner_id = f1.right_id
            EXCEPT
            SELECT
                left_id,
                right_id,
                record_ref
            FROM {db_name}.{table_name}
        ) AS t
    '''.format(
        db_name=DB_NAME,
        table_name=table_name,
        relation_table=relation_table,
        record_type=record_type,
        default_fanout_policy=SOCIAL_FEED_FANOUT_POLICY_JSON_STR,
        relation_fanout_policy=json.dumps(relation_fanout_policy)
    ))
    conn.execute(reindex_feed_sql)


def reindex_relation_index_if_fanout_policy_change_to_true(conn, relation,
                                                           relation_direction,
                                                           record_type):
    if relation_direction == DIRECTION_MUTUAL:
        reindex_mutual_relation_index_if_fanout_policy_change_to_true(
            conn=conn,
            relation=relation,
            record_type=record_type
        )
    elif relation_direction == DIRECTION_OUTWARD:
        reindex_outward_relation_index_if_fanout_policy_change_to_true(
            conn=conn,
            relation=relation,
            record_type=record_type,
        )


def reset_social_feed_fanout_policy_is_dirty_flag(conn):
    reset_flag_sql = sa.text('''
        UPDATE {db_name}.user
        SET social_feed_fanout_policy_is_dirty = FALSE
        WHERE social_feed_fanout_policy_is_dirty = TRUE
    '''.format(db_name=DB_NAME))
    conn.execute(reset_flag_sql)


def register_update_index_if_fanout_policy_change():
    @every("@every 15m")
    def update_index_if_fanout_policy_change():
        with db.conn() as conn:
            for record_type in SOCIAL_FEED_RECORD_TYPES:
                remove_relation_index_if_fanout_policy_change_to_false(
                    conn=conn,
                    relation='friends',
                    record_type=record_type
                )
                remove_relation_index_if_fanout_policy_change_to_false(
                    conn=conn,
                    relation='following',
                    record_type=record_type
                )
                reindex_relation_index_if_fanout_policy_change_to_true(
                    conn=conn,
                    relation='friends',
                    relation_direction=DIRECTION_MUTUAL,
                    record_type=record_type
                )
                reindex_relation_index_if_fanout_policy_change_to_true(
                    conn=conn,
                    relation='following',
                    relation_direction=DIRECTION_OUTWARD,
                    record_type=record_type
                )
            reset_social_feed_fanout_policy_is_dirty_flag(conn)
