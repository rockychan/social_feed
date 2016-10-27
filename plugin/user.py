import sqlalchemy as sa


def should_record_be_indexed(db_name, default_fanout_policy,
                             db, user_id, relation):
    get_user_fanout_policy_sql = sa.text('''
        SELECT social_feed_fanout_policy as fanout_policy
        FROM {db_name}.user
        WHERE _id=:user_id
    '''.format(db_name=db_name))
    user_fanout_policy = db.execute(
        get_user_fanout_policy_sql,
        user_id=user_id
    ).first()[0]

    if user_fanout_policy is not None:
        if relation in user_fanout_policy:
            return user_fanout_policy[relation]
        return False

    if relation in default_fanout_policy:
        return default_fanout_policy[relation]

    return False
