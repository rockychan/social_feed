def name_for_relation_index(prefix, relation, record_type):
    table_name_format = '{prefix}_{relation}_{record_type}'
    return table_name_format.format(
        prefix=prefix,
        relation=relation,
        record_type=record_type
    )


def name_for_friends_relation_index(prefix, record_type):
    return name_for_relation_index(prefix, 'friends', record_type)


def name_for_followings_relation_index(prefix, record_type):
    return name_for_relation_index(prefix, 'following', record_type)
