import copy


def generate_skygear_query_from_indexed_ids(query, records_ids):
    query_clone = copy.deepcopy(query)
    if 'predicate' in query_clone:
        original_predicate = query_clone['predicate']
        query_clone['predicate'] = [
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
        query_clone['predicate'] = [
            'in',
            {
                '$type': 'keypath',
                '$val': '_id'
            },
            records_ids
        ]
    return query_clone
