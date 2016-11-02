# Social Feed Plugin for Skygear

## Configuration
The plugin is configured by environment variables.

* `SKYGEAR_SOCIAL_FEED_RECORD_TYPES` - String of array of your record types which you want them to be indexed
* `SKYGEAR_SOCIAL_FEED_FANOUT_POLICY` - Json string of record fanout policy 

## Initialization

curl `http://<your-skygear-endpoint>/social-feed-init`

## JS API

### addFriend(user)
add a friend and then indexing your friend's records

| Param  | Type                | Description  |
| ------ | ------------------- | ------------ |
| user  | <code>Skygear User Object</code> | |

### addFriends(users)
add friends and then indexing your friends' record

| Param  | Type                | Description  |
| ------ | ------------------- | ------------ |
| users  | <code>array of Skygear User Object</code> | |

### addFollowing(user)
add a followee and then indexing your followee's records

| Param  | Type                | Description  |
| ------ | ------------------- | ------------ |
| user  | <code>Skygear User Object</code> | |

### addFollowings(users)
add followees and then indexing your followees' record

| Param  | Type                | Description  |
| ------ | ------------------- | ------------ |
| users  | <code>array of Skygear User Object</code> | |

### removeFriend(user)
remove a friend and then remove the records indexing of that friend

| Param  | Type                | Description  |
| ------ | ------------------- | ------------ |
| user  | <code>Skygear User Object</code> | |

### removeFriends(users)
remove friends and then remove the records indexing of those friends

| Param  | Type                | Description  |
| ------ | ------------------- | ------------ |
| users  | <code>array of Skygear User Object</code> | |

### removeFollowing(user)
unfollow a user and then emove the records indexing of that user

| Param  | Type                | Description  |
| ------ | ------------------- | ------------ |
| user  | <code>Skygear User Object</code> | |

### removeFollowings(users)
unfollow users and then emove the records indexing of those users

| Param  | Type                | Description  |
| ------ | ------------------- | ------------ |
| users  | <code>array of Skygear User Object</code> | |

### createSocialFeedIndexForFriends(users)
Index users' records if you are friends

| Param  | Type                | Description  |
| ------ | ------------------- | ------------ |
| users  | <code>array of Skygear User Object</code> | |

### createSocialFeedIndexForFollowees(users)
Index followees' records

| Param  | Type                | Description  |
| ------ | ------------------- | ------------ |
| users  | <code>array of Skygear User Object</code> | |

### removeSocialFeedIndexForFriends(users)
Remove the indexing of users' records if you are friends

| Param  | Type                | Description  |
| ------ | ------------------- | ------------ |
| users  | <code>array of Skygear User Object</code> | |

### removeSocialFeedIndexForFollowees(users)
Remove the indexing of users' records if you are followers of them

| Param  | Type                | Description  |
| ------ | ------------------- | ------------ |
| users  | <code>array of Skygear User Object</code> | |

### queryMyFriendsRecords(query)
filter out the records which are not created by your friends

| Param  | Type                | Description  |
| ------ | ------------------- | ------------ |
| query  | <code>Skygear Query</code> | |

### queryMyFolloweesRecords(query)
filter out the records which are not created by your followees

| Param  | Type                | Description  |
| ------ | ------------------- | ------------ |
| query  | <code>Skygear Query</code> | |

### reindexSocialFeedIndexForFriends()
Reindex your index to your friends' records

| Param  | Type                | Description  |
| ------ | ------------------- | ------------ |

### reindexSocialFeedIndexForFollowings()
Reindex your index to your followees' records

| Param  | Type                | Description  |
| ------ | ------------------- | ------------ |

### enableFanoutToFriends()
Enable your records fanout to your friends' feed

| Param  | Type                | Description  |
| ------ | ------------------- | ------------ |

### disableFanoutToFriends()
Disable your records fanout to your friends' feed

| Param  | Type                | Description  |
| ------ | ------------------- | ------------ |

### enableFanoutToFollowers()
Enable your records fanout to your followers' feed

| Param  | Type                | Description  |
| ------ | ------------------- | ------------ |

### disableFanoutToFollowers()
Disable your records fanout to your followers' feed

| Param  | Type                | Description  |
| ------ | ------------------- | ------------ |
