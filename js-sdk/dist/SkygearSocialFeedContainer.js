const skygear = require('skygear');
const QueryResult = require('skygear/dist/query_result');

function SkygearSocialFeedContainer() {

  this.addFriend = function addFriend(user) {
    return this.addFriends([user]);
  }

  this.addFriends = function addFriends(users) {
    const beFriend = new skygear.relation.Friend(users);
    const context = this;
    return skygear.relation.add(beFriend).then(function(response) {
      const maybeMyFriends = response.success;
      context.createSocialFeedIndex(maybeMyFriends);
      return Promise.resolve(response);
    });
  }

  this.createSocialFeedIndex = function createSocialFeedIndex(mayBeMyFriends) {
    return skygear.lambda('social_feed:create_index', [
      mayBeMyFriends
    ]).then(function(response) {
      return Promise.resolve(response);
    }, function(error) {
      console.log('Error when create index', error);
    });
  }

  this.queryMyFriendsRecords = function queryMyFriendsRecords(query) {
    const Cls = query.recordCls;
    const serializedQuery = query.toJSON();
    return skygear.lambda('social_feed:query_my_friends_records', [
      serializedQuery
    ]).then(function(response) {
      const records = response.result.map(function (attrs) {
        return new Cls(attrs);
      });
      const result = QueryResult.createFromResult(records);
      return Promise.resolve(result);
    }, function(error) {
      return Promise.reject(error);
    });
  }

}

module.exports = new SkygearSocialFeedContainer();
