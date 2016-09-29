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
      context.createSocialFeedIndexForFriends(maybeMyFriends);
      return Promise.resolve(response);
    });
  }

  this.addFollowing = function addFollowing(user) {
    return this.addFollowings([user]);
  }

  this.addFollowings = function addFollowings(users) {
    const toFollow = new skygear.relation.Following(users);
    const context = this;
    return skygear.relation.add(toFollow).then(function(response) {
      const followees = response.success;
      context.createSocialFeedIndexForFollowees(followees);
      return Promise.resolve(response);
    });
  }

  this.removeFriend = function removeFriend(user) {
    return this.removeFriends([user]);
  }

  this.removeFriends = function removeFriends(users) {
    const unFriends = new skygear.relation.Friend(users);
    const context = this;
    return skygear.relation.remove(unFriends).then(function(response) {
      context.removeSocialFeedIndexForFriends(users);
      return Promise.resolve(response);
    });
  }

  this.removeFollowing = function removeFollowing(user) {
    return this.removeFollowings([user]);
  }

  this.removeFollowings = function removeFollowings(users) {
    const unFollow = new skygear.relation.Following(users);
    const context = this;
    return skygear.relation.remove(unFollow).then(function(response) {
      context.removeSocialFeedIndexForFollowees(unFollow);
      return Promise.resolve(response);
    });
  }

  this.createSocialFeedIndexForFriends =
    function createSocialFeedIndexForFriends(mayBeMyFriends) {
      return skygear.lambda('social_feed:create_index_for_friends', [
        mayBeMyFriends
      ]).then(function(response) {
        return Promise.resolve(response);
      }, function(error) {
        console.log('Error when create index', error);
      });
    }

  this.createSocialFeedIndexForFollowees =
    function createSocialFeedIndexForFollowees(followees) {
      return skygear.lambda('social_feed:create_index_for_followees', [
        followees
      ]).then(function(response) {
        return Promise.resolve(response);
      }, function(error) {
        console.log('Error when create index', error);
      });
    }

  this.removeSocialFeedIndexForFriends =
    function removeSocialFeedIndexForFriends(friends) {
      return skygear.lambda('social_feed:remove_index_for_friends', [
        friends
      ]);
    }

  this.removeSocialFeedIndexForFollowees =
    function removeSocialFeedIndexForFollowees(followees) {
      return skygear.lambda('social_feed:remove_index_for_followees', [
        followees
      ]);
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

  this.queryMyFolloweesRecords = function queryMyFolloweesRecords(query) {
    const Cls = query.recordCls;
    const serializedQuery = query.toJSON();
    return skygear.lambda('social_feed:query_my_followees_records', [
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

  this.reindexSocialFeedIndexForFriends =
    function reindexSocialFeedIndexForFriends() {
      return skygear.lambda('social_feed:reindex_for_friends');
    }

}

module.exports = new SkygearSocialFeedContainer();
