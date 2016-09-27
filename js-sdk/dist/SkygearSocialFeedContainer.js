const skygear = require('skygear');

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

}

module.exports = new SkygearSocialFeedContainer();
