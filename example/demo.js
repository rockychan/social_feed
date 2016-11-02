const User = skygear.Record.extend('user');
const Post = skygear.Record.extend('post');

class Demo {

  constructor(container, plugin) {
    this.container = container;
    this.plugin = plugin;
    this.user = null;

    this.$configSkygearSection = $('config-skygear');

    this.$configUserSection = $('login-signup-section');
    this.$configUserSection.style.visibility = 'hidden';

    this.$afterLoginSection = $('after-login-section');
    this.$afterLoginSection.style.visibility = 'hidden';

    this.$userSection = $('user-section');
    this.$currentUsername = $('currentUsername');

    this.$allUsers = $('all-users');
    this.$allMyFriends = $('my-friends');

    this.$allMyFriendsPosts = $('all-my-friends-posts');
  }

  configSkygear(endPoint, apiKey) {
    this.container.config({
      endPoint: endPoint,
      apiKey: apiKey
    }).then(() => {
      this.removeConfigSkygearSection();
      this.$configUserSection.style.visibility = 'visible';
    });
  }

  signUp(username, password) {
    this.container.signupWithUsername(username, password).then((user) => {
      const userRecord = new User({
        _id: 'user/' + user.id,
        name: user.username
      });
      this.container.publicDB.save(userRecord);
      this.user = user;
      this.removeLoginSection();
      this.showAfterLoginSection();
      this.displayUser();
      console.log(user);
    }, (e) => {
      alert('Cannot sign up for a moment');
    });
  }

  login(username, password) {
    this.container.loginWithUsername(username, password).then((user) => {
      console.log(user);
      this.user = user;
      this.removeLoginSection();
      this.showAfterLoginSection();
      this.displayUser();
    }, (e) => {
      console.log(e);
      alert('Cannot login for a moment');
    });
  }

  fetchAndShowAllUsers() {
    const userQuery = new skygear.Query(User);
    userQuery.notEqualTo('_id', this.user.id);
    this.container.publicDB.query(userQuery).then((users) => {
      this.$allUsers.innerHTML = '<h3>All Users</h3>';
      for (const user of users) {
        const $el = createElement('p');

        const $username = createElement('label');
        $username.innerHTML = user.name;
        $el.appendChild($username);

        const $addFriend = createElement('button');
        $addFriend.innerHTML = 'Add friend';
        $addFriend.setAttribute('onclick', 'addFriend(\'' + user.id + '\') ');
        $el.appendChild($addFriend);

        this.$allUsers.appendChild($el);
      }
    });
  }

  fetchAllMyFriends() {
    this.container.relation.queryFriend(skygear.currentUser).then((users) => {
      this.$allMyFriends.innerHTML = '<h3>My Friends</h3>';
      for (const user of users) {
        const $el = createElement('p');

        const $username = createElement('label');
        $username.innerHTML = user.username;
        $el.appendChild($username);

        const $addFriend = createElement('button');
        $addFriend.innerHTML = 'Get my friend posts';
        $addFriend.setAttribute(
          'onclick',
          'queryFriendPosts(\'' + user.id + '\') '
        );
        $el.appendChild($addFriend);

        const $removeFriend = createElement('button');
        $removeFriend.innerHTML = 'Remove friend';
        $removeFriend.setAttribute(
          'onclick',
          'removeFriend(\'' + user.id + '\') '
        );
        $el.appendChild($removeFriend);

        this.$allMyFriends.appendChild($el);
      }
    });
  }

  addFriend(userID) {
    const typelessUserID = userID.split('/')[1];
    const myFriend = new skygear.User({
      user_id: typelessUserID
    });
    this.plugin.addFriend(myFriend).then((r) => {
      console.log(r);
      alert('You two are friends now');
    });
  }

  removeFriend(userID) {
    const myFriend = new skygear.User({
      user_id: userID
    });
    this.plugin.removeFriend(myFriend).then((r) => {
      console.log(r);
      alert('You two are not friends now');
    });
  }

  postSomething() {
    const postContent = prompt('What are you thinking?');
    if (postContent) {
      const post = new Post({
        content: postContent,
        author: new skygear.Reference({
          id: 'user/' + this.user.id
        })
      });
      this.container.publicDB.save(post).then(() => {
        alert('Posted!');
      }, () => {
        alert('Your post cannot be posted');
      });
    }
  }

  reindex() {
    this.plugin.reindexSocialFeedIndexForFriends();
  }

  fetchAllMyFriendsPosts() {
    const query = new skygear.Query(Post);
    query.transientInclude('author');
    this.plugin.queryMyFriendsRecords(query).then((results) => {
      this.displayMyFriendsPost(results);
    });
  }

  enableFanout() {
    this.plugin.enableFanoutToFollowers();
  }

  disableFanout() {
    this.plugin.disableFanoutToFriends();
  }

  removeConfigSkygearSection() {
    removeElement(this.$configSkygearSection);
    this.$configSkygearSection = null;
  }

  removeLoginSection() {
    removeElement(this.$configUserSection);
    this.$configUserSection = null;
  }

  showAfterLoginSection() {
    this.$afterLoginSection.style.visibility = 'visible';
  }

  displayUser() {
    if (this.user) {
      this.$currentUsername.innerHTML = this.user.username;
    }
  }

  displayMyFriendsPost(posts) {
    this.$allMyFriendsPosts.innerHTML = '<h3>Post from my friends</h3>';
    for (const post of posts) {
      const postAuthor = post.$transient.author;

      const $el = createElement('p');
      const $content = createElement('label');

      $content.innerHTML = post.content + ' by ' + postAuthor.name;
      $el.appendChild($content);

      this.$allMyFriendsPosts.appendChild($el);
    }
  }
}
