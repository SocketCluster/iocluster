var EventEmitter = require('events').EventEmitter;
var ndata = require('ndata');
var async = require('async');
var ClientCluster = require('./clientcluster').ClientCluster;
var KeyManager = require('./keymanager').KeyManager;
var Channel = require('./channel');
var utils = require('./utils');
var isEmpty = utils.isEmpty;
var domain = require('domain');


var AbstractDataClient = function (dataClient) {
  this._dataClient = dataClient;
};

AbstractDataClient.prototype = Object.create(EventEmitter.prototype);

AbstractDataClient.prototype.set = function () {
  arguments[0] = this._localizeDataKey(arguments[0]);
  this._dataClient.set.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.expire = function () {
  var keys = arguments[0];
  for (var i in keys) {
    keys[i] = this._localizeDataKey(keys[i]);
  }
  arguments[0] = keys;
  
  this._dataClient.expire.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.unexpire = function () {
  var keys = arguments[0];
  for (var i in keys) {
    keys[i] = this._localizeDataKey(keys[i]);
  }
  arguments[0] = keys;
  
  this._dataClient.unexpire.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.add = function () {
  arguments[0] = this._localizeDataKey(arguments[0]);
  this._dataClient.add.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.get = function () {
  arguments[0] = this._localizeDataKey(arguments[0]);
  this._dataClient.get.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.getRange = function () {
  arguments[0] = this._localizeDataKey(arguments[0]);
  this._dataClient.getRange.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.getAll = function (callback) {
  var clientRootKey = this._localizeDataKey();
  this._dataClient.get.call(this._dataClient, clientRootKey, callback);
};

AbstractDataClient.prototype.count = function () {
  arguments[0] = this._localizeDataKey(arguments[0]);
  this._dataClient.count.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.remove = function () {
  arguments[0] = this._localizeDataKey(arguments[0]);
  this._dataClient.remove.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.removeRange = function () {
  arguments[0] = this._localizeDataKey(arguments[0]);
  this._dataClient.removeRange.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.removeAll = function (callback) {
  var clientRootKey = this._localizeDataKey();
  this._dataClient.set.call(this._dataClient, clientRootKey, {}, callback);
};

AbstractDataClient.prototype.splice = function () {
  arguments[0] = this._localizeDataKey(arguments[0]);
  this._dataClient.splice.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.pop = function () {
  arguments[0] = this._localizeDataKey(arguments[0]);
  this._dataClient.pop.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.hasKey = function () {
  arguments[0] = this._localizeDataKey(arguments[0]);
  this._dataClient.hasKey.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.stringify = function (value) {
  return this._dataClient.stringify(value);
};

AbstractDataClient.prototype.extractKeys = function (object) {
  return this._dataClient.extractKeys(object);
};

AbstractDataClient.prototype.extractValues = function (object) {
  return this._dataClient.extractValues(object);
};

/*
  run(queryFn,[ data, callback])
*/
AbstractDataClient.prototype.run = function () {
  var options = {
    baseKey: this._localizeDataKey()
  };
  
  var callback;
  
  if (arguments[1] instanceof Function) {
    callback = arguments[1];
  } else {
    options.data = arguments[1];
    callback = arguments[2];
  }
  if (arguments[1] && !(arguments[1] instanceof Function)) {
    options.data = arguments[1];
  }
  this._dataClient.run(arguments[0], options, callback);
};


var Global = function (privateClientCluster, publicClientCluster, ioClusterClient) {
  AbstractDataClient.call(this, publicClientCluster);
  
  this._privateClientCluster = privateClientCluster;
  this._publicClientCluster = publicClientCluster;
  this._ioClusterClient = ioClusterClient;
  this._keyManager = new KeyManager();
  this._channelEmitter = new EventEmitter();
  this._channels = {};
  
  this._messageHander = this._handleChannelMessage.bind(this);
  
  this._ioClusterClient.on('message', this._messageHander);
};

Global.prototype = Object.create(AbstractDataClient.prototype);

Global.prototype.destroy = function () {
  this._ioClusterClient.removeListener('message', this._messageHander);
};

Global.prototype._localizeDataKey = function (key) {
  return null;
};

Global.prototype._handleChannelMessage = function (message) {
  var channelName = message.channel;
  if (this.isSubscribed(channelName)) {
    this._channelEmitter.emit(channelName, message.data);
  }
};

Global.prototype._triggerChannelSubscribe = function (channel) {
  var channelName = channel.name;
  
  channel.state = channel.SUBSCRIBED;
  
  channel.emit('subscribe', channelName);
  EventEmitter.prototype.emit.call(this, 'subscribe', channelName);
};

Global.prototype._triggerChannelSubscribeFail = function (err, channel) {
  var channelName = channel.name;
  
  channel.state = channel.UNSUBSCRIBED;
  
  channel.emit('subscribeFail', err, channelName);
  EventEmitter.prototype.emit.call(this, 'subscribeFail', err, channelName);
};

Global.prototype._triggerChannelUnsubscribe = function (channel, newState) {
  var channelName = channel.name;
  var oldState = channel.state;
  
  if (newState) {
    channel.state = newState;
  } else {
    channel.state = channel.UNSUBSCRIBED;
  }
  if (oldState == channel.SUBSCRIBED) {
    channel.emit('unsubscribe', channelName);
    EventEmitter.prototype.emit.call(this, 'unsubscribe', channelName);
  }
};

Global.prototype.publish = function (channelName, data, callback) {
  this._ioClusterClient.publish(channelName, data, callback);
};

Global.prototype.subscribe = function (channelName) {
  var self = this;
  
  var channel = this._channels[channelName];
  
  if (!channel) {
    channel = new Channel(channelName, this);
    this._channels[channelName] = channel;
  }
  
  if (channel.state == channel.UNSUBSCRIBED) {
    channel.state = channel.PENDING;
    this._ioClusterClient.subscribe(channelName, function (err) {
      if (err) {
        self._triggerChannelSubscribeFail(err, channel);
      } else {
        self._triggerChannelSubscribe(channel);
      }
    });
  }
  return channel;
};

Global.prototype.unsubscribe = function (channelName) {
  var channel = this._channels[channelName];
  
  if (channel) {
    if (channel.state != channel.UNSUBSCRIBED) {
    
      this._triggerChannelUnsubscribe(channel);
      
      // The only case in which unsubscribe can fail is if the connection is closed or dies.
      // If that's the case, the server will automatically unsubscribe the client so
      // we don't need to check for failure since this operation can never really fail.
      
      this._ioClusterClient.unsubscribe(channelName);
    }
  }
};

Global.prototype.channel = function (channelName) {
  var currentChannel = this._channels[channelName];
  
  if (!currentChannel) {
    currentChannel = new Channel(channelName, this);
    this._channels[channelName] = currentChannel;
  }
  return currentChannel;
};

Global.prototype.destroyChannel = function (channelName) {
  var channel = this._channels[channelName];
  channel.unwatch();
  channel.unsubscribe();
  delete this._channels[channelName];
};

Global.prototype.subscriptions = function (includePending) {
  var subs = [];
  var channel, includeChannel;
  for (var channelName in this._channels) {
    channel = this._channels[channelName];
    
    if (includePending) {
      includeChannel = channel && (channel.state == channel.SUBSCRIBED || 
        channel.state == channel.PENDING);
    } else {
      includeChannel = channel && channel.state == channel.SUBSCRIBED;
    }
    
    if (includeChannel) {
      subs.push(channelName);
    }
  }
  return subs;
};

Global.prototype.isSubscribed = function (channelName, includePending) {
  var channel = this._channels[channelName];
  if (includePending) {
    return !!channel && (channel.state == channel.SUBSCRIBED ||
      channel.state == channel.PENDING);
  }
  return !!channel && channel.state == channel.SUBSCRIBED;
};

Global.prototype.watch = function (channelName, handler) {
  this._channelEmitter.on(channelName, handler);
};

Global.prototype.unwatch = function (channelName, handler) {
  if (handler) {
    this._channelEmitter.removeListener(channelName, handler);
  } else {
    this._channelEmitter.removeAllListeners(channelName);
  }
};

Global.prototype.watchers = function (channelName) {
  return this._channelEmitter.listeners(channelName);
};

Global.prototype.setMapper = function (mapper) {
  this._publicClientCluster.setMapper(mapper);
};

Global.prototype.getMapper = function () {
  return this._publicClientCluster.getMapper();
};

Global.prototype.map = function () {
  return this._publicClientCluster.map.apply(this._publicClientCluster, arguments);
};


var IOCluster = module.exports.IOCluster = function (options) {
  var self = this;

  var dataServer;
  this._dataServers = [];
  
  var readyCount = 0;
  var len = options.stores.length;
  var firstTime = true;
  
  for (var i=0; i<len; i++) {
    var launchServer = function (i) {
      var socketPath = options.stores[i];
      
      dataServer = ndata.createServer({
        id: i,
        instanceId: options.instanceId,
        socketPath: socketPath,
        secretKey: options.secretKey,
        expiryAccuracy: options.expiryAccuracy,
        downgradeToUser: options.downgradeToUser,
        storeControllerPath: options.appStoreControllerPath,
        processTermTimeout: options.processTermTimeout,
        storeOptions: options.storeOptions
      });
      
      self._dataServers[i] = dataServer;
      
      if (firstTime) {
        dataServer.on('ready', function () {
          if (++readyCount >= options.stores.length) {
            firstTime = false;
            self.emit('ready');
          }
        });
      }
      
      dataServer.on('error', function (err) {
        self.emit('error', err);
      });
      
      dataServer.on('exit', function () {
        self.emit('error', new Error('nData server at socket path ' + socketPath + ' exited'));
        launchServer(i);
      });
    };
    
    launchServer(i);
  }
};

IOCluster.prototype = Object.create(EventEmitter.prototype);

IOCluster.prototype.destroy = function () {
  for (var i in this._dataServers) {
    this._dataServers[i].destroy();
  }
};


var IOClusterClient = module.exports.IOClusterClient = function (options) {
  var self = this;
  
  this._errorDomain = domain.createDomain();
  this._errorDomain.on('error', function (err) {
    self.emit('error', err);
  });
  
  this._socketChannelLimit = options.socketChannelLimit;
  this.options = options;
  
  this._keyManager = new KeyManager();
  this._ready = false;
  
  var dataClient;
  var dataClients = [];
  
  for (var i in options.stores) {
    var socketPath = options.stores[i];
    dataClient = ndata.createClient({
      socketPath: socketPath,
      secretKey: options.secretKey
    });
    dataClients.push(dataClient);
  }
  
  var hasher = function (key) {
    var ch;
    var hash = 0;
    if (typeof key == 'number') {
      hash = key;
    } else {
      if (key == null || key.length == 0) {
        return hash;
      }
      for (var i = 0; i < key.length; i++) {
        ch = key.charCodeAt(i);
        hash = ((hash << 5) - hash) + ch;
        hash = hash & hash;
      }
    }
    return Math.abs(hash) % dataClients.length;
  };
  
  var channelMethods = {
    publish: true,
    subscribe: true,
    unsubscribe: true,
    isSubscribed: true
  };
  
  this._privateMapper = function (key, method, clientIds) {
    if (channelMethods[method]) {
      if (key == null) {
        return clientIds;
      }
      return hasher(key);
    }
    
    if (method == 'query' || method == 'run') {
      if (key.mapIndex) {
        return hasher(key.mapIndex);
      }
      return 0;
    }
    if (key instanceof Array) {
      if (key[2] == null) {
        return clientIds;
      }
      return hasher(key[2]);
    }
    return hasher(key);
  };
  
  this._publicMapper = function (key, method, clientIds) {
    return 0;
  };
  
  this._privateClientCluster = new ClientCluster(dataClients);
  this._privateClientCluster.setMapper(this._privateMapper);
  this._errorDomain.add(this._privateClientCluster);
  
  this._publicClientCluster = new ClientCluster(dataClients);
  this._publicClientCluster.setMapper(this._publicMapper);
  this._errorDomain.add(this._publicClientCluster);
  
  this._sockets = {};
  
  this._globalSubscriptions = {};
  this._globalClient = new Global(this._privateClientCluster, this._publicClientCluster, this);
  
  this._clientSubscribers = {};
  
  var readyNum = 0;
  var firstTime = true;
  
  var dataClientReady = function () {
    if (++readyNum >= dataClients.length && firstTime) {
      firstTime = false;
      self._ready = true;
      self.emit('ready');
    }
  };
  
  for (var j in dataClients) {
    dataClients[j].on('ready', dataClientReady);
  }
  
  this._privateClientCluster.on('message', this._handleGlobalMessage.bind(this));
};

IOClusterClient.prototype = Object.create(EventEmitter.prototype);

IOClusterClient.prototype.destroy = function (callback) {
  this._privateClientCluster.removeAll(callback);
};

IOClusterClient.prototype.on = function (event, listener) {
  if (event == 'ready' && this._ready) {
    listener();
  } else {
    EventEmitter.prototype.on.apply(this, arguments);
  }
};

IOClusterClient.prototype._handshake = function (socket, callback) {
  var self = this;
  
  if (socket.id == null) {
    callback && callback("Failed handshake - Invalid handshake data");
  } else {
    callback && callback();
  }
};

// Add pub/sub functions to socket object
IOClusterClient.prototype._decorateSocket = function (socket) {
  var self = this;
  
  socket.channelSubscriptions = {};
  socket.channelSubscriptionCount = 0;
  
  // Decorate the socket with a kickOut method to allow us to kick it out
  // from a particular sub/sub channels at any time.
  socket.kickOut = function (channel, message, callback) {
    if (channel == null) {
      for (var i in socket.channelSubscriptions) {
        socket.emit('kickOut', {message: message, channel: i});
      }
    } else {
      socket.emit('kickOut', {message: message, channel: channel});
    }
    self.unsubscribeClientSocket(socket, channel, callback);
  };
  
  socket.subscriptions = function () {
    var subs = [];
    for (var i in socket.channelSubscriptions) {
      subs.push(i);
    }
    return subs;
  };
  
  socket.isSubscribed = function (channel) {
    return !!socket.channelSubscriptions[channel];
  };
};

IOClusterClient.prototype.bind = function (socket, callback) {
  var self = this;
  
  callback = this._errorDomain.bind(callback);
  
  this._decorateSocket(socket);
  
  this._handshake(socket, function (err) {
    if (err) {
      callback(err, socket, true);
    } else {
      self._sockets[socket.id] = socket;
      
      socket.on('subscribe', function (channel, res) {
        self.subscribeClientSocket(socket, channel, function (err) {
          if (err) {
            res(err);
            self.emit('notice', err);
          } else {
            res();
          }
        });
      });
      
      socket.on('unsubscribe', function (channel, res) {
        self.unsubscribeClientSocket(socket, channel, function (err, isNotice) {
          if (err) {
            res(err);
            
            if (isNotice) {
              self.emit('notice', err);
            } else {
              self.emit('error', err);
            }
          } else {
            res();
          }
        });
      });

      callback(null, socket);
    }
  });
};

IOClusterClient.prototype.unbind = function (socket, callback) {
  var self = this;
  
  callback = this._errorDomain.bind(callback);
  
  async.waterfall([
    function () {
      var cb = arguments[arguments.length - 1];
      socket.removeAllListeners('subscribe');
      socket.removeAllListeners('unsubscribe');
      self.unsubscribeClientSocket(socket, null, cb);
    },
    function () {
      var cb = arguments[arguments.length - 1];
      delete self._sockets[socket.id];
      cb();
    }
  ],
  function (err) {
    callback(err, socket);
  });
};

IOClusterClient.prototype.global = function () {
  return this._globalClient;
};

IOClusterClient.prototype._dropUnusedSubscriptions = function (channel, callback) {
  var self = this;
  
  if (isEmpty(this._clientSubscribers[channel])) {
    delete this._clientSubscribers[channel];
    if (!this._globalSubscriptions[channel]) {
      self._privateClientCluster.unsubscribe(channel, callback);
      return;
    }
  }
  callback && callback();
};

IOClusterClient.prototype.publish = function (channelName, data, callback) {
  this._privateClientCluster.publish(channelName, data, callback);
};

IOClusterClient.prototype.subscribe = function (channel, callback) {
  var self = this;
  
  if (!this._globalSubscriptions[channel]) {
    this._globalSubscriptions[channel] = 'pending';
    this._privateClientCluster.subscribe(channel, function (err) {
      if (err) {
        delete self._globalSubscriptions[channel];
        self._dropUnusedSubscriptions(channel);
      } else {
        self._globalSubscriptions[channel] = true;
      }
      callback && callback(err);
    });
  } else {
    callback && callback();
  }
};

IOClusterClient.prototype.unsubscribe = function (channel, callback) {
  delete this._globalSubscriptions[channel];
  this._dropUnusedSubscriptions(channel, callback);
};

IOClusterClient.prototype.unsubscribeAll = function (callback) {
  var self = this;
  
  var tasks = [];
  for (var channel in this._globalSubscriptions) {
    delete this._globalSubscriptions[channel];
    (function (channel) {
      tasks.push(function (cb) {
        self._dropUnusedSubscriptions(channel, cb);
      });
    })(channel);
  }
  async.parallel(tasks, callback);
};

IOClusterClient.prototype.isSubscribed = function (channel, includePending) {
  if (includePending) {
    return !!this._globalSubscriptions[channel];
  }
  return this._globalSubscriptions[channel] === true;
};

IOClusterClient.prototype.subscribeClientSocket = function (socket, channels, callback) {
  var self = this;
  
  if (channels instanceof Array) {
    var tasks = [];
    for (var i in channels) {
      (function (channel) {
        tasks.push(function (cb) {
          self._subscribeSingleClientSocket(socket, channel, cb);
        });
      })(channels[i]);
    }
    async.waterfall(tasks, function (err) {
      callback && callback(err);
    });
  } else {
    this._subscribeSingleClientSocket(socket, channels, callback);
  }
};

IOClusterClient.prototype.unsubscribeClientSocket = function (socket, channels, callback) {
  var self = this;
  
  if (channels == null) {
    channels = [];
    for (var channel in socket.channelSubscriptions) {
      channels.push(channel);
    }
  }
  if (channels instanceof Array) {
    var tasks = [];
    for (var i in channels) {
      (function (channel) {
        tasks.push(function (cb) {
          self._unsubscribeSingleClientSocket(socket, channel, cb);
        });
      })(channels[i]);
    }
    async.waterfall(tasks, function (err) {
      callback && callback(err);
    });
  } else {
    this._unsubscribeSingleClientSocket(socket, channels, callback);
  }
};

IOClusterClient.prototype._subscribeSingleClientSocket = function (socket, channel, callback) {
  var self = this;
  
  if (this._socketChannelLimit && socket.channelSubscriptionCount >= this._socketChannelLimit) {
    callback('Socket ' + socket.id + ' tried to exceed the channel subscription limit of ' +
      this._socketChannelLimit);
  } else {
    if (socket.channelSubscriptionCount == null) {
      socket.channelSubscriptionCount = 0;
    }
    
    if (socket.channelSubscriptions == null) {
      socket.channelSubscriptions = {};
    }
    if (socket.channelSubscriptions[channel] == null) {
      socket.channelSubscriptions[channel] = true;
      socket.channelSubscriptionCount++;
    }
    
    var addSubscription = function (err) {
      if (err) {
        delete socket.channelSubscriptions[channel];
        socket.channelSubscriptionCount--;
      } else {
        if (!self._clientSubscribers[channel]) {
          self._clientSubscribers[channel] = {};
        }
        self._clientSubscribers[channel][socket.id] = socket;
      }
      callback && callback(err);
    };
    
    this._privateClientCluster.subscribe(channel, addSubscription);
  }
};

IOClusterClient.prototype._unsubscribeSingleClientSocket = function (socket, channel, callback) {
  if (this._clientSubscribers[channel]) {
    delete this._clientSubscribers[channel][socket.id];
    if (isEmpty(this._clientSubscribers[channel])) {
      delete this._clientSubscribers[channel];
    }
  }
  delete socket.channelSubscriptions[channel];
  if (socket.channelSubscriptionCount != null) {
    socket.channelSubscriptionCount--;
  }
  this._dropUnusedSubscriptions(channel, callback);
};

IOClusterClient.prototype._handleGlobalMessage = function (channel, message, options) {
  var packet = {
    channel: channel,
    data: message
  };
  
  var subscriberSockets = this._clientSubscribers[channel];
  
  for (var i in subscriberSockets) {
    subscriberSockets[i].emit('publish', packet);
  }
  
  this.emit('message', packet);
};
