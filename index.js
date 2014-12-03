var EventEmitter = require('events').EventEmitter;
var ndata = require('ndata');
var async = require('async');
var LinkedList = require('linkedlist');
var ClientCluster = require('./clientcluster').ClientCluster;
var KeyManager = require('./keymanager').KeyManager;
var domain = require('domain');
var utils = require('./utils');
var isEmpty = utils.isEmpty;


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
  query(query,[ data, callback])
*/
AbstractDataClient.prototype.query = function () {
  var options = {
    baseKey: this._localizeDataKey()
  };
  
  if (arguments[1] && !(arguments[1] instanceof Function)) {
    options.data = arguments[1];
  }
  this._dataClient.run(arguments[0], options, arguments[2]);
};


var Global = function (privateClientCluster, publicClientCluster, ioClusterClient) {
  AbstractDataClient.call(this, publicClientCluster);
  
  this._privateClientCluster = privateClientCluster;
  this._publicClientCluster = publicClientCluster;
  this._ioClusterClient = ioClusterClient;
  this._keyManager = new KeyManager();
};

Global.prototype = Object.create(AbstractDataClient.prototype);

Global.prototype._localizeDataKey = function (key) {
  return this._keyManager.getGlobalDataKey(key);
};

Global.prototype.publish = function (channel, data, callback) {
  this._ioClusterClient.publish(channel, data, callback);
};

Global.prototype.subscribe = function (channel, callback) {
  this._ioClusterClient.subscribe(channel, callback);
};

Global.prototype.unsubscribe = function (channel, callback) {
  this._ioClusterClient.unsubscribe(channel, callback);
};

Global.prototype.unsubscribeAll = function (callback) {
  this._ioClusterClient.unsubscribeAll(callback);
};

Global.prototype.watch = function (channel, handler) {
  this._ioClusterClient.watch(channel, handler);
};

Global.prototype.unwatch = function (channel, handler) {
  this._ioClusterClient.unwatch(channel, handler);
};

Global.prototype.watchers = function (channel) {
  return this._ioClusterClient.watchers(channel);
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


var Session = function (sessionId, socketId, dataClient, ioClusterClient) {
  AbstractDataClient.call(this, dataClient);
  
  this.id = sessionId;
  this.socketId = socketId;
  this._ioClusterClient = ioClusterClient;
  this._keyManager = new KeyManager();
};

Session.prototype = Object.create(AbstractDataClient.prototype);

Session.prototype._localizeDataKey = function (key) {
  return this._keyManager.getSessionDataKey(this.id, key);
};

Session.prototype.setAuth = function (data, callback) {
  this.set('__auth', data, callback);
};

Session.prototype.getAuth = function (callback) {
  this.get('__auth', callback);
};

Session.prototype.clearAuth = function (callback) {
  this.remove('__auth', callback);
};

Session.prototype.emit = function (event, data, callback) {
  var sockets = this._ioClusterClient.getSessionSockets(this.id);
  this._ioClusterClient.notifySockets(sockets, {
    event: event,
    data: data
  }, callback);
  EventEmitter.prototype.emit.call(this, event, data);
};

Session.prototype.transmit = function (event, data, callback) {
  var sockets = this._ioClusterClient.getSessionSockets(this.id);
  this._ioClusterClient.notifySockets(sockets, {
    event: event,
    data: data,
    exclude: this.socketId
  }, callback);
  EventEmitter.prototype.emit.call(this, event, data);
};

Session.prototype.kickOut = function (channel, message, callback) {
  var self = this;
  var sockets = this.getSockets();
  
  var tasks = [];
  
  for (var i in sockets) {
    (function (socket) {
      tasks.push(function (cb) {
        socket.kickOut(channel, message, cb);
      });
    })(sockets[i]);
  }
  async.parallel(tasks, callback);
};

Session.prototype.getSockets = function () {
  return this._ioClusterClient.getSessionSockets(this.id);
};

Session.prototype.destroy = function () {
  this.removeAllListeners();
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
  
  this._dataExpiry = options.dataExpiry;
  this._connectTimeout = options.connectTimeout;
  this._addressSocketLimit = options.addressSocketLimit;
  this._socketChannelLimit = options.socketChannelLimit;
  this._heartRate = options.heartRate || 4;
  
  // Expressed in milliseconds.
  // Added 15% safety margin to heartRate in order to shift expiryReset forward
  // so that it doesn't clash with expiry.
  this._expiryReset = Math.floor(this._dataExpiry * 1000 / (this._heartRate * 1.15));
  
  this._expiryBatchSize = 1000;
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
  
  var hasher = function (str) {
    var ch;
    var hash = 0;
    if (str == null || str.length == 0) {
      return hash;
    }
    for (var i = 0; i < str.length; i++) {
      ch = str.charCodeAt(i);
      hash = ((hash << 5) - hash) + ch;
      hash = hash & hash;
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
      if (key[3] == 'addresses' && key[2] == '__meta') {
        return hasher(key[4]);
      }
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
  this._sessions = {};
  this._addresses = {};
  
  this._globalEmitter = new EventEmitter();
  this._globalSubscriptions = {};
  this._globalClient = new Global(this._privateClientCluster, this._publicClientCluster, this);
  
  this._clientSubscribers = {};
  
  var readyNum = 0;
  var firstTime = true;
  
  var dataClientReady = function () {
    if (++readyNum >= dataClients.length && firstTime) {
      firstTime = false;
      self._expiryInterval = setInterval(function () {
        self._extendExpiries();
      }, self._expiryReset);
      
      self._ready = true;
      
      self.emit('ready');
    }
  };
  
  for (var j in dataClients) {
    dataClients[j].on('ready', dataClientReady);
  }
  
  this._privateClientCluster.on('message', this._handleGlobalMessage.bind(this));
  
  process.on('exit', function () {
    for (var i in self._addresses) {
      self._privateClientCluster.remove(self._addresses[i].dataKey, {noAck: 1});
    }
  });
};

IOClusterClient.prototype = Object.create(EventEmitter.prototype);

IOClusterClient.prototype.destroy = function (callback) {
  clearInterval(this._expiryInterval);
  this._privateClientCluster.removeAll(callback);
};

IOClusterClient.prototype.on = function (event, listener) {
  if (event == 'ready' && this._ready) {
    listener();
  } else {
    EventEmitter.prototype.on.apply(this, arguments);
  }
};

IOClusterClient.prototype._processExpiryList = function (expiryList) {
  var self = this;
  var key, i;
  var keys = [];
  for (i=0; i<this._expiryBatchSize; i++) {
    key = expiryList.shift();
    if (key == null) {
      break;
    }
    keys.push(key);
  }
  if (keys.length > 0) {
    this._privateClientCluster.expire(keys, this._dataExpiry, function () {
      self._processExpiryList(expiryList);
    });
  }
};

IOClusterClient.prototype._extendExpiries = function () {
  var dataExpiryList = new LinkedList();
  var addressExpiryList = new LinkedList();
  
  var sessions = this._sessions;
  var addresses = this._addresses;
  
  var i;
  for (i in sessions) {
    dataExpiryList.push(sessions[i].dataKey);
  }
  for (i in addresses) {
    addressExpiryList.push(addresses[i].dataKey);
  }
  
  this._processExpiryList(dataExpiryList);
  this._processExpiryList(addressExpiryList);
};

IOClusterClient.prototype._handshake = function (socket, callback) {
  var self = this;
  
  if (socket.remoteAddress == null || socket.id == null) {
    callback && callback("Failed handshake - Invalid handshake data");
  } else {
    var remoteAddr = socket.remoteAddress;
    
    if (remoteAddr.remoteAddress) {
      remoteAddr = remoteAddr.remoteAddress;
    }
    
    var acceptHandshake = function () {
      var addressStartQuery = function (dataMap, dataExpirer) {
        dataExpirer.expire([dataKey], expiry);
        dataMap.set(addressSocketKey, 1);
      };
      addressStartQuery.mapIndex = remoteAddr;
      addressStartQuery.data = {
        dataKey: self._keyManager.getGlobalDataKey(['__meta', 'addresses', remoteAddr]),
        expiry: self._connectTimeout,
        addressSocketKey: self._keyManager.getGlobalDataKey(['__meta', 'addresses', remoteAddr, 'sockets', socket.id])
      };
      
      self._privateClientCluster.query(addressStartQuery, function (err) {
        callback && callback(err);
      });
    };
    
    if (this._addressSocketLimit > 0) {
      this.getAddressSockets(remoteAddr, function (err, sockets) {
        if (err) {
          callback && callback(err);
        } else {
          if (sockets.length < self._addressSocketLimit) {
            acceptHandshake();
          } else {
            callback && callback("Reached connection limit for the address " + remoteAddr);
          }
        }
      });
    } else {
      acceptHandshake();
    }
  }
};

IOClusterClient.prototype.bind = function (socket, callback) {
  var self = this;
  
  callback = this._errorDomain.bind(callback);
  
  socket.sessionDataKey = this._keyManager.getSessionDataKey(socket.ssid);
  socket.addressDataKey = this._keyManager.getGlobalDataKey(['__meta', 'addresses', socket.remoteAddress]);
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
  
  this._handshake(socket, function (err) {
    if (err) {
      callback(err, socket, true);
    } else {
      self._sockets[socket.id] = socket;
      if (self._sessions[socket.ssid] == null) {
        self._sessions[socket.ssid] = {
          dataKey: socket.sessionDataKey,
          sockets: {}
        };
      }
      self._sessions[socket.ssid].sockets[socket.id] = socket;
      
      if (self._addresses[socket.remoteAddress] == null) {
        self._addresses[socket.remoteAddress] = {
          dataKey: socket.addressDataKey,
          sockets: {}
        };
      }
      self._addresses[socket.remoteAddress].sockets[socket.id] = socket;
      
      var sessionStartQuery = function (dataMap, dataExpirer) {
        dataExpirer.expire([dataKey], expiry);
        dataMap.set(metaDataKey, 1);
      };
      sessionStartQuery.mapIndex = socket.ssid;
      sessionStartQuery.data = {
        dataKey: socket.sessionDataKey,
        expiry: self._dataExpiry,
        metaDataKey: self._keyManager.getSessionDataKey(socket.ssid, ['__meta', 'sockets', socket.id])
      };
      
      socket.on('subscribe', function (channel, res) {
        self.subscribeClientSocket(socket, channel, function (err) {
          if (err) {
            res.error(err);
            self.emit('notice', err);
          } else {
            res.end();
          }
        });
      });
      
      socket.on('unsubscribe', function (channel, res) {
        self.unsubscribeClientSocket(socket, channel, function (err, isNotice) {
          if (err) {
            res.error(err);
            
            if (isNotice) {
              self.emit('notice', err);
            } else {
              self.emit('error', err);
            }
          } else {
            res.end();
          }
        });
      });
      
      async.parallel([
        function () {
          var cb = arguments[arguments.length - 1];
          self._privateClientCluster.query(sessionStartQuery, cb);
        },
        function () {
          var cb = arguments[arguments.length - 1];
          self._privateClientCluster.expire([socket.addressDataKey], self._dataExpiry, cb);
        }
      ],
      function (err) {
        callback(err, socket);
      });
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
      self._privateClientCluster.remove(self._keyManager.getSessionDataKey(socket.ssid, ['__meta', 'sockets', socket.id]));
      if (self._addresses[socket.remoteAddress]) {
        delete self._addresses[socket.remoteAddress].sockets[socket.id];
        self._privateClientCluster.remove(self._keyManager.getGlobalDataKey(['__meta', 'addresses', socket.remoteAddress, 'sockets', socket.id]));
        if (isEmpty(self._addresses[socket.remoteAddress].sockets)) {
          delete self._addresses[socket.remoteAddress];
        }
      }
      if (self._sessions[socket.ssid]) {
        delete self._sessions[socket.ssid].sockets[socket.id];
        if (isEmpty(self._sessions[socket.ssid].sockets)) {
          delete self._sessions[socket.ssid];
          self._privateClientCluster.expire([socket.sessionDataKey], self._dataExpiry, cb);
          self.emit('sessionEnd', socket.ssid);
        } else {
          cb();
        }
      } else {
        cb();
      }
    }
  ],
  function (err) {
    callback(err, socket);
  });
};

IOClusterClient.prototype.getSessionSockets = function (ssid) {
  var session = this._sessions[ssid];
  if (session == null) {
    return {};
  }
  return session.sockets || {};
};

IOClusterClient.prototype.getAddressSockets = function (ipAddress, callback) {
  var self = this;
  
  var addressDataKey = ['__meta', 'addresses', ipAddress, 'sockets'];
  this._privateClientCluster.get(this._keyManager.getGlobalDataKey(addressDataKey), function (err, data) {
    var sockets = [];
    var i;
    for (i in data) {
      sockets.push(i);
    }
    callback(err, sockets);
  });
};

IOClusterClient.prototype.global = function () {
  return this._globalClient;
};

IOClusterClient.prototype.session = function (sessionId, socketId) {
  return new Session(sessionId, socketId, this._privateClientCluster.map(sessionId)[0], this);
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

IOClusterClient.prototype.publish = function (channel, data, callback) {
  this._privateClientCluster.publish(channel, data, callback);
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

IOClusterClient.prototype.watch = function (channel, handler) {
  this._globalEmitter.on(channel, handler);
};

IOClusterClient.prototype.unwatch = function (channel, handler) {
  if (handler) {
    this._globalEmitter.removeListener(channel, handler);
  } else if (channel != null) {
    this._globalEmitter.removeAllListeners(channel);
  } else {
    this._globalEmitter.removeAllListeners();
  }
};

IOClusterClient.prototype.watchers = function (channel) {
  return this._globalEmitter.listeners(channel);
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

IOClusterClient.prototype.notifySockets = function (sockets, data, callback) {
  if (callback) {
    var tasks = {};
    
    var errorMap;
    var dataMap = {};
    for (var i in sockets) {
      (function (socket) {
        if (data.exclude == null || socket.id != data.exclude) {
          tasks[socket.id] = function (cb) {
            socket.emit(data.event, data.data, function (err, data) {
              if (err) {
                if (errorMap == null) {
                  errorMap = {};
                }
                errorMap[socket.id] = err;
              } else {
                dataMap[socket.id] = data;
              }
              cb();
            });
          };
        }
      })(sockets[i]);
    }
    async.parallel(tasks, function () {
      callback(errorMap, dataMap);
    });
  } else {
    for (var i in sockets) {
      var socket = sockets[i];
      if (data.exclude == null || socket.id != data.exclude) {
        socket.emit(data.event, data.data);
      }
    }
  }
};

IOClusterClient.prototype.rawNotifySockets = function (sockets, data) {
  for (var i in sockets) {
    var socket = sockets[i];
    if (data.exclude == null || socket.id != data.exclude) {
      socket.emitRaw(data);
    }
  }
};

IOClusterClient.prototype._handleGlobalMessage = function (channel, message) {
  var data = {
    channel: channel,
    data: message
  };
  this.rawNotifySockets(this._clientSubscribers[channel], data);
  this._globalEmitter.emit(channel, message);
};
