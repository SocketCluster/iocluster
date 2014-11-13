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

Global.prototype.publish = function (event, data, callback) {
  this._ioClusterClient.publishGlobalEvent(event, data, callback);
};

Global.prototype.on = function (event, handler, callback) {
  this._ioClusterClient.onGlobalEvent(event, handler, callback);
};

Global.prototype.removeListener = function (event, handler, callback) {
  this._ioClusterClient.removeGlobalListener(event, handler, callback);
};

Global.prototype.removeAllListeners = function (event, callback) {
  this._ioClusterClient.removeAllGlobalListeners(event, callback);
};

Global.prototype.listeners = function (event) {
  return this._ioClusterClient.getGlobalListeners(event);
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

Global.prototype.destroy = function (callback) {
  this.removeAllListeners(null, callback);
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

Session.prototype.emit = function (event, data) {
  var sockets = this._ioClusterClient.getSessionSockets(this.id);
  this._ioClusterClient.notifySockets(sockets, {
    event: event,
    data: data
  });
  EventEmitter.prototype.emit.call(this, event, data);
};

Session.prototype.transmit = function (event, data) {
  var sockets = this._ioClusterClient.getSessionSockets(this.id);
  this._ioClusterClient.notifySockets(sockets, {
    event: event,
    data: data,
    exclude: this.socketId
  });
  EventEmitter.prototype.emit.call(this, event, data);
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
        socketPath: socketPath,
        secretKey: options.dataKey,
        expiryAccuracy: options.expiryAccuracy,
        downgradeToUser: options.downgradeToUser,
        storeControllerPath: options.appStoreControllerPath
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
  this._socketEventLimit = options.socketEventLimit;
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
      secretKey: options.dataKey
    });
    dataClients.push(dataClient);
  }
  
  var hasher = function (str) {
    var ch;
    var hash = 0;
    // Solves TypeError: Cannot read property 'length' of null
    if ((str === null) || (str === undefined)) return hash;
    if (str.length == 0) return hash;
    for (var i = 0; i < str.length; i++) {
      ch = str.charCodeAt(i);
      hash = ((hash << 5) - hash) + ch;
      hash = hash & hash;
    }
    return Math.abs(hash) % dataClients.length;
  };
  
  var eventMethods = {
    publish: true,
    subscribe: true,
    unsubscribe: true,
    isSubscribed: true
  };
  
  this._privateMapper = function (key, method, clientIds) {
    if (eventMethods[method]) {
      if (key == null) {
        return clientIds;
      }
      return hasher(key);
    }
    
    if (method == 'query' || method == 'run') {
      return hasher(key.mapIndex || 0);
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
  this._globalSubscribers = {};
  
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
  
  if (socket.address == null || socket.id == null) {
    callback && callback("Failed handshake - Invalid handshake data");
  } else {
    var remoteAddr = socket.address;
    
    if (remoteAddr.address) {
      remoteAddr = remoteAddr.address;
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
  socket.addressDataKey = this._keyManager.getGlobalDataKey(['__meta', 'addresses', socket.address]);
  socket.eventSubscriptions = {};
  socket.eventSubscriptionCount = 0;
  
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
      
      if (self._addresses[socket.address] == null) {
        self._addresses[socket.address] = {
          dataKey: socket.addressDataKey,
          sockets: {}
        };
      }
      self._addresses[socket.address].sockets[socket.id] = socket;
      
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
      
      socket.on('subscribe', function (event, res) {
        self._subscribeClientSocket(socket, event, function (err) {
          if (err) {
            res.error(err);
            self.emit('notice', err);
          } else {
            res.end();
          }
        });
      });
      
      socket.on('unsubscribe', function (event, res) {
        self._unsubscribeClientSocket(socket, event, function (err, isNotice) {
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
      self._unsubscribeClientSocket(socket, null, cb);
    },
    function () {
      var cb = arguments[arguments.length - 1];
      delete self._sockets[socket.id];
      self._privateClientCluster.remove(self._keyManager.getSessionDataKey(socket.ssid, ['__meta', 'sockets', socket.id]));
      if (self._addresses[socket.address]) {
        delete self._addresses[socket.address].sockets[socket.id];
        self._privateClientCluster.remove(self._keyManager.getGlobalDataKey(['__meta', 'addresses', socket.address, 'sockets', socket.id]));
        if (isEmpty(self._addresses[socket.address].sockets)) {
          delete self._addresses[socket.address];
        }
      }
      if (self._sessions[socket.ssid]) {
        delete self._sessions[socket.ssid].sockets[socket.id];
        if (isEmpty(self._sessions[socket.ssid].sockets)) {
          delete self._sessions[socket.ssid];
          self._privateClientCluster.expire([socket.sessionDataKey], self._dataExpiry, cb);
          self.emit('sessionend', socket.ssid);
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
  return new Global(this._privateClientCluster, this._publicClientCluster, this);
};

IOClusterClient.prototype.session = function (sessionId, socketId) {
  return new Session(sessionId, socketId, this._privateClientCluster.map(sessionId)[0], this);
};

IOClusterClient.prototype._dropUnusedSubscriptions = function (globalEvent, callback) {
  var self = this;
  
  if (isEmpty(this._globalSubscribers[globalEvent])) {
    delete this._globalSubscribers[globalEvent];
    if (EventEmitter.listenerCount(this._globalEmitter, globalEvent) < 1) {
      self._privateClientCluster.unsubscribe(globalEvent, callback);
      return;
    }
  }
  callback && callback();
};

IOClusterClient.prototype.publishGlobalEvent = function (event, data, callback) {
  this._privateClientCluster.publish(event, data, callback);
};

IOClusterClient.prototype.onGlobalEvent = function (event, handler, callback) {
  this._globalEmitter.on(event, handler);
  this._privateClientCluster.subscribe(event, callback);
};

IOClusterClient.prototype.removeGlobalListener = function (event, handler, callback) {
  this._globalEmitter.removeListener(event, handler);
  this._dropUnusedSubscriptions(event, callback);
};

IOClusterClient.prototype.removeAllGlobalListeners = function (event, callback) {
  var self = this;
  
  if (event instanceof Function) {
    callback = event;
    event = null;
  }
  if (event) {
    this._globalEmitter.removeAllListeners(event);
    this._dropUnusedSubscriptions(event, callback);
  } else {
    this._globalEmitter.removeAllListeners();
    var tasks = [];
    for (var event in this._globalSubscribers) {
      (function (event) {
        tasks.push(function (cb) {
          self._dropUnusedSubscriptions(event, cb);
        });
      })(event);
    }
    async.parallel(tasks, function (err) {
      callback && callback(err);
    });
  }
};

IOClusterClient.prototype.getGlobalListeners = function (event) {
  return this._globalEmitter.listeners(event);
};

IOClusterClient.prototype._subscribeClientSocket = function (socket, events, callback) {
  var self = this;
  
  if (events instanceof Array) {
    var tasks = [];
    for (var i in events) {
      (function (event) {
        tasks.push(function (cb) {
          self._subscribeSingleClientSocket(socket, event, cb);
        });
      })(events[i]);
    }
    async.waterfall(tasks, function (err) {
      callback && callback(err);
    });
  } else {
    this._subscribeSingleClientSocket(socket, events, callback);
  }
};

IOClusterClient.prototype._unsubscribeClientSocket = function (socket, events, callback) {
  var self = this;
  
  if (events == null) {
    events = [];
    for (var event in socket.eventSubscriptions) {
      events.push(event);
    }
  }
  if (events instanceof Array) {
    var tasks = [];
    for (var i in events) {
      (function (event) {
        tasks.push(function (cb) {
          self._unsubscribeSingleClientSocket(socket, event, cb);
        });
      })(events[i]);
    }
    async.waterfall(tasks, function (err) {
      callback && callback(err);
    });
  } else {
    this._unsubscribeSingleClientSocket(socket, events, callback);
  }
};

IOClusterClient.prototype._subscribeSingleClientSocket = function (socket, event, callback) {
  var self = this;
  
  if (this._socketEventLimit && socket.eventSubscriptionCount >= this._socketEventLimit) {
    callback('Socket ' + socket.id + ' tried to exceed the event subscription limit of ' +
      this._socketEventLimit);
  } else {
    if (socket.eventSubscriptionCount == null) {
      socket.eventSubscriptionCount = 0;
    }
    
    if (socket.eventSubscriptions == null) {
      socket.eventSubscriptions = {};
    }
    if (socket.eventSubscriptions[event] == null) {
      socket.eventSubscriptions[event] = true;
      socket.eventSubscriptionCount++;
    }
    
    var addSubscription = function (err) {
      if (err) {
        delete socket.eventSubscriptions[event];
        socket.eventSubscriptionCount--;
      } else {
        if (!self._globalSubscribers[event]) {
          self._globalSubscribers[event] = {};
        }
        self._globalSubscribers[event][socket.id] = socket;
      }
      callback && callback(err);
    };
    
    this._privateClientCluster.subscribe(event, addSubscription);
  }
};

IOClusterClient.prototype._unsubscribeSingleClientSocket = function (socket, event, callback) {
  if (this._globalSubscribers[event]) {
    delete this._globalSubscribers[event][socket.id];
    if (isEmpty(this._globalSubscribers[event])) {
      delete this._globalSubscribers[event];
    }
  }
  delete socket.eventSubscriptions[event];
  if (socket.eventSubscriptionCount != null) {
    socket.eventSubscriptionCount--;
  }
  this._dropUnusedSubscriptions(event, callback);
};

IOClusterClient.prototype.notifySockets = function (sockets, data) {
  for (var i in sockets) {
    var socket = sockets[i];
    if (data.exclude == null || socket.id != data.exclude) {
      socket.emit(data.event, data.data);
    }
  }
};

IOClusterClient.prototype._handleGlobalMessage = function (channel, message) {
  var data = {
    event: channel,
    data: message
  };
  this.notifySockets(this._globalSubscribers[channel], data);
  this._globalEmitter.emit(channel, message);
};
