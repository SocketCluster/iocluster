var EventEmitter = require('events').EventEmitter;
var ndata = require('ndata');
var async = require('async');
var LinkedList = require('linkedlist');
var ClientCluster = require('./clientcluster').ClientCluster;
var domain = require('domain');

var KeyManager = function () {};

KeyManager.prototype.getGlobalDataKey = function (keys) {
  var keyChain = ['__iocl', 'gld'];
  if (keys != null) {
    keyChain = keyChain.concat(keys);
  }
  return keyChain;
};

KeyManager.prototype.getSessionDataKey = function (sessionId, keys) {
  var keyChain = ['__iocl', 'sed', sessionId];
  if (keys != null) {
    keyChain = keyChain.concat(keys);
  }
  return keyChain;
};

KeyManager.prototype.getSocketDataKey = function (socketId, keys) {
  var keyChain = ['__iocl', 'sod', socketId];
  if (keys != null) {
    keyChain = keyChain.concat(keys);
  }
  return keyChain;
};

KeyManager.prototype.getGlobalEventKey = function (key) {
  if (key == null) {
    return ['__iocl', 'gle'];
  } else {
    return ['__iocl', 'gle', key];
  }
};

KeyManager.prototype.getSessionEventKey = function (sessionId, key) {
  if (key == null) {
    return ['__iocl', 'see', sessionId];
  } else {
    return ['__iocl', 'see', sessionId, key];
  }
};

KeyManager.prototype.getSocketEventKey = function (socketId, key) {
  if (key == null) {
    return ['__iocl', 'soe', socketId];
  } else {
    return ['__iocl', 'soe', socketId, key];
  }
};

var isEmpty = function (obj) {
  var i;
  for (i in obj) {
    return false;
  }
  return true;
};

var AbstractDataClient = function (dataClient) {
  this._dataClient = dataClient;
};

AbstractDataClient.prototype = Object.create(EventEmitter.prototype);

AbstractDataClient.prototype.set = function() {
  arguments[0] = this._localizeDataKey(arguments[0]);
  this._dataClient.set.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.expire = function() {
  var keys = arguments[0];
  for (var i in keys) {
    keys[i] = this._localizeDataKey(keys[i]);
  }
  arguments[0] = keys;
  
  this._dataClient.expire.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.unexpire = function() {
  var keys = arguments[0];
  for (var i in keys) {
    keys[i] = this._localizeDataKey(keys[i]);
  }
  arguments[0] = keys;
  
  this._dataClient.unexpire.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.add = function() {
  arguments[0] = this._localizeDataKey(arguments[0]);
  this._dataClient.add.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.get = function() {
  arguments[0] = this._localizeDataKey(arguments[0]);
  this._dataClient.get.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.getRange = function() {
  arguments[0] = this._localizeDataKey(arguments[0]);
  this._dataClient.getRange.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.getAll = function(callback) {
  var clientRootKey = this._localizeDataKey();
  this._dataClient.get.call(this._dataClient, clientRootKey, callback);
};

AbstractDataClient.prototype.count = function() {
  arguments[0] = this._localizeDataKey(arguments[0]);
  this._dataClient.count.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.remove = function() {
  arguments[0] = this._localizeDataKey(arguments[0]);
  this._dataClient.remove.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.removeRange = function() {
  arguments[0] = this._localizeDataKey(arguments[0]);
  this._dataClient.removeRange.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.removeAll = function(callback) {
  var clientRootKey = this._localizeDataKey();
  this._dataClient.set.call(this._dataClient, clientRootKey, {}, callback);
};

AbstractDataClient.prototype.pop = function() {
  arguments[0] = this._localizeDataKey(arguments[0]);
  this._dataClient.pop.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.hasKey = function() {
  arguments[0] = this._localizeDataKey(arguments[0]);
  this._dataClient.hasKey.apply(this._dataClient, arguments);
};

AbstractDataClient.prototype.stringify = function(value) {
  return this._dataClient.stringify(value);
};

AbstractDataClient.prototype.extractKeys = function(object) {
  return this._dataClient.extractKeys(object);
};

AbstractDataClient.prototype.extractValues = function(object) {
  return this._dataClient.extractValues(object);
};

/*
  query(query,[ data, callback])
*/
AbstractDataClient.prototype.query = function() {
  var options = {
    baseKey: this._localizeDataKey()
  };
  
  if (arguments[1] && !(arguments[1] instanceof Function)) {
    options.data = arguments[1];
  }
  this._dataClient.run(arguments[0], options, arguments[2]);
};


var Global = function (socketId, privClientCluster, pubClientCluster, eventEmitter) {
  this.socketId = socketId;
  this._privClientCluster = privClientCluster;
  this._pubClientCluster = pubClientCluster;
  this._eventEmitter = eventEmitter;
  this._keyManager = new KeyManager();
  
  AbstractDataClient.call(this, this._pubClientCluster);
};

Global.prototype = Object.create(AbstractDataClient.prototype);

Global.prototype._localizeDataKey = function (key) {
  return this._keyManager.getGlobalDataKey(key);
};

Global.prototype.broadcast = function (event, data, callback) {
  this._privClientCluster.broadcast(this._keyManager.getGlobalEventKey(event), data, callback);
};

Global.prototype._emit = function (sessionId, event, data, excludeCurrentSocket, callback) {
  var eventData = {session: sessionId, event: event, data: data};
  if (excludeCurrentSocket) {
    eventData.exclude = this.socketId;
  }
  this._privClientCluster.broadcast(this._keyManager.getSessionEventKey(sessionId), eventData, callback);
};

Global.prototype.emit = function (sessionId, event, data, callback) {
  this._emit(sessionId, event, data, false, callback);
};

Global.prototype.transmit = function (sessionId, event, data, callback) {
  this._emit(sessionId, event, data, true, callback);
};

Global.prototype.watch = function (event, handler) {
  this._eventEmitter.on(event, handler);
};

Global.prototype.unwatch = function (event, handler) {
  if (handler) {
    this._eventEmitter.removeListener(event, handler);
  } else {
    this._eventEmitter.removeAllListeners.apply(this._eventEmitter, arguments);
  }
};

Global.prototype.setMapper = function (mapper) {
  this._pubClientCluster.setMapper(mapper);
};

Global.prototype.getMapper = function () {
  return this._pubClientCluster.getMapper();
};

Global.prototype.map = function () {
  return this._pubClientCluster.map.apply(this._pubClientCluster, arguments);
};


var Session = function (sessionId, socketId, dataClient, eventEmitter) {
  this.id = sessionId;
  this.socketId = socketId;
  this._dataClient = dataClient;
  this._eventEmitter = eventEmitter;
  this._keyManager = new KeyManager();
};

Session.prototype = Object.create(AbstractDataClient.prototype);

Session.prototype._localizeDataKey = function (key) {
  return this._keyManager.getSessionDataKey(this.id, key);
};

Session.prototype.setAuth = function(data, callback) {
  this.set('__auth', data, callback);
};

Session.prototype.getAuth = function(callback) {
  this.get('__auth', callback);
};

Session.prototype.clearAuth = function(callback) {
  this.remove('__auth', callback);
};

Session.prototype.emit = function (event, data, callback) {
  this._dataClient.broadcast(this._keyManager.getSessionEventKey(this.id), {session: this.id, event: event, data: data}, callback);
};

Session.prototype.transmit = function (event, data, callback) {
  this._dataClient.broadcast(this._keyManager.getSessionEventKey(this.id), {session: this.id, event: event, data: data, exclude: this.socketId}, callback);
};

Session.prototype.countSockets = function (callback) {
  this._dataClient.count(this._keyManager.getSessionDataKey(this.id, ['__meta', 'sockets']), callback);
};

Session.prototype.watch = function (event, handler) {
  this._eventEmitter.on(event, handler);
};

Session.prototype.unwatch = function (event, handler) {
  if (handler) {
    this._eventEmitter.removeListener(event, handler);
  } else {
    this._eventEmitter.removeAllListeners.apply(this._eventEmitter, arguments);
  }
};


/*
  LocalSession is a session which resides in the current process. Its interface is identical to Session
  but it has some performance optimizations.
*/
var LocalSession = function (sessionId, socketId, dataClient, eventEmitter, ioClusterClient) {
  Session.call(this, sessionId, socketId, dataClient, eventEmitter);
  this._ioClusterClient = ioClusterClient;
};

LocalSession.prototype = Object.create(Session.prototype);

LocalSession.prototype.emit = function (event, data, callback) {
   this._ioClusterClient._handleSessionEvent({
    event: event,
    session: this.id,
    data: data
   });
};

LocalSession.prototype.transmit = function (event, data, callback) {
  this._ioClusterClient._handleSessionEvent({
    event: event,
    session: this.id,
    data: data,
    exclude: this.socketId
  });
};


var Socket = function (socketId, dataClient, eventEmitter) {
  this.id = socketId;
  this._dataClient = dataClient;
  this._eventEmitter = eventEmitter;
  this._keyManager = new KeyManager();
};

Socket.prototype = Object.create(AbstractDataClient.prototype);

Socket.prototype._localizeDataKey = function (key) {
  return this._keyManager.getSocketDataKey(this.id, key);
};

Socket.prototype.emit = function (event, data, callback) {
  this._dataClient.broadcast(this._keyManager.getSocketEventKey(this.id), {socket: this.id, event: event, data: data}, callback);
};

Socket.prototype.watch = function (event, handler) {
  this._eventEmitter.on(event, handler);
};

Socket.prototype.unwatch = function (event, handler) {
  if (handler) {
    this._eventEmitter.removeListener(event, handler);
  } else {
    this._eventEmitter.removeAllListeners.apply(this._eventEmitter, arguments);
  }
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
      var curStore = options.stores[i];
      if (typeof curStore == 'number') {
        curStore = {port: curStore};
      }
      dataServer = ndata.createServer(curStore.port, options.dataKey, options.expiryAccuracy);
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
        self.emit('error', new Error('nData server at port ' + curStore.port + ' exited'));
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
  this._heartRate = options.heartRate || 4;
  
  // Expressed in milliseconds.
  // Added 15% safety margin to heartRate in order to shift expiryReset forward
  // so that it doesn't clash with expiry.
  this._expiryReset = Math.floor(this._dataExpiry * 1000 / (this._heartRate * 1.15));
  
  this._expiryBatchSize = 1000;
  this._keyManager = new KeyManager();
  this._ready = false;
  
  var dataClient, curStore;
  var dataClients = [];
  
  for (var i in options.stores) {
    curStore = options.stores[i];
    if (typeof curStore == 'number') {
      curStore = {port: curStore};
    }
    dataClient = ndata.createClient(curStore.port, options.dataKey);
    dataClients.push(dataClient);
  }
  
  var hasher = function (string) {
    if (string == null) {
      string = '';
    }
    var result = 0;
    
    var len = string.length;
    for (var i=0; i<len; i++) {
      result += string.charCodeAt(i);
    }
    
    return result % dataClients.length;
  };
  
  var eventMethods = {
    broadcast: true,
    watch: true,
    watchOnce: true,
    unwatch: true,
    isWatching: true
  };
  
  this._privateMapper = function (key, method, clientIds) {
    if (eventMethods[method]) {
      console.log('map', method, key[2], hasher(key[2]));
      return hasher(key[2]);
    }
    if (method == 'query' || method == 'run') {
      return hasher(key.mapIndex || 0);
    }
    if (key instanceof Array) {
      if (key[3] == 'addresses') {
        return hasher(key[4]);
      }
      return hasher(key[2]);
    }
    return hasher(key);
  };
  
  this._publicMapper = function (key, method, clientIds) {
    return 0;
  };
  
  this._privClientCluster = new ClientCluster(dataClients);
  this._privClientCluster.setMapper(this._privateMapper);
  this._errorDomain.add(this._privClientCluster);
  
  this._pubClientCluster = new ClientCluster(dataClients);
  this._pubClientCluster.setMapper(this._publicMapper);
  this._errorDomain.add(this._pubClientCluster);
  
  var readyNum = 0;
  var dataClientReady = function () {
    if (++readyNum >= dataClients.length) {
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
  
  this._sockets = {};
  this._sessions = {};
  this._addresses = {};
  
  this._eventQueues = {};
  
  this._socketEmitter = new EventEmitter();
  this._sessionEmitter = new EventEmitter();
  this._globalEmitter = new EventEmitter();
  
  process.on('exit', function () {
    for (var i in self._addresses) {
      self._privClientCluster.remove(self._addresses[i].dataKey, {noAck: 1});
    }
  });
};

IOClusterClient.prototype = Object.create(EventEmitter.prototype);

IOClusterClient.prototype.destroy = function (callback) {
  clearInterval(this._expiryInterval);
  this._privClientCluster.removeAll(callback);
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
    this._privClientCluster.expire(keys, this._dataExpiry, function() {
      self._processExpiryList(expiryList);
    });
  }
};

IOClusterClient.prototype._extendExpiries = function () {
  var sessionExpiryList = new LinkedList();
  var addressExpiryList = new LinkedList();
  
  var sockets = this._sockets;
  var sessions = this._sessions;
  var addresses = this._addresses;
  
  var i;
  for (i in sockets) {
    sessionExpiryList.push(sockets[i].dataKey);
  }
  for (i in sessions) {
    sessionExpiryList.push(sessions[i].dataKey);
  }
  for (i in addresses) {
    addressExpiryList.push(addresses[i].dataKey);
  }
  
  this._processExpiryList(sessionExpiryList);
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
      
      self._privClientCluster.query(addressStartQuery, function (err) {
        callback && callback(err);
      });
    };
    
    if (this._addressSocketLimit > 0) {
      this.getAddressSockets(remoteAddr, function(err, sockets) {
        if (err) {
          callback && callback(err);
        } else {
          if (sockets.length < self._addressSocketLimit) {
            acceptHandshake();
          } else {
            callback && callback("Reached connection limit for the address " + remoteAddr, true);
          }
        }
      });
    } else {
      acceptHandshake();
    }
  }
};

IOClusterClient.prototype._subscribe = function (socket, event, callback) {
  var self = this;
  
  var addSubscription = function (err) {
    self._eventQueues[event][socket.id] = socket;
    if (socket.subscriptions == null) {
      socket.subscriptions = {};
    }
    socket.subscriptions[event] = true;
    
    callback(err);
  };
  
  if (this._eventQueues[event] == null) {
    this._eventQueues[event] = {};
    var eventKey = this._keyManager.getGlobalEventKey(event);
    this._privClientCluster.watch(eventKey, this._handleGlobalEvent.bind(this, event), addSubscription);
  } else {
    addSubscription();
  }
};

IOClusterClient.prototype._unsubscribeFromEvent = function (socket, event, callback) {
  var self = this;
  
  if (this._eventQueues[event] && this._eventQueues[event][socket.id]) {
    delete this._eventQueues[event][socket.id];
    if (socket.subscriptions) {
      delete socket.subscriptions[event];
    }
    if (isEmpty(this._eventQueues[event])) {
      var eventKey = this._keyManager.getGlobalEventKey(event);
      this._privClientCluster.unwatch(eventKey, null, function (err) {
        delete self._eventQueues[event];
        callback(err);
      });
    } else {
      callback();
    }
  }
};

IOClusterClient.prototype._unsubscribe = function (socket, events, callback) {
  var self = this;
  
  if (events instanceof Array) {
    var tasks = [];
    for (var i in events) {
      (function (event) {
        tasks.push(function (cb) {
          self._unsubscribeFromEvent(socket, event, cb);
        });
      })(events[i]);
    }
    async.waterfall(tasks, callback);
  } else {
    this._unsubscribeFromEvent(socket, events, callback);
  }
};

IOClusterClient.prototype.bind = function (socket, callback) {
  var self = this;
  
  callback = this._errorDomain.bind(callback);
  
  socket.eventKey = this._keyManager.getSocketEventKey(socket.id);
  socket.dataKey = this._keyManager.getSocketDataKey(socket.id);
  socket.sessionEventKey = this._keyManager.getSessionEventKey(socket.ssid);
  socket.sessionDataKey = this._keyManager.getSessionDataKey(socket.ssid);
  socket.addressDataKey = this._keyManager.getGlobalDataKey(['__meta', 'addresses', socket.address]);
  
  this._handshake(socket, function (err, notice) {
    if (err) {
      callback(err, socket, notice);
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
        self._subscribe(socket, event, function (err) {
          if (err) {
            res.error(err);
            self.emit('error', err);
          } else {
            res.end();
          }
        });
      });
      
      socket.on('unsubscribe', function (event, res) {
        self._unsubscribe(socket, event, function (err) {
          if (err) {
            res.error(err);
            self.emit('error', err);
          } else {
            res.end();
          }
        });
      });
      
      async.parallel([
        function () {
          var cb = arguments[arguments.length - 1];
          self._privClientCluster.watchOnce(socket.eventKey, function () { self._handleSocketEvent.apply(self, arguments); }, cb);
        },
        function () {
          var cb = arguments[arguments.length - 1];
          self._privClientCluster.watchOnce(socket.sessionEventKey, function () { self._handleSessionEvent.apply(self, arguments); }, cb);
        },
        function () {
          var cb = arguments[arguments.length - 1];
          self._privClientCluster.query(sessionStartQuery, cb);
        },
        function () {
          var cb = arguments[arguments.length - 1];
          self._privClientCluster.expire([socket.dataKey], self._dataExpiry, cb);
        },
        function () {
          var cb = arguments[arguments.length - 1];
          self._privClientCluster.expire([socket.addressDataKey], self._dataExpiry, cb);
        }
      ],
      function(err) {
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
      self._privClientCluster.unwatch(socket.eventKey, null, cb);
    },
    function () {
      var cb = arguments[arguments.length - 1];
      delete self._sockets[socket.id];
      self._privClientCluster.remove(self._keyManager.getSessionDataKey(socket.ssid, ['__meta', 'sockets', socket.id]));
      if (self._addresses[socket.address]) {
        delete self._addresses[socket.address].sockets[socket.id];
        self._privClientCluster.remove(self._keyManager.getGlobalDataKey(['__meta', 'addresses', socket.address, 'sockets', socket.id]));
        if (isEmpty(self._addresses[socket.address].sockets)) {
          delete self._addresses[socket.address];
        }
      }
      if (self._sessions[socket.ssid]) {
        delete self._sessions[socket.ssid].sockets[socket.id];
      }
      
      if (self._sessions[socket.ssid] && isEmpty(self._sessions[socket.ssid].sockets)) {
        delete self._sessions[socket.ssid];        
        self.emit('sessiondestroy', socket.ssid);
        self._privClientCluster.unwatch(socket.sessionEventKey, null, cb);
      } else {
        cb();
      }
      
      var subs = socket.subscriptions;
      for (var i in subs) {
        self._unsubscribe(socket, i);
      }
    }
  ], callback);
};

IOClusterClient.prototype.getLocalSessionSockets = function (ssid) {
  var session = this._sessions[ssid];
  if (session == null) {
    return {};
  }
  return session.sockets || {};
};

IOClusterClient.prototype.getAddressSockets = function (ipAddress, callback) {
  var self = this;
  
  var addressDataKey = ['__meta', 'addresses', ipAddress, 'sockets'];
  this._privClientCluster.get(this._keyManager.getGlobalDataKey(addressDataKey), function (err, data) {
    var sockets = [];
    var i;
    for (i in data) {
      sockets.push(i);
    }
    
    callback(err, sockets);
  });
};

IOClusterClient.prototype.global = function (socketId) {
  return new Global(socketId, this._privClientCluster, this._pubClientCluster, this._globalEmitter);
};

IOClusterClient.prototype.session = function (sessionId, socketId, isLocal) {
  if (isLocal) {
    return new LocalSession(sessionId, socketId, this._privClientCluster.map(sessionId)[0], this._sessionEmitter, this);
  } else {
    return new Session(sessionId, socketId, this._privClientCluster.map(sessionId)[0], this._sessionEmitter);
  }
};

IOClusterClient.prototype.socket = function (socketId, sessionId) {
  if (!sessionId) {
    sessionId = this._sockets[socketId].ssid;
  }
  
  return new Socket(socketId, this._privClientCluster.map(sessionId)[0], this._socketEmitter);
};

IOClusterClient.prototype._handleSocketEvent = function (e) {
  this._socketEmitter.emit(e.event, e.data);
  
  if (this._sockets[e.socket] != null) {
    var socket = this._sockets[e.socket];
    if (this._eventQueues[e.event] && this._eventQueues[e.event][socket.id]) {
      socket.emit(e.event, e.data);
    }
  }
};

IOClusterClient.prototype._handleSessionEvent = function (e) {
  this._sessionEmitter.emit(e.event, e.data);
  
  if (this._sessions[e.session] != null) {
    var sockets = this._sessions[e.session].sockets;
    var socket;
    
    for (var i in sockets) {
      socket = sockets[i];
      if (socket.id != e.exclude && this._eventQueues[e.event] &&
        this._eventQueues[e.event][socket.id]) {
        
        socket.emit(e.event, e.data);
      }
    }
  }
};

IOClusterClient.prototype._handleGlobalEvent = function (event, data) {
  this._globalEmitter.emit(event, data);
  
  var subscribers = this._eventQueues[event];
  var socket;
  
  for (var i in subscribers) {
    socket = subscribers[i];
    socket.emit(event, data);
  }
};