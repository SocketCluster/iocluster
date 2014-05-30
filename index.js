var EventEmitter = require('events').EventEmitter;
var ndata = require('ndata');
var async = require('async');
var LinkedList = require('linkedlist');
var ClientCluster = require('./clientcluster').ClientCluster;
var PubSubManager = require('./pubsubmanager').PubSubManager;
var KeyManager = require('./keymanager').KeyManager;
var domain = require('domain');
var utils = require('./utils');
var isEmpty = utils.isEmpty;


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

var Global = function (socketId, privateClientCluster, publicClientCluster, pubSubManager) {
  this.socketId = socketId;
  this._privateClientCluster = privateClientCluster;
  this._publicClientCluster = publicClientCluster;
  this._pubSubManager = pubSubManager;
  this._keyManager = new KeyManager();
  
  AbstractDataClient.call(this, this._publicClientCluster);  
};

Global.prototype = Object.create(AbstractDataClient.prototype);

Global.prototype._localizeDataKey = function (key) {
  return this._keyManager.getGlobalDataKey(key);
};

Global.prototype.broadcast = function (event, data, callback) {
  this._pubSubManager.publishGlobalEvent(event, data, callback);
};

Global.prototype._emit = function (sessionId, event, data, excludeCurrentSocket, callback) {
  var excludeSocket = null;
  if (excludeCurrentSocket) {
    excludeSocket = this.socketId;
  }
  this._pubSubManager.publishSessionEvent(sessionId, event, data, excludeSocket, callback);
};

Global.prototype.emit = function (sessionId, event, data, callback) {
  this._emit(sessionId, event, data, false, callback);
};

Global.prototype.transmit = function (sessionId, event, data, callback) {
  this._emit(sessionId, event, data, true, callback);
};

Global.prototype.on = function (event, handler, callback) {
  this._pubSubManager.onGlobalEvent(event, handler, callback);
};

Global.prototype.removeListener = function (event, handler, callback) {
  this._pubSubManager.removeGlobalServerListener(event, handler, callback);
};

Global.prototype.removeAllListeners = function (event, callback) {
  this._pubSubManager.removeAllGlobalServerListeners(event, callback);
};

/*
  off([event, listener, callback])
*/
Global.prototype.off = function () {
  if (arguments.length > 1) {
    this.removeListener.apply(this, arguments);
  } else {
    this.removeAllListeners.apply(this, arguments);
  }
};

Global.prototype.listeners = function (event) {
  return this._pubSubManager.globalServerListeners(event);
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


var Session = function (sessionId, socketId, dataClient, pubSubManager) {
  this.id = sessionId;
  this.socketId = socketId;
  this._dataClient = dataClient;
  this._pubSubManager = pubSubManager;
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
  this._pubSubManager.publishSessionEvent(this.id, event, data, null, callback);
};

Session.prototype.transmit = function (event, data, callback) {
  this._pubSubManager.publishSessionEvent(this.id, event, data, this.socketId, callback);
};

Session.prototype.countSockets = function (callback) {
  this._dataClient.count(this._keyManager.getSessionDataKey(this.id, ['__meta', 'sockets']), callback);
};

Session.prototype.on = function (event, handler, callback) {
  this._pubSubManager.onSessionEvent(this.id, event, handler, callback);
};

Session.prototype.removeListener = function (event, handler, callback) {
  this._pubSubManager.removeSessionServerListener(this.id, event, handler, callback);
};

Session.prototype.removeAllListeners = function (event, callback) {
  this._pubSubManager.removeAllSessionServerListeners(this.id, event, callback);
};

/*
  off([event, listener, callback])
*/
Session.prototype.off = function () {
  if (arguments.length > 1) {
    this.removeListener.apply(this, arguments);
  } else {
    this.removeAllListeners.apply(this, arguments);
  }
};

Session.prototype.listeners = function (event) {
  return this._pubSubManager.sessionServerListeners(this.id, event);
};

/*
  LocalSession is a session which resides in the current process. Its interface is identical to Session
  but it has some performance optimizations.
*/
var LocalSession = function (sessionId, socketId, dataClient, pubSubManager, ioClusterClient) {
  Session.call(this, sessionId, socketId, dataClient, pubSubManager);
  this._ioClusterClient = ioClusterClient;
};

LocalSession.prototype = Object.create(Session.prototype);

LocalSession.prototype.emit = function (event, data, callback) {
   this._ioClusterClient._handleSessionMessage({
    event: event,
    session: this.id,
    data: data
   });
};

LocalSession.prototype.transmit = function (event, data, callback) {
  this._ioClusterClient._handleSessionMessage({
    event: event,
    session: this.id,
    data: data,
    exclude: this.socketId
  });
};


var Socket = function (socketId, dataClient, pubSubManager) {
  this.id = socketId;
  this._dataClient = dataClient;
  this._pubSubManager = pubSubManager;
  this._keyManager = new KeyManager();
};

Socket.prototype = Object.create(AbstractDataClient.prototype);

Socket.prototype._localizeDataKey = function (key) {
  return this._keyManager.getSocketDataKey(this.id, key);
};

Socket.prototype.emit = function (event, data, callback) {
  this._pubSubManager.publishSocketEvent(this.id, event, data, callback);
};

Socket.prototype.on = function (event, handler, callback) {
  this._pubSubManager.onSocketEvent(this.id, event, handler, callback);
};

Socket.prototype.removeListener = function (event, handler, callback) {
  this._pubSubManager.removeSocketServerListener(this.id, event, handler, callback);
};

Socket.prototype.removeAllListeners = function (event, callback) {
  this._pubSubManager.removeAllSocketServerListeners(this.id, event, callback);
};

/*
  off([event, listener, callback])
*/
Socket.prototype.off = function () {
  if (arguments.length > 1) {
    this.removeListener.apply(this, arguments);
  } else {
    this.removeAllListeners.apply(this, arguments);
  }
};

Socket.prototype.listeners = function (event) {
  return this._pubSubManager.socketServerListeners(this.id, event);
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
  this._socketEventLimit = options.socketEventLimit;
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
    publish: true,
    subscribe: true,
    unsubscribe: true,
    isSubscribed: true
  };
  
  this._privateMapper = function (key, method, clientIds) {
    if (eventMethods[method]) {
      if (key[2] == null) {
        return clientIds;
      }
      return hasher(key[2]);
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
  
  this._privateClientCluster.on('message', this._handleMessage.bind(this));
  
  this._publicClientCluster = new ClientCluster(dataClients);
  this._publicClientCluster.setMapper(this._publicMapper);
  this._errorDomain.add(this._publicClientCluster);
  
  this._pubSubManager = new PubSubManager(this._privateClientCluster, this._socketEventLimit);
  
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
  
  this._subscribers = {};
  
  this._socketEmitter = new EventEmitter();
  this._sessionEmitter = new EventEmitter();
  this._globalEmitter = new EventEmitter();
  
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
    this._privateClientCluster.expire(keys, this._dataExpiry, function() {
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
      
      self._privateClientCluster.query(addressStartQuery, function (err) {
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

IOClusterClient.prototype.bind = function (socket, callback) {
  var self = this;
  
  callback = this._errorDomain.bind(callback);
  
  socket.eventKey = this._keyManager.getSocketEventKey(socket.id);
  socket.dataKey = this._keyManager.getSocketDataKey(socket.id);
  socket.sessionEventKey = this._keyManager.getSessionEventKey(socket.ssid);
  socket.sessionDataKey = this._keyManager.getSessionDataKey(socket.ssid);
  socket.addressDataKey = this._keyManager.getGlobalDataKey(['__meta', 'addresses', socket.address]);
  socket.eventSubscriptions = {};
  socket.eventSubscriptionCount = 0;
  
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
      
      async.parallel([
        function () {
          var cb = arguments[arguments.length - 1];
          self._privateClientCluster.subscribe(socket.eventKey, cb);
        },
        function () {
          var cb = arguments[arguments.length - 1];
          self._privateClientCluster.subscribe(socket.sessionEventKey, cb);
        },
        function () {
          var cb = arguments[arguments.length - 1];
          self._privateClientCluster.query(sessionStartQuery, cb);
        },
        function () {
          var cb = arguments[arguments.length - 1];
          self._privateClientCluster.expire([socket.dataKey], self._dataExpiry, cb);
        },
        function () {
          var cb = arguments[arguments.length - 1];
          self._privateClientCluster.expire([socket.addressDataKey], self._dataExpiry, cb);
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
      self._privateClientCluster.unsubscribe(socket.eventKey, cb);
    },
    function () {
      var cb = arguments[arguments.length - 1];
      self._socketEmitter.removeAllListeners(socket.id);
      self._unsubscribe(socket, null, cb);
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
      }
      
      if (self._sessions[socket.ssid] && isEmpty(self._sessions[socket.ssid].sockets)) {
        self._sessionEmitter.removeAllListeners(socket.ssid);
        delete self._sessions[socket.ssid];
        self.emit('sessiondestroy', socket.ssid);
        self._privateClientCluster.unsubscribe(socket.sessionEventKey, cb);
      } else {
        cb();
      }
    }
  ], function (err) {
    callback(err, socket);
  });
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
  this._privateClientCluster.get(this._keyManager.getGlobalDataKey(addressDataKey), function (err, data) {
    var sockets = [];
    var i;
    for (i in data) {
      sockets.push(i);
    }
    
    callback(err, sockets);
  });
};

IOClusterClient.prototype.global = function (socketId) {
  return new Global(socketId, this._privateClientCluster, this._publicClientCluster, this._pubSubManager);
};

IOClusterClient.prototype.session = function (sessionId, socketId, isLocal) {isLocal = false; // TODO: Remove
  if (isLocal) {
    return new LocalSession(sessionId, socketId, this._privateClientCluster.map(sessionId)[0], this._pubSubManager, this);
  } else {
    return new Session(sessionId, socketId, this._privateClientCluster.map(sessionId)[0], this._pubSubManager);
  }
};

IOClusterClient.prototype.socket = function (socketId, sessionId) {
  if (!sessionId) {
    sessionId = this._sockets[socketId].ssid;
  }
  
  return new Socket(socketId, this._privateClientCluster.map(sessionId)[0], this._pubSubManager);
};