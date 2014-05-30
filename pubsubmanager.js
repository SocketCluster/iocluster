var async = require('async');
var utils = require('./utils');
var isEmpty = utils.isEmpty;
var KeyManager = require('./keymanager').KeyManager;

var PubSubManager = function (privateClientCluster, socketEventLimit) {
  this._privateClientCluster = privateClientCluster;
  this._keyManager = new KeyManager();
  this._socketEventLimit = socketEventLimit;
  
  this._globalEmitter = new EventEmitter();
  this._sessionEmitters = {};
  this._socketEmitters = {};
  
  this._privateClientCluster.on('message', this._handleMessage.bind(this));
};

PubSubManager.prototype.bind = function (socket, callback) {
  var self = this;
  
  socket.on('subscribe', function (event, res) {
    self.subscribeClientSocket(socket, event, function (err, isNotice) {
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

  socket.on('unsubscribe', function (event, res) {
    self.unsubscribeClientSocket(socket, event, function (err, isNotice) {
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
    function (cb) {
      self._privateClientCluster.subscribe(socket.eventKey, cb);
    },
    function (cb) {
      self._privateClientCluster.subscribe(socket.sessionEventKey, cb);
    },
  ],
  function (err) {
    callback(err, socket);
  });
};

PubSubManager.prototype.unbind = function (socket, callback) {
  socket.off('subscribe');
  socket.off('unsubscribe');
  
  async.waterfall([
    function (cb) {
      self._privateClientCluster.unsubscribe(socket.eventKey, cb);
    },
    function (cb) {
      self._socketEmitter.removeAllListeners(socket.id);
      self._unsubscribe(socket, null, cb);
    }
  ],
  function (err) {
    callback(err, socket);
  });
};

PubSubManager.prototype.watchSession = function (sessionId) {
  this._sessionEmitters[sessionId] = new EventEmitter();
  // TODO: Subscribe to privateClientCluster session
};

PubSubManager.prototype.unwatchSession = function (sessionId) {
  this._sessionEmitters[sessionId].removeAllListeners();
  delete this._sessionEmitters[sessionId];
  // TODO: Unsubscribe from privateClientCluster session
};

PubSubManager.prototype.watchSocket = function (socketId) {
  this._socketEmitters[socketId] = new EventEmitter();
  // TODO: Subscribe to privateClientCluster socket
};

PubSubManager.prototype.unwatchSocket = function (socketId) {
  this._socketEmitters[socketId].removeAllListeners();
  delete this._socketEmitters[socketId];
  // TODO: Unsubscribe from privateClientCluster socket
};

PubSubManager.prototype.subscribeClientSocket = function (socket, events, callback) {
  var self = this;
  
  if (events instanceof Array) {
    var tasks = [];
    for (var i in events) {
      (function (event) {
        tasks.push(function (cb) {
          self._subscribeSingleClient(socket, event, cb);
        });
      })(events[i]);
    }
    async.waterfall(tasks, callback);
  } else {
    this._subscribeSingleClient(socket, events, callback);
  }
};

PubSubManager.prototype.unsubscribeClientSocket = function (socket, events, callback) {
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
          self._unsubscribeSingleClient(socket, event, cb);
        });
      })(events[i]);
    }
    async.waterfall(tasks, callback);
  } else {
    this._unsubscribeSingleClient(socket, events, callback);
  }
};

PubSubManager.prototype.publishGlobalEvent = function (event, data, callback) {
  var eventData = {global: 1, event: event, data: data};
  this._privateClientCluster.publish(this._keyManager.getGlobalEventKey(event), eventData, callback);
};

PubSubManager.prototype.onGlobalEvent = function (event, handler, callback) {
  this._globalEmitter.on(event, handler);
  var eventKey = this._keyManager.getGlobalEventKey(event);
  this._privateClientCluster.subscribe(eventKey, callback);
};

PubSubManager.prototype.removeGlobalServerListener = function (event, handler, callback) {
  this._globalEmitter.removeListener(event, handler);

  if (EventEmitter.listenerCount(this._globalEmitter, event) < 1 &&
    isEmpty(this._globalSubscribers[event])) {
    
    var eventKey = this._keyManager.getGlobalEventKey(event);
    this._privateClientCluster.unsubscribe(eventKey, callback);
  }
};

PubSubManager.prototype.removeAllGlobalServerListeners = function (event, callback) {
  if (event instanceof Function) {
    callback = event;
    event = null;
  }
  if (event) {
    this._globalEmitter.removeAllListeners(event);
    
    if (isEmpty(this._globalSubscribers[event])) {
      var eventKey = this._keyManager.getGlobalEventKey(event);
      this._privateClientCluster.unsubscribe(eventKey, callback);
    } else {
      callback && callback();
    }
  } else {
    this._globalEmitter.removeAllListeners();
    // TODO: Unsubscribe from all global events - Keep session and socket events subscriptions
    // maybe unsubscribe from this._keyManager.getGlobalEventKey()?
    callback && callback();
  }
};

PubSubManager.prototype.globalServerListeners = function (event) {
  return this._globalEmitter.listeners(event);
};

PubSubManager.prototype.publishSessionEvent = function (sessionId, event, data, excludeSocket, callback) {
  var eventData = {session: sessionId, event: event, data: data};
  if (excludeSocket != null) {
    eventData.exclude = excludeSocket;
  }
  this._privateClientCluster.publish(this._keyManager.getSessionEventKey(sessionId), eventData, callback);
};

PubSubManager.prototype.onSessionEvent = function (sessionId, event, handler, callback) {
  this._sessionEmitters[sessionId].on(event, handler);
  callback && callback();
};

PubSubManager.prototype.removeSessionServerListener = function (sessionId, event, handler, callback) {
  this._sessionEmitters[sessionId].removeListener(event, handler);
  callback && callback();
};

PubSubManager.prototype.removeAllSessionServerListeners = function (sessionId, event, callback) {
  if (event instanceof Function) {
    callback = event;
    event = null;
  }
  if (event) {
    this._sessionEmitters[sessionId].removeAllListeners(event);
  } else {
    this._sessionEmitters[sessionId].removeAllListeners();
  }
  callback && callback();
};

PubSubManager.prototype.sessionServerListeners = function (sessionId, event) {
  return this._sessionEmitters[sessionId].listeners(event);
};

PubSubManager.prototype.publishSocketEvent = function (socketId, event, data, excludeSocket, callback) {
  var eventData = {socket: socketId, event: event, data: data};
  if (excludeSocket != null) {
    eventData.exclude = excludeSocket;
  }
  this._privateClientCluster.publish(this._keyManager.getSocketEventKey(socketId), eventData, callback);
};

PubSubManager.prototype.onSocketEvent = function (socketId, event, handler, callback) {
  this._socketEmitters[socketId].on(event, handler);
  callback && callback();
};

PubSubManager.prototype.removeSocketServerListener = function (socketId, event, handler, callback) {
  this._socketEmitters[socketId].removeListener(event, handler);
  callback && callback();
};

PubSubManager.prototype.removeAllSocketServerListeners = function (socketId, event, callback) {
  if (event instanceof Function) {
    callback = event;
    event = null;
  }
  if (event) {
    this._socketEmitters[socketId].removeAllListeners(event);
  } else {
    this._socketEmitters[socketId].removeAllListeners();
  }
  
  callback && callback();
};

PubSubManager.prototype.socketServerListeners = function (socketId, event) {
  return this._socketEmitters[socketId].listeners(event);
};

PubSubManager.prototype._subscribeSingleClient = function (socket, event, callback) {
  var self = this;
  
  if (this._socketEventLimit && socket.eventSubscriptionCount >= this._socketEventLimit) {
    callback('Socket ' + socket.id + ' tried to exceed the event subscription limit of ' +
      this._socketEventLimit, true);
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
        
        if (!self._sessionSubscribers[socket.ssid]) {
          self._sessionSubscribers[socket.ssid] = {};
        }
        if (!self._sessionSubscribers[socket.ssid][event]) {
          self._sessionSubscribers[socket.ssid][event] = {};
        }
        self._sessionSubscribers[socket.ssid][event][socket.id] = socket;
        
        if (!self._socketSubscribers[socket.id]) {
          self._socketSubscribers[socket.id] = {};
        }
        if (!self._socketSubscribers[socket.id][event]) {
          self._socketSubscribers[socket.id][event] = {};
        }
        self._socketSubscribers[socket.id][event][socket.id] = socket;
      }
      callback && callback(err);
    };
    
    var eventKey = this._keyManager.getGlobalEventKey(event);
    this._privateClientCluster.subscribe(eventKey, addSubscription);
  }
};

PubSubManager.prototype._unsubscribeSingleClient = function (socket, event, callback) {
  if (this._globalSubscribers[event]) {
    delete this._globalSubscribers[event][socket.id];
  }
  if (this._sessionSubscribers[socket.ssid] && this._sessionSubscribers[socket.ssid][event]) {
    delete this._sessionSubscribers[socket.ssid][event][socket.id];
    if (isEmpty(this._sessionSubscribers[socket.ssid][event])) {
      delete this._sessionSubscribers[socket.ssid][event];
      if (isEmpty(this._sessionSubscribers[socket.ssid])) {
        delete this._sessionSubscribers[socket.ssid];
      }
    }
  }
  if (this._socketSubscribers[socket.id] && this._socketSubscribers[socket.id][event]) {
    delete this._socketSubscribers[socket.id][event][socket.id];
    if (isEmpty(this._socketSubscribers[socket.id][event])) {
      delete this._socketSubscribers[socket.id][event];
      if (isEmpty(this._socketSubscribers[socket.id])) {
        delete this._socketSubscribers[socket.id];
      }
    }
  }
  
  delete socket.eventSubscriptions[event];
  
  if (socket.eventSubscriptionCount != null) {
    socket.eventSubscriptionCount--;
  }
  
  if (isEmpty(this._globalSubscribers[event])) {
    delete this._globalSubscribers[event];
    
    if (EventEmitter.listenerCount(this._globalEmitter, event) < 1) {
      var eventKey = this._keyManager.getGlobalEventKey(event);
      this._privateClientCluster.unsubscribe(eventKey, function (err) {
        callback && callback(err);
      });
    } else {
      callback && callback();
    }
  } else {
    callback && callback();
  }
};

PubSubManager.prototype._notifySubscribers = function (eventSubscribers, message) {
  for (var i in eventSubscribers) {
    var socket = eventSubscribers[i];
    if (message.exclude == null || socket.id != message.exclude) {
      socket.emit(message.event, message.data);
    }
  }
};

PubSubManager.prototype._handleMessage = function (channel, message) {
  if (message.global) {
    this._notifySubscribers(this._globalSubscribers[message.event], message);
    this._globalEmitter.emit(message.event, message.data);
  } else if (message.session) {
    if (this._sessionSubscribers[message.session]) {
      this._notifySubscribers(this._sessionSubscribers[message.session][message.event], message);
    }
    if (this._sessionEmitters[message.session]) {
      this._sessionEmitters[message.session].emit(message.event, message.data);
    }
  } else if (message.socket) {
    if (this._socketSubscribers[message.socket]) {
      this._notifySubscribers(this._socketSubscribers[message.socket][message.event], message);
    }
    if (this._socketEmitters[message.socket]) {
      this._socketEmitters[message.socket].emit(message.event, message.data);
    }
  }
};

module.exports.PubSubManager = PubSubManager;