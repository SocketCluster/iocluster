var async = require('async');
var domain = require('domain');
var EventEmitter = require('events').EventEmitter;

var ClientCluster = function (clients) {
  var self = this;
  
  var handleMessage = function (channel, message) {
    self.emit('message', channel, message);
  };
  
  for (var c in clients) {
    clients[c].on('message', handleMessage);
  }
  
  var i, method;
  var client = clients[0];
  var clientIds = [];
  
  var clientInterface = [
    'subscribe',
    'isSubscribed',
    'unsubscribe',
    'publish',
    'set',
    'getExpiry',
    'add',
    'concat',
    'get',
    'getRange',
    'getAll',
    'count',
    'registerDeathQuery',
    'run',
    'query',
    'remove',
    'removeRange',
    'removeAll',
    'splice',
    'pop',
    'hasKey',
    'end'
  ];
  
  var clientUtils = [
    'extractKeys',
    'extractValues'
  ];
  
  var errorDomain = domain.createDomain();
  errorDomain.on('error', function (err) {
    self.emit('error', err);
  });
  
  for (var i in clients) {
    errorDomain.add(clients[i]);
    clients[i].id = i;
    clientIds.push(i);
  }
  
  // Default mapper maps to all clients.
  var mapper = function () {
    return clientIds;
  };
  
  for (var j in clientInterface) {
    (function (method) {
      self[method] = function () {
        var key = arguments[0];
        var lastArg = arguments[arguments.length - 1];
        var results = [];
        var activeClients = self.map(key, method);
        
        if (lastArg instanceof Function) {
          if (activeClients.length < 2) {
            activeClients[0][method].apply(activeClients[0], arguments);
          } else {
            var result;
            var tasks = [];
            var args = Array.prototype.slice.call(arguments, 0, -1);
            var cb = lastArg;
            
            for (var i in activeClients) {
              (function (activeClient) {
                tasks.push(function () {
                  var callback = arguments[arguments.length - 1];
                  result = activeClient[method].apply(activeClient, args.concat(callback));
                  results.push(result);
                });
              })(activeClients[i]);
            }
            async.parallel(tasks, cb);
          }
        } else {
          for (var i in activeClients) {
            result = activeClients[i][method].apply(activeClients[i], arguments);
            results.push(result);
          }
        }
        return results;
      }
    })(clientInterface[j]);
  }
  
  var multiKeyClientInterface = [
    'expire',
    'unexpire'
  ];
  
  for (var m in multiKeyClientInterface) {
    (function (method) {
      self[method] = function () {
        var activeClients, mapping, key;
        var keys = arguments[0];
        var tasks = [];
        var results = [];
        var expiryMap = {};
        
        var lastArg = arguments[arguments.length - 1];
        var cb = lastArg;
        
        for (var j in keys) {
          key = keys[j];
          activeClients = self.map(key, method);
          for (var k in activeClients) {
            mapping = activeClients[k].id;
            if (expiryMap[mapping] == null) {
              expiryMap[mapping] = [];
            }
            expiryMap[mapping].push(key);
          }
        }
        
        var partArgs = Array.prototype.slice.call(arguments, 1, -1);
        
        for (mapping in expiryMap) {
          (function (activeClient, expiryKeys) {
            var newArgs = [expiryKeys].concat(partArgs);
            tasks.push(function () {  
              var callback = arguments[arguments.length - 1];
              var result = activeClient[method].apply(activeClient, newArgs.concat(callback));
              results.push(result);
            });
          })(clients[mapping], expiryMap[mapping]);
        }
        async.parallel(tasks, cb);
        
        return results;
      };
    })(multiKeyClientInterface[m]);
  }
  
  for (var n in clientUtils) {
    method = clientUtils[n];
    this[method] = client[method].bind(client);
  }
  
  this.setMapper = function (mapperFunction) {
    mapper = mapperFunction;
  };
  
  this.getMapper = function (mapperFunction) {
    return mapper;
  };
  
  this.map = function (key, method) {
    var result = mapper(key, method, clientIds);
    if (typeof result == 'number') {
      return [clients[result % clients.length]];
    } else if (result instanceof Array) {
      var dataClients = [];
      for (var i in result) {
        dataClients.push(clients[result[i] % clients.length]);
      }
      return dataClients;
    }
    
    return [];
  };
};

ClientCluster.prototype = Object.create(EventEmitter.prototype);

module.exports.ClientCluster = ClientCluster;