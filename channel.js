var EventEmitter = require('events').EventEmitter;

if (!Object.create) {
  Object.create = require('./objectcreate');
}

var Channel = function (name, client) {
  var self = this;
  
  EventEmitter.call(this);
  
  this.PENDING = 'pending';
  this.SUBSCRIBED = 'subscribed';
  this.UNSUBSCRIBED = 'unsubscribed';
  
  this.name = name;
  this.state = this.UNSUBSCRIBED;
  this.client = client;
};

Channel.prototype = Object.create(EventEmitter.prototype);

Channel.prototype.getState = function () {
  return this.state;
};

Channel.prototype.subscribe = function () {
  this.client.subscribe(this.name);
};

Channel.prototype.unsubscribe = function () {
  this.client.unsubscribe(this.name);
};

Channel.prototype.isSubscribed = function (includePending) {
  return this.client.isSubscribed(this.name, includePending);
};

// publish([data, serviceLevel, callback])
Channel.prototype.publish = function () {
  var data = arguments[0];
  var serviceLevel, callback;
  if (arguments[1] instanceof Function) {
    serviceLevel = 0;
    callback = arguments[1];
  } else {
    serviceLevel = arguments[1] || 0;
    callback = arguments[2];
  }
  
  this.client.publish(this.name, data, serviceLevel, callback);
};

Channel.prototype.watch = function (handler) {
  this.client.watch(this.name, handler);
};

Channel.prototype.unwatch = function (handler) {
  this.client.unwatch(this.name, handler);
};

Channel.prototype.destroy = function () {
  this.client.destroyChannel(this.name);
};

module.exports = Channel;