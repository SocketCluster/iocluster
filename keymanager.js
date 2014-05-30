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

KeyManager.prototype.isMemberKey = function (parentKey, memberKey) {
  for (var i in parentKey) {
    if (parentKey[i] != memberKey[i]) {
      return false;
    }
  }
  return true;
};

KeyManager.prototype.getGlobalEventKey = function (key) {
  if (key == null) {
    return ['__iocl', 'gle'];
  } else {
    return ['__iocl', 'gle', key];
  }
};

KeyManager.prototype.isGlobalEventKey = function (key) {
  return this.isMemberKey(['__iocl', 'gle'], key);
};

KeyManager.prototype.getSessionEventKey = function (sessionId, key) {
  if (key == null) {
    return ['__iocl', 'see', sessionId];
  } else {
    return ['__iocl', 'see', sessionId, key];
  }
};

KeyManager.prototype.isSessionEventKey = function (key) {
  return this.isMemberKey(['__iocl', 'see'], key);
};

KeyManager.prototype.getSocketEventKey = function (socketId, key) {
  if (key == null) {
    return ['__iocl', 'soe', socketId];
  } else {
    return ['__iocl', 'soe', socketId, key];
  }
};

KeyManager.prototype.isSocketEventKey = function (key) {
  return this.isMemberKey(['__iocl', 'soe'], key);
};

module.exports.KeyManager = KeyManager;