var KeyManager = function () {};

KeyManager.prototype.isMemberKey = function (parentKey, memberKey) {
  for (var i in parentKey) {
    if (parentKey[i] != memberKey[i]) {
      return false;
    }
  }
  return true;
};

KeyManager.prototype.getGlobalDataKey = function (keys) {
  var keyChain = ['__iocl', 'gld'];
  if (keys != null) {
    keyChain = keyChain.concat(keys);
  }
  return keyChain;
};

module.exports.KeyManager = KeyManager;