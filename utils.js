module.exports.isEmpty = function (obj) {
  var i;
  for (i in obj) {
    return false;
  }
  return true;
};