const acceptor = require('./acceptors/websocket-acceptor');

module.exports.create = (opts, cb) => acceptor.create(opts, cb);
