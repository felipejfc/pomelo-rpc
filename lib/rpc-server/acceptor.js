var acceptor = require('./acceptors/nats-acceptor');

module.exports.create = function(opts, cb) {
	return acceptor.create(opts, cb);
};
