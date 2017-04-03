var logger = require('pomelo-logger').getLogger('pomelo-rpc', 'nats-acceptor');
var EventEmitter = require('events').EventEmitter;
var Tracer = require('../../util/tracer');
var utils = require('../../util/utils');
var MqttCon = require('mqtt-connection');
var util = require('util');
var net = require('net');
var NATS = require('nats');
var path = require('path');

var curId = 1;

var Acceptor = function(opts, cb) {
  EventEmitter.call(this);
  this.interval = opts.interval; // flush interval in ms
  this.bufferMsg = opts.bufferMsg;
  this.rpcLogger = opts.rpcLogger;
  this.rpcDebugLog = opts.rpcDebugLog;
  this.opts = opts;
  this.config = opts.context.get('rpcConfig') || {};
  this.natsServers = this.config.natsServers || [];
  this.natsPrefix = this.config.prefix || "pomelo";
  this._interval = null; // interval object
  this.sockets = {};
  this.msgQueues = {};
  this.cb = cb;
};

util.inherits(Acceptor, EventEmitter);

var pro = Acceptor.prototype;

pro.getMailboxAddr = function(serverId){
  var addr = path.join('/', this.natsPrefix, 'mailbox', serverId);
  return addr;
}

pro.sendReply = function(msg, replyTo) {
  this.client.publish(replyTo, JSON.stringify(msg));
}

pro.listen = function(port) {
  //check status
  if (!!this.inited) {
    this.cb(new Error('already inited.'));
    return;
  }
  this.inited = true;

  var self = this;
  //TODO nats reconnect?
  this.client = NATS.connect({'servers': this.natsServers});
  var mailbox = this.getMailboxAddr(this.opts.context.serverId);

  this.client.on('connect', function(){
    logger.debug('successfully connected to nats')

    self.client.subscribe(mailbox, function onMessage(pkg, replyTo) {
      var isArray = false;
      try {
        pkg = JSON.parse(pkg);
        if (pkg instanceof Array) {
          processMsgs(self, pkg, replyTo);
          isArray = true;
        } else {
          processMsg(self, pkg, replyTo);
        }
      } catch (err) {
        if (!isArray) {
          self.sendReply(replyTo, {
            resp: [cloneError(err)]
          });
        }
        logger.error('process rpc message error %s', err.stack);
      }
    });

    self.client.on('error', function(err) {
      logger.error('rpc server is error: %j', err.stack);
      self.emit('error', err);
    });
  })

//  if (this.bufferMsg) {
//    this._interval = setInterval(function() {
//      flush(self);
//    }, this.interval);
//  }
};

//pro.close = function() {
//  if (this.closed) {
//    return;
//  }
//  this.closed = true;
//  if (this._interval) {
//    clearInterval(this._interval);
//    this._interval = null;
//  }
//  this.emit('closed');
//};

//pro.onSocketClose = function(socket) {
//  if (!socket['closed']) {
//    var id = socket.id;
//    socket['closed'] = true;
//    delete this.sockets[id];
//    delete this.msgQueues[id];
//  }
//}

var cloneError = function(origin) {
  // copy the stack infos for Error instance json result is empty
  var res = {
    msg: origin.msg,
    stack: origin.stack
  };
  return res;
};

var processMsg = function(acceptor, pkg, replyTo) {
  var tracer = null;
  //TODO fix
  //if (this.rpcDebugLog) {
  //  tracer = new Tracer(acceptor.rpcLogger, acceptor.rpcDebugLog, pkg.remote, pkg.source, pkg.msg, pkg.traceId, pkg.seqId);
  //  tracer.info('server', __filename, 'processMsg', 'mqtt-acceptor receive message and try to process message');
  //}
  acceptor.cb(tracer, pkg, function(cb) {
    // var args = Array.prototype.slice.call(arguments, 0);
    var len = arguments.length;
    var args = new Array(len);
    for (var i = 0; i < len; i++) {
      args[i] = arguments[i];
    }

    var errorArg = args[0]; // first callback argument can be error object, the others are message
    if (errorArg && errorArg instanceof Error) {
      args[0] = cloneError(errorArg);
    }

    var resp;
    //TODO fix
    //if (tracer && tracer.isEnabled) {
    //  resp = {
    //    traceId: tracer.id,
    //    seqId: tracer.seq,
    //    source: tracer.source,
    //    id: pkg.id,
    //    resp: args
    //  };
    //} else {
    resp = {
      //id: pkg.id,
      resp: args
    };
    //}
    //if (acceptor.bufferMsg) {
    //  enqueue(socket, acceptor, resp);
    //} else {
     // doSend(socket, resp);
    //}
    acceptor.sendReply(resp, replyTo);
  });
};

var processMsgs = function(acceptor, pkgs, replyTo) {
  for (var i = 0, l = pkgs.length; i < l; i++) {
    processMsg(acceptor, pkgs[i], replyTo);
  }
};

//var enqueue = function(socket, acceptor, msg) {
//  var id = socket.id;
//  var queue = acceptor.msgQueues[id];
//  if (!queue) {
//    queue = acceptor.msgQueues[id] = [];
//  }
//  queue.push(msg);
//};
//
//var flush = function(acceptor) {
//  var sockets = acceptor.sockets,
//    queues = acceptor.msgQueues,
//    queue, socket;
//  for (var socketId in queues) {
//    socket = sockets[socketId];
//    if (!socket) {
//      // clear pending messages if the socket not exist any more
//      delete queues[socketId];
//      continue;
//    }
//    queue = queues[socketId];
//    if (!queue.length) {
//      continue;
//    }
//    doSend(socket, queue);
//    queues[socketId] = [];
//  }
//};

var doSend = function(socket, msg) {
  socket.publish({
    topic: 'rpc',
    payload: JSON.stringify(msg)
  });
}

/**
 * create acceptor
 *
 * @param opts init params
 * @param cb(tracer, msg, cb) callback function that would be invoked when new message arrives
 */
module.exports.create = function(opts, cb) {
  return new Acceptor(opts || {}, cb);
};
