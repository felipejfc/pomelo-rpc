var logger = require('pomelo-logger').getLogger('pomelo-rpc', 'MailStation');
var EventEmitter = require('events').EventEmitter;
var blackhole = require('./mailboxes/blackhole');
var defaultMailboxFactory = require('./mailbox');
var constants = require('../util/constants');
var utils = require('../util/utils');
var util = require('util');
var NATS = require('nats');
var path = require('path');

var STATE_INITED = 1; // station has inited
var STATE_STARTED = 2; // station has started
var STATE_CLOSED = 3; // station has closed

/**
 * Mail station constructor.
 *
 * @param {Object} opts construct parameters
 */
var MailStation = function(opts) {
  EventEmitter.call(this);
  this.opts = opts;
  this.config = opts.context.get('rpcConfig') || {};
  this.natsServers = this.config.natsServers || [];
  this.natsPrefix = this.config.prefix || "pomelo";
  this.requestTimeout = this.config.requestTimeout || 3000;
  this.servers = {}; // remote server info map, key: server id, value: info
  this.serversMap = {}; // remote server info map, key: serverType, value: servers array
  this.onlines = {}; // remote server online map, key: server id, value: 0/offline 1/online
  this.mailboxFactory = opts.mailboxFactory || defaultMailboxFactory;

  // filters
  this.befores = [];
  this.afters = [];

  // pending request queues
  this.pendings = {};
  this.pendingSize = opts.pendingSize || constants.DEFAULT_PARAM.DEFAULT_PENDING_SIZE;

// working mailbox map
  this.mailboxes = {};

  this.state = STATE_INITED;
};

util.inherits(MailStation, EventEmitter);

var pro = MailStation.prototype;

/**
 * Init and start station. Connect all mailbox to remote servers.
 *
 * @param  {Function} cb(err) callback function
 * @return {Void}
 */
pro.start = function(cb) {
  if (this.state > STATE_INITED) {
    cb(new Error('station has started.'));
    return;
  }

  var self = this;
  //TODO nats reconnect?
  var nats = NATS.connect({'servers': this.natsServers});
  nats.on('connect', function(client){
    self.client = client;
    logger.debug('successfully connected to nats')
    process.nextTick(function() {
      self.state = STATE_STARTED;
      cb();
    });
  })
};

/**
 * Stop station and all its mailboxes
 *
 * @param  {Boolean} force whether stop station forcely
 * @return {Void}
 */
pro.stop = function(force) {
  if (this.state !== STATE_STARTED) {
    logger.warn('[pomelo-rpc] client is not running now.');
    return;
  }
  this.state = STATE_CLOSED;

  var self = this;
  this.client.close();
};

/**
 * Add a new server info into the mail station and clear
 * the blackhole associated with the server id if any before.
 *
 * @param {Object} serverInfo server info such as {id, host, port}
 */
pro.addServer = function(serverInfo) {
  if (!serverInfo || !serverInfo.id) {
    return;
  }

  var id = serverInfo.id;
  var type = serverInfo.serverType;
  this.servers[id] = serverInfo;
  this.onlines[id] = 1;

  if (!this.serversMap[type]) {
    this.serversMap[type] = [];
  }

  if (this.serversMap[type].indexOf(id) < 0) {
    this.serversMap[type].push(id);
  }
  this.emit('addServer', id);
};

/**
 * Batch version for add new server info.
 *
 * @param {Array} serverInfos server info list
 */
pro.addServers = function(serverInfos) {
  if (!serverInfos || !serverInfos.length) {
    return;
  }

  for (var i = 0, l = serverInfos.length; i < l; i++) {
    this.addServer(serverInfos[i]);
  }
};

/**
 * Remove a server info from the mail station and remove
 * the mailbox instance associated with the server id.
 *
 * @param  {String|Number} id server id
 */
pro.removeServer = function(id) {
  this.onlines[id] = 0;
  this.emit('removeServer', id);
};

/**
 * Batch version for remove remote servers.
 *
 * @param  {Array} ids server id list
 */
pro.removeServers = function(ids) {
  if (!ids || !ids.length) {
    return;
  }

  for (var i = 0, l = ids.length; i < l; i++) {
    this.removeServer(ids[i]);
  }
};

/**
 * Clear station infomation.
 *
 */
pro.clearStation = function() {
  this.onlines = {};
  this.serversMap = {};
}

/**
 * Replace remote servers info.
 *
 * @param {Array} serverInfos server info list
 */
//TODO FIX
pro.replaceServers = function(serverInfos) {
  //TODO 
  this.clearStation();
  if (!serverInfos || !serverInfos.length) {
    return;
  }

  for (var i = 0, l = serverInfos.length; i < l; i++) {
    var id = serverInfos[i].id;
    var type = serverInfos[i].serverType;
    this.onlines[id] = 1;
    if (!this.serversMap[type]) {
      this.serversMap[type] = [];
    }
    this.servers[id] = serverInfos[i];
    if (this.serversMap[type].indexOf(id) < 0) {
      this.serversMap[type].push(id);
    }
  }
};

pro.getMailboxAddr = function(serverId){
  var addr = path.join('/', this.natsPrefix, 'mailbox', serverId);
  return addr;
}

/**
 * Dispatch rpc message to the mailbox
 *
 * @param  {Object}   tracer   rpc debug tracer
 * @param  {String}   serverId remote server id
 * @param  {Object}   msg      rpc invoke message
 * @param  {Object}   opts     rpc invoke option args
 * @param  {Function} cb       callback function
 * @return {Void}
 */
pro.dispatch = function(tracer, serverId, msg, opts, cb) {
  var self = this;
  tracer && tracer.info('client', __filename, 'dispatch', 'dispatch rpc message to the mailbox');
  tracer && (tracer.cb = cb);
  if (this.state !== STATE_STARTED) {
    tracer && tracer.error('client', __filename, 'dispatch', 'client is not running now');
    logger.error('[pomelo-rpc] client is not running now.');
    this.emit('error', constants.RPC_ERROR.SERVER_NOT_STARTED, tracer, serverId, msg, opts);
    return;
  }

  //TODO ajeitar isso, se dest nao ta conectado emite isso e poe em pending?
  //self.emit('error', constants.RPC_ERROR.NO_TRAGET_SERVER, tracer, serverId, msg, opts);

  var send = function(tracer, err, serverId, msg, opts) {
    tracer && tracer.info('client', __filename, 'send', 'get corresponding mailbox and try to send message');
    var mailbox = self.getMailboxAddr(serverId);
    var msg = JSON.stringify(msg);
    self.client.requestOne(mailbox, msg, {}, self.requestTimeout, function(response){
      if(response.code && response.code === NATS.REQ_TIMEOUT) {
       logger.error('[pomelo-rpc] fail to send message, timeout on maibox: %s', mailbox);
        self.emit('error', constants.RPC_ERROR.TIMEOUT, tracer, serverId, msg, opts);
        //TODO fix send_err
        cb && cb(send_err);
        return;
      }
      var resp = JSON.parse(response).resp;
      doFilter(tracer, null, serverId, msg, opts, self.afters, 0, 'after', function(tracer, err, serverId, msg, opts) {
        if (err) {
          errorHandler(tracer, self, err, serverId, msg, opts, false, cb);
        }
        utils.applyCallback(cb, resp);
      });
    })
  };

  doFilter(tracer, null, serverId, msg, opts, this.befores, 0, 'before', send);
};

/**
 * Add a before filter
 *
 * @param  {[type]} filter [description]
 * @return {[type]}        [description]
 */
pro.before = function(filter) {
  if (Array.isArray(filter)) {
    this.befores = this.befores.concat(filter);
    return;
  }
  this.befores.push(filter);
};

/**
 * Add after filter
 *
 * @param  {[type]} filter [description]
 * @return {[type]}        [description]
 */
pro.after = function(filter) {
  if (Array.isArray(filter)) {
    this.afters = this.afters.concat(filter);
    return;
  }
  this.afters.push(filter);
};

/**
 * Add before and after filter
 *
 * @param  {[type]} filter [description]
 * @return {[type]}        [description]
 */
pro.filter = function(filter) {
  this.befores.push(filter);
  this.afters.push(filter);
};

/**
 * Do before or after filter
 */
var doFilter = function(tracer, err, serverId, msg, opts, filters, index, operate, cb) {
  if (index < filters.length) {
    tracer && tracer.info('client', __filename, 'doFilter', 'do ' + operate + ' filter ' + filters[index].name);
  }
  if (index >= filters.length || !!err) {
    cb(tracer, err, serverId, msg, opts);
    return;
  }
  var self = this;
  var filter = filters[index];
  if (typeof filter === 'function') {
    filter(serverId, msg, opts, function(target, message, options) {
      index++;
      //compatible for pomelo filter next(err) method
      if (utils.getObjectClass(target) === 'Error') {
        doFilter(tracer, target, serverId, msg, opts, filters, index, operate, cb);
      } else {
        doFilter(tracer, null, target || serverId, message || msg, options || opts, filters, index, operate, cb);
      }
    });
    return;
  }
  if (typeof filter[operate] === 'function') {
    filter[operate](serverId, msg, opts, function(target, message, options) {
      index++;
      if (utils.getObjectClass(target) === 'Error') {
        doFilter(tracer, target, serverId, msg, opts, filters, index, operate, cb);
      } else {
        doFilter(tracer, null, target || serverId, message || msg, options || opts, filters, index, operate, cb);
      }
    });
    return;
  }
  index++;
  doFilter(tracer, err, serverId, msg, opts, filters, index, operate, cb);
};

//
//var addToPending = function(tracer, station, serverId, args) {
//  tracer && tracer.info('client', __filename, 'addToPending', 'add pending requests to pending queue');
//  var pending = station.pendings[serverId];
//  if (!pending) {
//    pending = station.pendings[serverId] = [];
//  }
//  if (pending.length > station.pendingSize) {
//    tracer && tracer.debug('client', __filename, 'addToPending', 'station pending too much for: ' + serverId);
//    logger.warn('[pomelo-rpc] station pending too much for: %s', serverId);
//    return;
//  }
//  pending.push(args);
//};
//
//var flushPending = function(tracer, station, serverId, cb) {
//  tracer && tracer.info('client', __filename, 'flushPending', 'flush pending requests to dispatch method');
//  var pending = station.pendings[serverId];
//  var mailbox = station.mailboxes[serverId];
//  if (!pending || !pending.length) {
//    return;
//  }
//  if (!mailbox) {
//    tracer && tracer.error('client', __filename, 'flushPending', 'fail to flush pending messages for empty mailbox: ' + serverId);
//    logger.error('[pomelo-rpc] fail to flush pending messages for empty mailbox: ' + serverId);
//  }
//  for (var i = 0, l = pending.length; i < l; i++) {
//    station.dispatch.apply(station, pending[i]);
//  }
//  delete station.pendings[serverId];
//};
//
var errorHandler = function(tracer, station, err, serverId, msg, opts, flag, cb) {
  if (!!station.handleError) {
    station.handleError(err, serverId, msg, opts);
  } else {
    logger.error('[pomelo-rpc] rpc filter error with serverId: %s, err: %j', serverId, err.stack);
    station.emit('error', constants.RPC_ERROR.FILTER_ERROR, tracer, serverId, msg, opts);
  }
};
//
///**
// * Mail station factory function.
// *
// * @param  {Object} opts construct paramters
// *           opts.servers {Object} global server info map. {serverType: [{id, host, port, ...}, ...]}
// *           opts.mailboxFactory {Function} mailbox factory function
// * @return {Object}      mail station instance
// */
module.exports.create = function(opts) {
  return new MailStation(opts || {});
};
