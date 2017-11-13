const utils = require('../util/utils');
const constants = require('../util/constants');
const defaultMailboxFactory = require('./mailbox');
const EventEmitter = require('events');
const logger = require('dreamix-logger').getLogger('dreamix-rpc', __filename);

const STATE_INITED = 1; // station has inited
const STATE_STARTED = 2; // station has started
const STATE_CLOSED = 3; // station has closed


function errorHandler(tracer, station, err, serverId, msg, opts) {
    if (station.handleError) {
        station.handleError(err, serverId, msg, opts);
    } else {
        logger.error('[dreamix-rpc] rpc filter error with serverId: %s, err: %j', serverId, err.stack);
        station.emit('error', constants.RPC_ERROR.FILTER_ERROR, tracer, serverId, msg, opts);
    }
}

function addToPending(tracer, station, serverId, args) {
    tracer.info('client', __filename, 'addToPending', 'add pending requests to pending queue');
    let pending = station.pendings[serverId];
    if (!pending) {
        station.pendings[serverId] = [];
        pending = [];
    }
    if (pending.length > station.pendingSize) {
        tracer.debug('client', __filename, 'addToPending', `station pending too much for: ${serverId}`);
        logger.warn('[dreamix-rpc] station pending too much for: %s', serverId);
        return;
    }
    pending.push(args);
}

function flushPending(tracer, station, serverId) {
    tracer.info('client', __filename, 'flushPending', 'flush pending requests to dispatch method');
    const pending = station.pendings[serverId];
    const mailbox = station.mailboxes[serverId];
    if (!pending || !pending.length) {
        return;
    }
    if (!mailbox) {
        tracer.error('client', __filename, 'flushPending', `fail to flush pending messages for empty mailbox: ${serverId}`);
        logger.error(`[dreamix-rpc] fail to flush pending messages for empty mailbox: ${serverId}`);
    }
    for (let i = 0, l = pending.length; i < l; i++) {
        station.dispatch(...pending[i]);
    }
    delete station.pendings[serverId];
}

function lazyConnect(tracer, station, serverId, factory, cb) {
    tracer.info('client', __filename, 'lazyConnect', 'create mailbox and try to connect to remote server');
    const server = station.servers[serverId];
    const online = station.onlines[serverId];
    if (!server) {
        logger.error('[dreamix-rpc] unknown server: %s', serverId);
        return false;
    }
    if (!online || online !== 1) {
        logger.error('[dreamix-rpc] server is not online: %s', serverId);
    }
    const mailbox = factory.create(server, station.opts);
    station.connecting[serverId] = true;
    station.mailboxes[serverId] = mailbox;
    station.connect(tracer, serverId, cb);
    return true;
}

/**
 * Do before or after filter
 */
function doFilter(tracer, err, serverId, msg, opts, filters, index, operate, cb) {
    if (index < filters.length) {
        tracer.info('client', __filename, 'doFilter', `do ${operate} filter ${filters[index].name}`);
    }
    if (index >= filters.length || !!err) {
        utils.invokeCallback(cb, tracer, err, serverId, msg, opts);
        return;
    }
    const filter = filters[index];
    if (typeof filter === 'function') {
        filter(serverId, msg, opts, (target, message, options) => {
            index++;
            // compatible for pomelo filter next(err) method
            if (utils.getObjectClass(target) === 'Error') {
                doFilter(tracer, target, serverId, msg, opts, filters, index, operate, cb);
            } else {
                doFilter(tracer, null, target || serverId, message || msg, options || opts, filters, index, operate, cb);
            }
        });
        return;
    }
    if (typeof filter[operate] === 'function') {
        filter[operate](serverId, msg, opts, (target, message, options) => {
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
}


class MailStation extends EventEmitter {
    constructor(opts) {
        super();
        this.opts = opts;
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

        // connecting remote server mailbox map
        this.connecting = {};

        // working mailbox map
        this.mailboxes = {};

        this.state = STATE_INITED;
    }

    /**
     * Init and start station. Connect all mailbox to remote servers.
     *
     * @param  {Function} cb(err) callback function
     * @return
     */
    start(cb) {
        if (this.state > STATE_INITED) {
            utils.invokeCallback(cb, new Error('station has started.'));
            return;
        }

        const self = this;
        process.nextTick(() => {
            self.state = STATE_STARTED;
            utils.invokeCallback(cb);
        });
    }

    /**
     * Stop station and all its mailboxes
     *
     * @param  {Boolean} force whether stop station forcely
     * @return
     */
    stop(force) {
        if (this.state !== STATE_STARTED) {
            logger.warn('[dreamix-rpc] client is not running now.');
            return;
        }
        this.state = STATE_CLOSED;

        const self = this;

        function closeAll() {
            for (const id in self.mailboxes) {
                if (self.mailboxes.hasOwnProperty(id)) {
                    self.mailboxes[id].close();
                }
            }
        }

        if (force) {
            closeAll();
        } else {
            setTimeout(closeAll, constants.DEFAULT_PARAM.GRACE_TIMEOUT);
        }
    }

    /**
     * Add a new server info into the mail station and clear
     *
     * @param {Object} serverInfo server info such as {id, host, port}
     */
    addServer(serverInfo) {
        if (!serverInfo || !serverInfo.id) {
            return;
        }

        const id = serverInfo.id;
        const type = serverInfo.serverType;
        this.servers[id] = serverInfo;
        this.onlines[id] = 1;

        if (!this.serversMap[type]) {
            this.serversMap[type] = [];
        }
        this.serversMap[type].push(id);
        this.emit('addServer', id);
    }

    /**
     * Batch version for add new server info.
     *
     * @param {Array} serverInfos server info list
     */
    addServers(serverInfos) {
        if (!serverInfos || !serverInfos.length) {
            return;
        }

        for (let i = 0, l = serverInfos.length; i < l; i++) {
            this.addServer(serverInfos[i]);
        }
    }

    /**
     * Remove a server info from the mail station and remove
     * the mailbox instance associated with the server id.
     *
     * @param  {String|Number} id server id
     */
    removeServer(id) {
        this.onlines[id] = 0;
        const mailbox = this.mailboxes[id];
        if (mailbox) {
            mailbox.close();
            delete this.mailboxes[id];
        }
        this.emit('removeServer', id);
    }

    /**
     * Batch version for remove remote servers.
     *
     * @param  {Array} ids server id list
     */
    removeServers(ids) {
        if (!ids || !ids.length) {
            return;
        }

        for (let i = 0, l = ids.length; i < l; i++) {
            this.removeServer(ids[i]);
        }
    }

    /**
     * Clear station infomation.
     *
     */
    clearStation() {
        this.onlines = {};
        this.serversMap = {};
    }

    /**
     * Replace remote servers info.
     *
     * @param {Array} serverInfos server info list
     */
    replaceServers(serverInfos) {
        this.clearStation();
        if (!serverInfos || !serverInfos.length) {
            return;
        }

        for (let i = 0, l = serverInfos.length; i < l; i++) {
            const id = serverInfos[i].id;
            const type = serverInfos[i].serverType;
            this.onlines[id] = 1;
            if (!this.serversMap[type]) {
                this.serversMap[type] = [];
            }
            this.servers[id] = serverInfos[i];
            this.serversMap[type].push(id);
        }
    }

    /**
     * Dispatch rpc message to the mailbox
     *
     * @param  {Object}   tracer   rpc debug tracer
     * @param  {String}   serverId remote server id
     * @param  {Object}   msg      rpc invoke message
     * @param  {Object}   opts     rpc invoke option args
     * @param  {Function} cb       callback function
     * @return
     */
    dispatch(tracer, serverId, msg, opts, cb) {
        tracer.info('client', __filename, 'dispatch', 'dispatch rpc message to the mailbox');
        tracer.cb = cb;
        if (this.state !== STATE_STARTED) {
            tracer.error('client', __filename, 'dispatch', 'client is not running now');
            logger.error('[dreamix-rpc] client is not running now.');
            this.emit('error', constants.RPC_ERROR.SERVER_NOT_STARTED, tracer, serverId, msg, opts);
            return;
        }

        const self = this;
        const mailbox = this.mailboxes[serverId];
        if (!mailbox) {
            tracer.debug('client', __filename, 'dispatch', 'mailbox is not exist');
            // try to connect remote server if mailbox instance not exist yet
            if (!lazyConnect(tracer, this, serverId, this.mailboxFactory, cb)) {
                tracer.error('client', __filename, 'dispatch', `fail to find remote server:${serverId}`);
                logger.error(`[dreamix-rpc] fail to find remote server:${serverId}`);
                self.emit('error', constants.RPC_ERROR.NO_TRAGET_SERVER, tracer, serverId, msg, opts);
            }
            // push request to the pending queue
            addToPending(tracer, this, serverId, Array.prototype.slice.call(arguments, 0));// eslint-disable-line
            return;
        }

        if (this.connecting[serverId]) {
            tracer.debug('client', __filename, 'dispatch', 'request add to connecting');
            // if the mailbox is connecting to remote server
            addToPending(tracer, this, serverId, Array.prototype.slice.call(arguments, 0));// eslint-disable-line
            return;
        }

        const send = (_tracer, _err, _serverId, _msg, _opts) => {
            tracer.info('client', __filename, 'send', 'get corresponding mailbox and try to send message');
            const _mailbox = self.mailboxes[_serverId];
            if (_err) {
                errorHandler(_tracer, self, _err, _serverId, _msg, _opts, true, cb);
                return;
            }
            if (!_mailbox) {
                _tracer.error('client', __filename, 'send', `can not find mailbox with id:${_serverId}`);
                logger.error(`[dreamix-rpc] could not find mailbox with id:${_serverId}`);
                self.emit('error', constants.RPC_ERROR.FAIL_FIND_MAILBOX, _tracer, _serverId, _msg, _opts);
                return;
            }
            _mailbox.send(_tracer, _msg, _opts, (tracerSend, sendErr, ...args) => {
                if (sendErr) {
                    logger.error('[pomelo-rpc] fail to send message');
                    self.emit('error', constants.RPC_ERROR.FAIL_SEND_MESSAGE, _tracer, _serverId, _msg, _opts);
                    return;
                }
                doFilter(tracerSend, null, _serverId, _msg, _opts, self.afters, 0, 'after', (__tracer, __err, __serverId, __msg, __opts) => {
                    if (__err) {
                        errorHandler(__tracer, self, __err, __serverId, __msg, __opts, false, cb);
                    }
                    utils.applyCallback(cb, args);
                });
            });
        };

        doFilter(tracer, null, serverId, msg, opts, this.befores, 0, 'before', send);
    }


    /**
     * Add a before filter
     *
     * @param  {[type]} filter [description]
     * @return {[type]}        [description]
     */
    before(filter) {
        if (Array.isArray(filter)) {
            this.befores = this.befores.concat(filter);
            return;
        }
        this.befores.push(filter);
    }

    /**
     * Add after filter
     *
     * @param  {[type]} filter [description]
     * @return {[type]}        [description]
     */
    after(filter) {
        if (Array.isArray(filter)) {
            this.afters = this.afters.concat(filter);
            return;
        }
        this.afters.push(filter);
    }

    /**
     * Add before and after filter
     *
     * @param  {[type]} filter [description]
     * @return {[type]}        [description]
     */
    filter(filter) {
        this.befores.push(filter);
        this.afters.push(filter);
    }

    /**
     * Try to connect to remote server
     *
     * @param  {Object}   tracer   rpc debug tracer
     * @param {String}   serverId remote server id
     */
    connect(tracer, serverId) {
        const self = this;
        const mailbox = self.mailboxes[serverId];
        mailbox.connect(tracer, (err) => {
            if (err) {
                tracer.error('client', __filename, 'lazyConnect', `fail to connect to remote server: ${serverId}`);
                logger.error(`[dreamix-rpc] mailbox fail to connect to remote server: ${serverId}`);
                if (self.mailboxes[serverId]) {
                    delete self.mailboxes[serverId];
                }
                self.emit('error', constants.RPC_ERROR.FAIL_CONNECT_SERVER, tracer, serverId, null, self.opts);
                return;
            }
            mailbox.on('close', (id) => {
                const mbox = self.mailboxes[id];
                if (mbox) {
                    mbox.close();
                    delete self.mailboxes[id];
                }
                self.emit('close', id);
            });
            delete self.connecting[serverId];
            flushPending(tracer, self, serverId);
        });
    }
}

/**
 * Mail station factory function.
 *
 * @param  {Object} opts construct paramters
 *           opts.servers {Object} global server info map. {serverType: [{id, host, port, ...}, ...]}
 *           opts.mailboxFactory {Function} mailbox factory function
 * @return {Object}      mail station instance
 */
module.exports.create = opts => new MailStation(opts || {});
