const Loader = require('dreamix-loader');
const Proxy = require('../util/proxy');
const Station = require('./mailstation');
const utils = require('../util/utils');
const router = require('./router');
const constants = require('../util/constants');
const Tracer = require('../util/tracer');
const failureProcess = require('./failureProcess');
const logger = require('dreamix-logger').getLogger('dreamix-rpc', __filename);

/**
 * Client states
 */
const STATE_INITED = 1; // client has inited
const STATE_STARTED = 2; // client has started
const STATE_CLOSED = 3; // client has closed

/**
 * Rpc to specified server id or servers.
 *
 * @param client {Object} current client instance.
 * @param msg {Object} rpc message.
 * @param serverType {String} remote server type.
 * @param routeParam {Object} mailbox init context parameter.
 * @param cb {Function} callback.
 * @api private
 */
function rpcToSpecifiedServer(client, msg, serverType, routeParam, cb) {
    if (typeof routeParam !== 'string') {
        logger.error('[dreamix-rpc] server id is not a string, server id: %j', routeParam);
        return;
    }
    if (routeParam === '*') {
        const servers = client._station.servers;
        for (const serverId in servers) {
            if (servers.hasOwnProperty(serverId)) {
                const server = servers[serverId];
                if (server.serverType === serverType) {
                    client.rpcInvoke(serverId, msg, cb);
                }
            }
        }
        return;
    }
    client.rpcInvoke(routeParam, msg, cb);
}

/**
 * Calculate remote target server id for rpc client.
 *
 * @param client {Object} current client instance.
 * @param serverType {String} remote server type.
 * @param msg {Object} remote server type.
 * @param routeParam {Object} mailbox init context parameter.
 * @param cb {Function} return rpc remote target server id.
 *
 * @api private
 */
function getRouteTarget(client, serverType, msg, routeParam, cb) {
    if (client.routerType) {
        let method;
        switch (client.routerType) {
        case constants.SCHEDULE.ROUNDROBIN:
            method = router.rr;
            break;
        case constants.SCHEDULE.WEIGHT_ROUNDROBIN:
            method = router.wrr;
            break;
        case constants.SCHEDULE.LEAST_ACTIVE:
            method = router.la;
            break;
        case constants.SCHEDULE.CONSISTENT_HASH:
            method = router.ch;
            break;
        default:
            method = router.rd;
            break;
        }
        method.call(null, client, serverType, msg, (err, serverId) => {
            utils.invokeCallback(cb, err, serverId);
        });
    } else {
        let route;
        let target;
        if (typeof client.router === 'function') {
            route = client.router;
            target = null;
        } else if (typeof client.router.route === 'function') {
            route = client.router.route;
            target = client.router;
        } else {
            logger.error('[dreamix-rpc] invalid route function.');
            return;
        }
        route.call(target, routeParam, msg, client._routeContext, (err, serverId) => {
            utils.invokeCallback(cb, err, serverId);
        });
    }
}

/**
 * Generate prxoy for function type field
 *
 * @param client {Object} current client instance.
 * @param serviceName {String} delegated service name.
 * @param methodName {String} delegated method name.
 * @param args {Object} rpc invoke arguments.
 * @param attach {Object} attach parameter pass to proxyCB.
 * @param isToSpecifiedServer {boolean} true means rpc route to specified remote server.
 *
 * @api private
 */
function proxyCB(client, serviceName, methodName, args, attach, isToSpecifiedServer) {
    if (client.state !== STATE_STARTED) {
        logger.error('[dreamix-rpc] fail to invoke rpc proxy for client is not running');
        return;
    }
    logger.debug('[dreamix-rpc] proxyCB args %j', args);

    if (args.length < 2) {
        logger.error('[dreamix-rpc] invalid rpc invoke, arguments length less than 2, namespace: %j, serverType, %j, serviceName: %j, methodName: %j',
            attach.namespace, attach.serverType, serviceName, methodName);
        return;
    }
    const routeParam = args.shift();
    const cb = args.pop();
    const serverType = attach.serverType;
    const msg = { namespace: attach.namespace,
        serverType: serverType,
        service: serviceName,
        method: methodName,
        args: args };

    if (isToSpecifiedServer) {
        rpcToSpecifiedServer(client, msg, serverType, routeParam, cb);
    } else {
        getRouteTarget(client, serverType, msg, routeParam, (err, serverId) => {
            if (err) {
                utils.invokeCallback(cb, err);
            } else {
                client.rpcInvoke(serverId, msg, cb);
            }
        });
    }
}

/**
 * Generate proxies for remote servers.
 *
 * @param client {Object} current client instance.
 * @param record {Object} proxy reocrd info. {namespace, serverType, path}
 * @param context {Object} mailbox init context parameter
 *
 * @api private
 */
function generateProxy(client, record, context) {
    if (!record) {
        return false;
    }
    let res;
    let name;
    const modules = Loader.load(record.path, context);
    if (modules) {
        res = {};
        for (name in modules) {
            if (modules.hasOwnProperty(name)) {
                res[name] = Proxy.create({
                    service: name,
                    origin: modules[name],
                    attach: record,
                    proxyCB: proxyCB.bind(null, client)
                });
            }
        }
    }

    logger.debug('client generateProxy results %j', res);
    return res;
}

/**
 * Add proxy into array.
 *
 * @param proxies {Object} rpc proxies
 * @param namespace {String} rpc namespace sys/user
 * @param serverType {String} rpc remote server type
 * @param proxy {Object} rpc proxy
 *
 * @api private
 */
function insertProxy(proxies, namespace, serverType, proxy) {
    proxies[namespace] = proxies[namespace] || {};
    if (proxies[namespace][serverType]) {
        for (const attr in proxy) {
            if (proxy.hasOwnProperty(attr)) {
                proxies[namespace][serverType][attr] = proxy[attr];
            }
        }
    } else proxies[namespace][serverType] = proxy;
}

class Client {
    constructor(opts) {
        opts = opts || {};
        this._context = opts.context;
        this._routeContext = opts.routeContext;
        this.router = opts.router || router.df;
        this.routerType = opts.routerType;
        if (this._context) {
            opts.clientId = this._context.serverId;
        }
        this.opts = opts;
        this.proxies = {};
        this._station = Station.create(opts);
        this.state = STATE_INITED;
    }
    /**
     * Start the rpc client which would try to connect the remote servers and
     * report the result by cb.
     *
     * @param cb {Function} cb(err)
     */
    start(cb) {
        if (this.state > STATE_INITED) {
            utils.invokeCallback(cb, new Error('rpc client has started.'));
            return;
        }

        const self = this;
        this._station.start((err) => {
            if (err) {
                logger.error(`[pomelo-rpc] client start fail for ${err.stack}`);
                utils.invokeCallback(cb, err);
                return;
            }
            self._station.on('error', failureProcess.bind(self._station));
            self.state = STATE_STARTED;
            utils.invokeCallback(cb);
        });
    }
    /**
     * Stop the rpc client.
     *
     * @param  {Boolean} force
     * @return 
     */
    stop(force) {
        if (this.state !== STATE_STARTED) {
            logger.warn('[dreamix-rpc] client is not running now.');
            return;
        }
        this.state = STATE_CLOSED;
        this._station.stop(force);
    }


    /**
     * Add a new proxy to the rpc client which would overrid the proxy under the
     * same key.
     *
     * @param {Object} record proxy description record, format:
     *                        {namespace, serverType, path}
     */
    addProxy(record) {
        if (!record) {
            return;
        }
        const proxy = generateProxy(this, record, this._context);
        logger.debug('rpc client addProxy %j', proxy);
        if (!proxy) {
            return;
        }
        insertProxy(this.proxies, record.namespace, record.serverType, proxy);
    }
    /**
     * Batch version for addProxy.
     *
     * @param {Array} records list of proxy description record
     */
    addProxies(records) {
        logger.debug('rpc client addProxies %j', records);
        if (!records || !records.length) {
            return;
        }
        for (let i = 0, l = records.length; i < l; i++) {
            this.addProxy(records[i]);
        }
    }

    /**
     * Add new remote server to the rpc client.
     *
     * @param {Object} server new server information
     */
    addServer(server) {
        this._station.addServer(server);
    }

    /**
     * Batch version for add new remote server.
     *
     * @param {Array} servers server info list
     */
    addServers(servers) {
        this._station.addServers(servers);
    }

    /**
     * Remove remote server from the rpc client.
     *
     * @param  {String|Number} id server id
     */
    removeServer(id) {
        this._station.removeServer(id);
    }

    /**
     * Batch version for remove remote server.
     *
     * @param  {Array} ids remote server id list
     */
    removeServers(ids) {
        this._station.removeServers(ids);
    }

    /**
     * Replace remote servers.
     *
     * @param {Array} servers server info list
     */
    replaceServers(servers) {
        this._station.replaceServers(servers);
    }

    /**
     * Do the rpc invoke directly.
     *
     * @param serverId {String} remote server id
     * @param msg {Object} rpc message. Message format:
     *    {serverType: serverType, service: serviceName, method: methodName, args: arguments}
     * @param cb {Function} cb(err, ...)
     */
    rpcInvoke(serverId, msg, cb) {
        const tracer = new Tracer(this.opts.rpcLogger, this.opts.rpcDebugLog, this.opts.clientId, serverId, msg);
        tracer.info('client', __filename, 'rpcInvoke', 'the entrance of rpc invoke');
        if (this.state !== STATE_STARTED) {
            tracer.error('client', __filename, 'rpcInvoke', 'fail to do rpc invoke for client is not running');
            logger.error('[dreamix-rpc] fail to do rpc invoke for client is not running');
            cb(new Error('[dreamix-rpc] fail to do rpc invoke for client is not running'));
            return;
        }
        this._station.dispatch(tracer, serverId, msg, this.opts, cb);
    }

    /**
     * Add rpc before filter.
     *
     * @param filter {Function} rpc before filter function.
     *
     * @api public
     */
    before(filter) {
        this._station.before(filter);
    }

    /**
     * Add rpc after filter.
     *
     * @param filter {Function} rpc after filter function.
     *
     * @api public
     */
    after(filter) {
        this._station.after(filter);
    }

    /**
     * Add rpc filter.
     *
     * @param filter {Function} rpc filter function.
     *
     * @api public
     */
    filter(filter) {
        this._station.filter(filter);
    }

    /**
     * Set rpc filter error handler.
     *
     * @param handler {Function} rpc filter error handler function.
     *
     * @api public
     */
    setErrorHandler(handler) {
        this._station.handleError = handler;
    }
}


module.exports.WebSocketMailbox = require('./mailboxes/websocket-mailbox');

/**
 * RPC client factory method.
 *
 * @param  {Object} opts client init parameter.
 *                       opts.context: mail box init parameter,
 *                       opts.router: (optional) rpc message route function, route(routeParam, msg, cb),
 *                       opts.mailBoxFactory: (optional) mail box factory instance.
 * @return {Object}      client instance.
 */
module.exports.create = opts => new Client(opts);

