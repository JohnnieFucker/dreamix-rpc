const Loader = require('dreamix-loader');
const Gateway = require('./gateway');
const logger = require('dreamix-logger').getLogger('dreamix-rpc', __filename);

function createNamespace(namespace, proxies) {
    proxies[namespace] = proxies[namespace] || {};
}

function loadRemoteServices(paths, context) {
    const res = {};
    let item;
    let m;
    for (let i = 0, l = paths.length; i < l; i++) {
        item = paths[i];
        m = Loader.load(item.path, context);
        if (m) {
            createNamespace(item.namespace, res);
            for (const s in m) {
                if (m.hasOwnProperty(s)) {
                    res[item.namespace][s] = m[s];
                }
            }
        }
    }

    return res;
}


/**
 * Create rpc server.
 *
 * @param opts {Object} init parameters
 *    opts.port {Number|String}: rpc server listen port
 *    opts.paths {Array}: remote service code paths, [{namespace, path}, ...]
 *    opts.acceptorFactory {Object}: acceptorFactory.create(opts, cb)
 */
/**
 * Create rpc server.
 *
 * @param  {Object} opts construct parameters
 *                       opts.port {Number|String} rpc server listen port
 *                       opts.paths {Array} remote service code paths, [{namespace, path}, ...]
 *                       opts.context {Object} context for remote service
 *                       opts.acceptorFactory {Object} (optionals)acceptorFactory.create(opts, cb)
 * @return {Object}      rpc server instance
 */
module.exports.create = (opts) => {
    if (!opts || !opts.port || opts.port < 0 || !opts.paths) {
        throw new Error('opts.port or opts.paths invalid.');
    }
    logger.debug('rpc server create opts %j', opts);
    opts.services = loadRemoteServices(opts.paths, opts.context);
    return Gateway.create(opts);
};


module.exports.WebSocketAcceptor = require('./acceptors/websocket-acceptor');
