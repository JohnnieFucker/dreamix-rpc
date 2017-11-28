const logger = require('dreamix-logger').getLogger('dreamix-rpc', __filename);

const exp = module.exports;

/**
 * Generate prxoy for function type field
 *
 * @param serviceName {String} delegated service name
 * @param methodName {String} delegated method name
 * @param origin {Object} origin object
 * @param attach {Object} attach object
 * @param proxyCB {Function} proxy callback function
 * @returns function proxy
 */
function genFunctionProxy(serviceName, methodName, origin, attach, proxyCB) {
    // logger.debug('rpc proxy genFunctionProxy serviceName : %s,methodName :%s', serviceName, methodName);
    return (() => {
        const proxy = (...args) => {
            logger.debug('rpc proxy args : %j', args);
            proxyCB(serviceName, methodName, args, attach);
        };
        proxy.toServer = (...args) => {
            logger.debug('rpc proxy toServer  args : %j', args);
            proxyCB(serviceName, methodName, args, attach, true);
        };
        return proxy;
    })();
}

function genObjectProxy(serviceName, origin, attach, proxyCB) {
    // generate proxy for function field
    const res = {};
    for (const field in origin) {
        if (origin.hasOwnProperty(field)) {
            if (typeof origin[field] === 'function') {
                res[field] = genFunctionProxy(serviceName, field, origin, attach, proxyCB);
            }
        }
    }
    // logger.debug('rpc proxy genObjectProxy  : %j', res);
    return res;
}

/**
 * Create proxy.
 *
 * @param  {Object} opts construct parameters
 *           opts.origin {Object} delegated object
 *           opts.proxyCB {Function} proxy invoke callback
 *           opts.service {String} deletgated service name
 *           opts.attach {Object} attach parameter pass to proxyCB
 * @return {Object}      proxy instance
 */
exp.create = (opts) => {
    if (!opts || !opts.origin) {
        logger.warn('opts and opts.origin should not be empty.');
        return null;
    }

    if (!opts.proxyCB || typeof opts.proxyCB !== 'function') {
        logger.warn('opts.proxyCB is not a function, return the origin module directly.');
        return opts.origin;
    }

    return genObjectProxy(opts.service, opts.origin, opts.attach, opts.proxyCB);
};

