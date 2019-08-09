const EventEmitter = require('events');
const defaultAcceptorFactory = require('./acceptor');
const Dispatcher = require('./dispatcher');
const fs = require('fs');
const Loader = require('dreamix-loader');


function createNamespace(namespace, proxies) {
    proxies[namespace] = proxies[namespace] || {};
}

function watchServices(gateway, dispatcher) {
    const paths = gateway.opts.paths;
    const app = gateway.opts.context;
    for (let i = 0; i < paths.length; i++) {
        ((index) => {
            fs.watch(paths[index].path, (event) => {
                if (event === 'change') {
                    const res = dispatcher.services || {};
                    const item = paths[index];
                    const m = Loader.load(item.path, app);
                    if (m) {
                        createNamespace(item.namespace, res);
                        for (const s in m) {
                            if (m.hasOwnProperty(s)) {
                                res[item.namespace][s] = m[s];
                            }
                        }
                    }
                    dispatcher.emit('reload', res);
                }
            });
        })(i);
    }
}

class Gateway extends EventEmitter {
    constructor(opts) {
        super();
        this.opts = opts || {};
        this.port = opts.port || 3050;
        this.started = false;
        this.stoped = false;
        this.acceptorFactory = opts.acceptorFactory || defaultAcceptorFactory;
        this.services = opts.services;
        const dispatcher = new Dispatcher(this.services);
        if (this.opts.reloadRemotes) {
            watchServices(this, dispatcher);
        }
        this.acceptor = this.acceptorFactory.create(opts, (tracer, msg, cb) => {
            dispatcher.route(tracer, msg, cb);
        });
    }

    stop() {
        if (!this.started || this.stoped) {
            return;
        }
        this.stoped = true;
        try {
            this.acceptor.close();
        } catch (err) {
        }// eslint-disable-line
    }

    start() {
        if (this.started) {
            throw new Error('gateway already start.');
        }
        this.started = true;

        const self = this;
        this.acceptor.on('error', self.emit.bind(self, 'error'));
        this.acceptor.on('closed', self.emit.bind(self, 'closed'));
        this.acceptor.listen(this.port);
    }
}

/**
 * create and init gateway
 *
 * @param opts object {services: {rpcServices}, connector:conFactory(optional), router:routeFunction(optional)}
 */
module.exports.create = (opts) => {
    if (!opts || !opts.services) {
        throw new Error('opts and opts.services should not be empty.');
    }
    return new Gateway(opts);
};

