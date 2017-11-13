const utils = require('../util/utils');
const EventEmitter = require('events');

class Dispatcher extends EventEmitter {
    constructor(services) {
        super();
        this.services = services;
        const self = this;
        this.on('reload', (newServices) => {
            self.services = newServices;
        });
    }

    /**
     * route the msg to appropriate service object
     * @param tracer
     * @param msg msg package {service:serviceString, method:methodString, args:[]}
     * @param cb(...) callback function that should be invoked as soon as the rpc finished
     */
    route(tracer, msg, cb) {
        tracer.info('server', __filename, 'route', 'route messsage to appropriate service object');
        const namespace = this.services[msg.namespace];
        if (!namespace) {
            tracer.error('server', __filename, 'route', `no such namespace:${msg.namespace}`);
            utils.invokeCallback(cb, new Error(`no such namespace:${msg.namespace}`));
            return;
        }

        const service = namespace[msg.service];
        if (!service) {
            tracer.error('server', __filename, 'route', `no such service:${msg.service}`);
            utils.invokeCallback(cb, new Error(`no such service:${msg.service}`));
            return;
        }

        const method = service[msg.method];
        if (!method) {
            tracer.error('server', __filename, 'route', `no such method:${msg.method}`);
            utils.invokeCallback(cb, new Error(`no such method:${msg.method}`));
            return;
        }

        const args = msg.args.slice(0);
        args.push(cb);
        method.apply(service, args);
    }
}

module.exports = Dispatcher;
