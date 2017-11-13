const EventEmitter = require('events').EventEmitter;
const utils = require('../../util/utils');
const logger = require('dreamix-logger').getLogger('dreamix-rpc', __filename);

const exp = new EventEmitter();
exp.connect = (tracer, cb) => {
    tracer.debug('client', __filename, 'connect', 'connect to blackhole');
    process.nextTick(() => {
        utils.invokeCallback(cb, new Error('fail to connect to remote server and switch to blackhole.'));
    });
};

exp.close = () => {
};

exp.send = (tracer, msg, opts, cb) => {
    tracer.debug('client', __filename, 'send', 'send rpc msg to blackhole');
    logger.debug('message into blackhole: %j', msg);
    process.nextTick(() => {
        utils.invokeCallback(cb, tracer, new Error('message was forward to blackhole.'));
    });
};

module.exports = exp;
