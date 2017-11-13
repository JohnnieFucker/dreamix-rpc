const EventEmitter = require('events');
const utils = require('../../util/utils');
const wsClient = require('ws');
const zlib = require('zlib');
const Tracer = require('../../util/tracer');
const logger = require('dreamix-logger').getLogger('dreamix-rpc', __filename);

const DEFAULT_CALLBACK_TIMEOUT = 25 * 1000;
const DEFAULT_INTERVAL = 50;

const KEEP_ALIVE_TIMEOUT = 10 * 1000;
const KEEP_ALIVE_INTERVAL = 30 * 1000;

let DEFAULT_ZIP_LENGTH = 1024 * 4;
const DEFAULT_USE_ZIP = false;
let useZipCompress = false;


function checkKeepAlive(mailbox) {
    if (mailbox.closed) {
        return;
    }
    const now = Date.now();
    if (mailbox._KP_last_ping_time > 0) {
        // 当pong的时间小于ping，说明pong还没有回来
        if (mailbox._KP_last_pong_time < mailbox._KP_last_ping_time) {
            // 判断是否超时
            if (now - mailbox._KP_last_ping_time > KEEP_ALIVE_TIMEOUT) {
                logger.error('ws rpc client checkKeepAlive error because > KEEP_ALIVE_TIMEOUT');
                mailbox.close();
                return;
            }

            // 还未超时，但是pong还没有回来，需要等下次timer再检查
            return;
        }
        if (mailbox._KP_last_pong_time > mailbox._KP_last_ping_time) {
            // 当pong的时间大于ping，就说明对端还活着，就认为正常
            mailbox.socket.ping();
            mailbox._KP_last_ping_time = Date.now();
        }
    } else {
        // 当一个ping都未发的时候，发一个ping
        mailbox.socket.ping();
        mailbox._KP_last_ping_time = Date.now();
    }
}

function enqueue(mailbox, msg) {
    mailbox.queue.push(msg);
}

function doSend(socket, dataObj) {
    const str = JSON.stringify({ body: dataObj });
    logger.debug(`websocket_mailbox_doSend: ${str.substring(0, 300)}`);
    logger.debug(`websocket_mailbox_doSend:str len = ${str.length}`);
    if (useZipCompress && str.length > DEFAULT_ZIP_LENGTH) { // send zip binary
        process.nextTick(() => {
            zlib.gzip(str, (err, result) => {
                if (err) {
                    logger.warn('ws rpc send message error: %j', err.stack);
                    socket.send(str);
                    return;
                }
                logger.debug(`ws rpc client send message by zip compress, buffer len = ${result.length}`);
                socket.send(result);
            });
        });
    } else { // send normal text
        logger.debug(`ws rpc client send message, len = ${str.length}`);
        socket.send(str);
    }
}

function flush(mailbox) {
    if (mailbox.closed || !mailbox.queue.length) {
        return;
    }
    doSend(mailbox.socket, mailbox.queue);
    mailbox.queue = [];
}

function clearCbTimeout(mailbox, id) {
    if (!mailbox.timeout[id]) {
        logger.warn('timer is not exsits, id: %s, host: %s, port: %s', id, mailbox.host, mailbox.port);
        return;
    }
    clearTimeout(mailbox.timeout[id]);
    delete mailbox.timeout[id];
}

function setCbTimeout(mailbox, id, tracer, cb) {
    mailbox.timeout[id] = setTimeout(() => {
        logger.warn('rpc request is timeout, id: %s, host: %s, port: %s', id, mailbox.host, mailbox.port);
        clearCbTimeout(mailbox, id);
        if (mailbox.requests[id]) {
            delete mailbox.requests[id];
        }
        logger.error('rpc callback timeout, remote server host: %s, port: %s', mailbox.host, mailbox.port);
        utils.invokeCallback(cb, tracer, new Error('rpc callback timeout'));
    }, mailbox.timeoutValue);
}

function processMsg(mailbox, pkg) {
    clearCbTimeout(mailbox, pkg.id);
    const cb = mailbox.requests[pkg.id];
    if (!cb) {
        return;
    }
    delete mailbox.requests[pkg.id];

    const tracer = new Tracer(mailbox.opts.rpcLogger, mailbox.opts.rpcDebugLog, mailbox.opts.clientId, pkg.source, pkg.resp, pkg.traceId, pkg.seqId);
    const args = [tracer, null];

    pkg.resp.forEach((arg) => {
        args.push(arg);
    });

    cb(...args);
}

function processMsgs(mailbox, pkgs) {
    for (let i = 0, l = pkgs.length; i < l; i++) {
        processMsg(mailbox, pkgs[i]);
    }
}


class MailBox extends EventEmitter {
    constructor(server, opts) {
        super();
        this.id = server.id;
        this.host = server.host;
        this.port = server.port;
        this.requests = {};
        this.timeout = {};
        this.curId = 0;
        this.queue = [];
        this.bufferMsg = opts.bufferMsg;
        this.interval = opts.interval || DEFAULT_INTERVAL;
        this.timeoutValue = opts.timeout || DEFAULT_CALLBACK_TIMEOUT;
        this.connected = false;
        this.closed = false;
        this.opts = opts;
        this._KPinterval = null;
        this._KP_last_ping_time = -1;
        this._KP_last_pong_time = -1;
        DEFAULT_ZIP_LENGTH = opts.doZipLength || DEFAULT_ZIP_LENGTH;
        useZipCompress = opts.useZipCompress || DEFAULT_USE_ZIP;
    }

    connect(tracer, cb) {
        tracer.info('client', __filename, 'connect', 'ws-mailbox try to connect');
        if (this.connected) {
            tracer.error('client', __filename, 'connect', 'mailbox has already connected');
            utils.invokeCallback(cb, new Error('mailbox has already connected.'));
            return;
        }

        this.socket = wsClient.connect(`ws://${this.host}:${this.port}`);
        // this.socket = wsClient.connect(this.host + ':' + this.port, {'force new connection': true, 'reconnect': false});

        const self = this;
        this.socket.on('message', async (data, flags) => {
            try {
                let result;
                if (flags.binary) {
                    logger.debug('ws rpc received message flags.binary, len = ', data.length);
                    result = await zlib.gunzipSync(data);
                    logger.debug(`ws rpc client received message unzip len = ${result.length}`);
                    logger.debug(`websocket_mailbox_recv_zip: ${result.substring(0, 300)}`);
                } else {
                    logger.debug(`ws rpc client received message = ${data}`);
                    logger.debug(`ws rpc client received message len = ${data.length}`);
                    logger.debug('websocket_mailbox_recv_normal: length=%s, ', data.length, data.substring(0, 300));
                    result = data;
                }
                if (result.length > 0 && result[result.length - 1] !== '}') {
                    logger.warn('websocket_mailbox_recv_zip: binary last word is not } ');
                    result = result.substring(0, result.length - 1);
                }
                const msg = JSON.parse(result);
                if (Array.isArray(msg.body)) {
                    processMsgs(self, msg.body);
                } else {
                    processMsg(self, msg.body);
                }
            } catch (e) {
                logger.error('ws rpc client process message with error: %j', e.stack);
                logger.error(data);
            }
        });

        this.socket.on('open', () => {
            if (self.connected) {
                // ignore reconnect
                return;
            }
            // success to connect
            self.connected = true;
            if (self.bufferMsg) {
                // start flush interval
                self._interval = setInterval(() => {
                    flush(self);
                }, self.interval);
            }
            self._KPinterval = setInterval(() => {
                checkKeepAlive(self);
            }, KEEP_ALIVE_INTERVAL);
            utils.invokeCallback(cb);
        });

        this.socket.on('error', (err) => {
            utils.invokeCallback(cb, err);
            self.close();
        });

        this.socket.on('close', () => {
            const reqs = self.requests;
            let _cb;
            for (const id in reqs) {
                if (reqs.hasOwnProperty(id)) {
                    _cb = reqs[id];
                    utils.invokeCallback(_cb, new Error('disconnect with remote server.'));
                }
            }
            self.emit('close', self.id);
            self.close();
        });

        //  this.socket.on('ping', function (data, flags) {
        //  });
        this.socket.on('pong', (data) => {
            logger.debug('ws received pong: %s', data);
            self._KP_last_pong_time = Date.now();
        });
    }

    /**
     * close mailbox
     */
    close() {
        if (this.closed) {
            return;
        }
        this.closed = true;
        if (this._interval) {
            clearInterval(this._interval);
            this._interval = null;
        }
        if (this._KPinterval) {
            clearInterval(this._KPinterval);
            this._KPinterval = null;
        }
        this.socket.close();
        this._KP_last_ping_time = -1;
        this._KP_last_pong_time = -1;
    }

    /**
     * send message to remote server
     * @param tracer
     * @param msg object {service:"", method:"", args:[]}
     * @param opts object {} attach info to send method
     * @param cb declaration decided by remote interface
     */
    send(tracer, msg, opts, cb) {
        tracer.info('client', __filename, 'send', 'ws-mailbox try to send');
        if (!this.connected) {
            tracer.error('client', __filename, 'send', 'ws-mailbox not init');
            utils.invokeCallback(cb, tracer, new Error('not init.'));
            return;
        }

        if (this.closed) {
            tracer.error('client', __filename, 'send', 'mailbox alread closed');
            utils.invokeCallback(cb, tracer, new Error('mailbox alread closed.'));
            return;
        }

        const id = this.curId++;
        this.requests[id] = cb;
        setCbTimeout(this, id, tracer, cb);

        let pkg;
        if (tracer.isEnabled) {
            pkg = {
                traceId: tracer.id,
                seqId: tracer.seq,
                source: tracer.source,
                remote: tracer.remote,
                id: id,
                msg: msg
            };
        } else {
            pkg = { id: id, msg: msg };
        }
        if (this.bufferMsg) {
            enqueue(this, pkg);
        } else {
            doSend(this.socket, pkg);
            // this.socket.send(JSON.stringify({body: pkg}));
        }
    }
}


/**
 * Factory method to create mailbox
 *
 * @param {Object} server remote server info {id:"", host:"", port:""}
 * @param {Object} opts construct parameters
 *                      opts.bufferMsg {Boolean} msg should be buffered or send immediately.
 *                      opts.interval {Boolean} msg queue flush interval if bufferMsg is true. default is 50 ms
 */
module.exports.create = (server, opts) => new MailBox(server, opts || {});
