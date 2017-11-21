const EventEmitter = require('events');
const utils = require('../../util/utils');
const WebSocket = require('ws');
const zlib = require('zlib');
const Tracer = require('../../util/tracer');
const logger = require('dreamix-logger').getLogger('dreamix-rpc', __filename);

let DEFAULT_ZIP_LENGTH = 1024 * 10;
let useZipCompress = false;

let gid = 1;


function doSend(socket, dataObj) {
    const str = JSON.stringify({ body: dataObj });
    logger.debug(`websocket_acceptor_doSend: ${str.substring(0, 300)}`);
    logger.debug(`websocket_acceptor_doSend: str len = ${str.length}`);
    if (useZipCompress && str.length > DEFAULT_ZIP_LENGTH) { // send zip binary
        process.nextTick(() => {
            zlib.gzip(str, (err, result) => {
                if (err) {
                    logger.warn('ws rpc server send message error: %j', err.stack);
                    socket.send(str);
                    return;
                }
                logger.debug(`ws rpc server send message by zip compress, buffer len = ${result.length}`);
                socket.send(result);
            });
        });
    } else {
        logger.debug(`ws rpc server send message, len = ${str.length}`);
        socket.send(str);
    }
}

function flush(acceptor) {
    const sockets = acceptor.sockets;
    const queues = acceptor.msgQueues;
    let queue;
    let socket;
    for (const socketId in queues) {
        if (queues.hasOwnProperty(socketId)) {
            socket = sockets[socketId];
            if (!socket) {
                // clear pending messages if the socket not exist any more
                delete queues[socketId];
            } else {
                queue = queues[socketId];
                if (queue.length) {
                    doSend(socket, queue);
                    //    socket.send(JSON.stringify({body: queue}));
                    queues[socketId] = [];
                }
            }
        }
    }
}

function ipFilter(obj) {
    if (typeof this.whitelist === 'function') {
        const self = this;
        self.whitelist((err, tmpList) => {
            if (err) {
                logger.error('%j.(RPC whitelist).', err);
                return;
            }
            if (!Array.isArray(tmpList)) {
                logger.error('%j is not an array.(RPC whitelist).', tmpList);
                return;
            }
            if (!!obj && !!obj.ip && !!obj.id) {
                for (const i in tmpList) {
                    if (tmpList.hasOwnProperty(i)) {
                        const exp = new RegExp(tmpList[i]);
                        if (exp.test(obj.ip)) {
                            return;
                        }
                    }
                }
                const sock = self.sockets[obj.id];
                if (sock) {
                    sock.close();
                    logger.warn('%s is rejected(RPC whitelist).', obj.ip);
                }
            }
        });
    }
}
function cloneError(origin) {
    // copy the stack infos for Error instance json result is empty
    return {
        msg: origin.msg,
        stack: origin.stack
    };
}

function enqueue(socket, acceptor, msg) {
    if (!acceptor.msgQueues[socket.id]) {
        acceptor.msgQueues[socket.id] = [];
    }
    acceptor.msgQueues[socket.id].push(msg);
}

function processMsg(socket, acceptor, pkg) {
    const tracer = new Tracer(acceptor.rpcLogger, acceptor.rpcDebugLog, pkg.remote, pkg.source, pkg.msg, pkg.traceId, pkg.seqId);
    tracer.info('server', __filename, 'processMsg', 'ws-acceptor receive message and try to process message');
    acceptor.cb.call(null, tracer, pkg.msg, (...args) => {
        for (let i = 0, l = args.length; i < l; i++) {
            if (args[i] instanceof Error) {
                args[i] = cloneError(args[i]);
            }
        }
        let resp;
        if (tracer.isEnabled) {
            resp = {
                traceId: tracer.id,
                seqId: tracer.seq,
                source: tracer.source,
                id: pkg.id,
                resp: args
            };
        } else {
            resp = { id: pkg.id, resp: args };
        }
        if (acceptor.bufferMsg) {
            enqueue(socket, acceptor, resp);
        } else {
            doSend(socket, resp);
            // socket.send(JSON.stringify({body: resp}));
        }
    });
}
function processMsgs(socket, acceptor, pkgs) {
    for (let i = 0, l = pkgs.length; i < l; i++) {
        processMsg(socket, acceptor, pkgs[i]);
    }
}


class Acceptor extends EventEmitter {
    constructor(opts, cb) {
        super();
        this.bufferMsg = opts.bufferMsg;
        this.interval = opts.interval; // flush interval in ms
        this.rpcDebugLog = opts.rpcDebugLog;
        this.rpcLogger = opts.rpcLogger;
        this.whitelist = opts.whitelist;
        this._interval = null; // interval object
        this.sockets = {};
        this.msgQueues = {};
        this.cb = cb;
        DEFAULT_ZIP_LENGTH = opts.doZipLength || DEFAULT_ZIP_LENGTH;
        useZipCompress = opts.useZipCompress || false;
    }
    listen(port) {
        // check status
        if (this.inited) {
            utils.invokeCallback(this.cb, new Error('already inited.'));
            return;
        }
        this.inited = true;

        const self = this;

        this.server = new WebSocket.Server({ port: port });

        this.server.on('error', (err) => {
            self.emit('error', err);
        });

        this.server.on('connection', (socket) => {
            const id = gid++;
            socket.id = id;
            self.sockets[id] = socket;

            self.emit('connection', { id: id, ip: socket._socket.remoteAddress });

            socket.on('message', async (data) => {
                try {
                    let result;
                    if (data instanceof Buffer) {
                        logger.debug('ws rpc received message flags.binary, len = ', data.length);
                        result = await zlib.gunzipSync(data);
                        logger.debug(`websocket_acceptor_recv_zip: ${result.substring(0, 300)}`);
                        logger.debug(`ws rpc server received message unzip len = ${result.length}`);
                        logger.debug(`ws rpc server received message unzip = ${result}`);
                    } else {
                        logger.debug(`ws rpc server received message = ${data}`);
                        logger.debug('websocket_acceptor_recv_normal: length=%s, ', data.length, data.substring(0, 300));
                        logger.debug(`ws rpc server received message len = ${data.length}`);
                        result = data;
                    }
                    if (result.length > 0 && result[result.length - 1] !== '}') {
                        console.warn('websocket_acceptor_recv: result last word is not } ');
                        result = result.substring(0, result.length - 1);
                    }
                    const msg = JSON.parse(result);
                    if (Array.isArray(msg.body)) {
                        processMsgs(socket, self, msg.body);
                    } else {
                        processMsg(socket, self, msg.body);
                    }
                } catch (e) {
                    logger.error('ws rpc server process message with error: %j', e.stack);
                    logger.error(data);
                }
            });

            socket.on('close', () => {
                delete self.sockets[id];
                delete self.msgQueues[id];
            });
        });

        this.on('connection', ipFilter.bind(this));

        if (this.bufferMsg) {
            this._interval = setInterval(() => {
                flush(self);
            }, this.interval);
        }
    }

    close() {
        if (this.closed) {
            return;
        }
        this.closed = true;
        if (this._interval) {
            clearInterval(this._interval);
            this._interval = null;
        }
        try {
            this.server.close();
        } catch (err) {
            logger.error('rpc server close error: %j', err.stack);
        }
        this.emit('closed');
    }
}

/**
 * create acceptor
 *
 * @param opts init params
 * @param cb(tracer, msg, cb) callback function that would be invoked when new message arrives
 */
module.exports.create = (opts, cb) => new Acceptor(opts || {}, cb);
