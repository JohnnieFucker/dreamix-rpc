const uuidv1 = require('uuid/v1');

function getModule(module) {
    let rs = '';
    const strs = module.split('/');
    const lines = strs.slice(-3);
    for (let i = 0; i < lines.length; i++) {
        rs += `/${lines[i]}`;
    }
    return rs;
}

class Tracer {
    constructor(logger, enabledRpcLog, source, remote, msg, id, seq) {
        this.logger = logger;
        this.isEnabled = enabledRpcLog;
        this.source = source;
        this.remote = remote;
        this.id = id || uuidv1();
        this.seq = seq || 1;
        this.msg = msg;
    }
    getLogger(role, module, method, des) {
        return {
            traceId: this.id,
            seq: this.seq++,
            role: role,
            source: this.source,
            remote: this.remote,
            module: getModule(module),
            method: method,
            args: this.msg,
            timestamp: Date.now(),
            description: des
        };
    }
    info(role, module, method, des) {
        if (this.isEnabled) {
            this.logger.info(JSON.stringify(this.getLogger(role, module, method, des)));
        }
    }
    debug(role, module, method, des) {
        if (this.isEnabled) {
            this.logger.debug(JSON.stringify(this.getLogger(role, module, method, des)));
        }
    }
    error(role, module, method, des) {
        if (this.isEnabled) {
            this.logger.error(JSON.stringify(this.getLogger(role, module, method, des)));
        }
    }
}
module.exports = Tracer;
