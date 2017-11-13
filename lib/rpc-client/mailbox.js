/**
 * Default mailbox factory
 */
const Mailbox = require('./mailboxes/websocket-mailbox');

/**
 * default mailbox factory
 *
 * @param {Object} serverInfo single server instance info, {id, host, port, ...}
 * @param {Object} opts construct parameters
 * @return {Object} mailbox instancef
 */
module.exports.create = (serverInfo, opts) => Mailbox.create(serverInfo, opts);
