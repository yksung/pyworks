var winston = require('winston');
var moment = require('moment');

var logger = new (winston.Logger)({
    transports:[
        new (require('winston-daily-rotate-file'))({
            level: 'debug',
            filename: 'es2scd',
            dirname: __dirname + '/../logs',
            datePattern: '.yyyy-MM-dd',
            timestamp: function() {
                return moment().format('YYYY-MM-DD HH:mm:ss');
            },
            json: false,
            colorize: true,
            humanReadableUnhandledException: true
        })
    ],
    exceptionHandlers: [
        new (require('winston-daily-rotate-file'))({
            level: 'error',
            filename: 'es2scd',
            dirname: __dirname + '/../logs',
            datePattern: '.yyyy-MM-dd',
            timestamp: function() {
                return moment().format('YYYY-MM-DD HH:mm:ss');
            },
            json: false,
            colorize: true,
            humanReadableUnhandledException: true
        })
    ],
    exitOnError: false
});

module.exports = logger;
module.exports.stream = {
    write: function(message, encoding) {
        logger.info(message);
    }
};
