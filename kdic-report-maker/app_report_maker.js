var PythonShell = require('python-shell');
var cluster = require('cluster');
var async = require('async');
var sleep = require('sleep');
var express = require('express');
var exec = require('child_process').exec;
var logger = require('./lib/logger');

if (cluster.isMaster) {
    cluster.fork();
    
    cluster.on('exit', function(worker, code, signal) {
        logger.error('worker ' + worker.process.pid + ' died - code : ' + code + ', signal : ' + signal);
    });

    var app = express();
    app.set('port', 9099);
    // app.use(morgan('combined', {'stream': logger.stream}));
    app.get('/health', function(req, res) {
        res.send('OK');
    });

    var server = app.listen(app.get('port'), function() {
        var host = server.address().address;
        var port = server.address().port;

        logger.info('Server Listening on port %d', port);
    });
} else {
    async.forever(
		function(next) {
			/*var pyOptions = {
				mode : 'text',
				pythonPath : '/home/wisenut/anaconda3/bin/python',
				//pythonOptions: ['-u'],
				scriptPath : '/data/dmap-report-maker/'
			};
			PythonShell.run('com/wisenut/excel_maker.py', pyOptions, function(err, results) {
				if(err) {
					logger.error(err);
					next(null);
				}else{
					logger.info(results);
					sleep.sleep(10); // 3 seconds
					next(null);
				}
			}); //run
			var runShell = new run_cmd('/data/dmap-report-maker/report_maker.sh', [], function (err, results){
				if(err){
					logger.error(err);
					next(null);
				}else{
					logger.info(results);
					sleep.sleep(10); // 3 seconds
					next(null);
				}
			});*/
			
			var child = exec('/data/dmap-report-maker/report_maker.sh', {maxBuffer: 1024 * 1024 * 2}, function(err, stdout, stderr) {
				if (err){
					logger.error(err);
					next(null);
				}else{
					logger.info(stdout);
					sleep.sleep(10); // 3 seconds
					next(null);
				}
			});
		},
        function(err) {
            if (err) {
                logger.error(err);
                cluster.worker.kill(-1);
            } else {
                logger.info('Module unexpectedly finished.');
                cluster.worker.kill(0);
            }
        }
    );
}
