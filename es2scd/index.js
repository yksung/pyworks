var express = require('express');
var morgan = require('morgan');
var logger = require('./lib/logger');
var PythonShell = require('python-shell');


var app = express();
app.set('port', 8081);
app.use(morgan('combined', {'stream': logger.stream}));

app.get('/health', function(req, res) {
    res.send('' + workerCnt);
});

app.get('/es2scd', function(req, res) {
    //var msg = 'retroactive||' + req.query.user_seq + '||' + req.query.item_grp_seq + '||' + req.query.item_seq + '||' + req.query.start_dt + '||' + req.query.end_dt;
	var callback = function(err, rets){
		if(err){			
			logger.error("error");
		}else{
			logger.info("good end");
		}
	}
    res.send(es2scd(callback));
});

var server = app.listen(app.get('port'), function() {
    var host = server.address().address;
    var port = server.address().port;

    logger.info('Server Listening on port %d', port);
});


function es2scd(callback){
	var pyOptions = {
            mode : 'text',
            pythonPath : '',
            scriptPath :  '.\\src\\com\\wisenut',
            args : ["E:\\data\\dmap-data\\scd", ]
    };

	PythonShell.run('es2scd.py', pyOptions, function(err, rets) {
        if(err) {
        	//----- OSError 발생시 ES 전반적인 커넥션 문제 발생 60초 sleep
            if (err.errno == 99){// Error: OSError: [Errno 99] Cannot assign requested address
            	logger.error('[topicMaker] getRelatedWords ' + err);
                sleep.sleep(60);
            }
            
            console.log("Python is not called.")
            console.log(err);
            callback(err, null);
        } else {
        	//logger.debug(rets);
        	console.log("Start python module successfully.");
        	callback(null, rets);
        }
	});
	
	return "Just started python module";
}