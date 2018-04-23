var cluster = require('cluster');
var express = require('express');
var morgan = require('morgan');
var moment = require('moment');

var logger = require('./lib/logger');

var DONE = 'done';

if (cluster.isMaster) {
  var prossCnt = 0;

  var maker = cluster.fork();
  var makerBusy = false;
  prossCnt++;
  maker.on('message', function(msg) {
    if (msg === DONE) {
      makerBusy = false;
    } else {
      logger.debug(msg);
      makerBusy = false;
    }
  });

  cluster.on('exit', function(worker, code, signal) {
    prossCnt--;
    logger.error('[app/master] worker ' + worker.process.pid + ' died - code : ' + code + ', signal : ' + signal);
  });

  var app = express();
  app.set('port', 8080);
  app.use(morgan('combined', {
    'stream': logger.stream
  }));
  app.use('/health', function(req, res) {
    var data = {};

    if (prossCnt === 1) {
      data.Result = 'OK';
      data.isBusy = makerBusy;
    } else {
      data.Result = 'ERROR';
      data.isBusy = makerBusy;
      data.Process = prossCnt;
    }

    res.send(data);
  });

  app.get('/report', function(req, res) {

    //현재시간
    var date = new Date();

    var params = {
      reportDay: moment(date).format('YYYYMMDD'),
      reportTime: moment(date).format('HH')
    };

    if (req.query.day !== undefined) {
      params.reportDay = req.query.day;
    }

    if (req.query.time !== undefined) {
      params.reportTime = req.query.time;
    }

    var data = {};
    data.Result = 'OK';
    maker.send(params);

    // if (!makerBusy) {
    //   maker.send(params);
    //   makerBusy = true;
    // } else {
    //   data.Message = 'maker is busy!';
    // }

    res.send(data);
  });

  var server = app.listen(app.get('port'), function() {
    var host = server.address().address;
    var port = server.address().port;

    logger.info('[app][master] Server Listening on port %d', port);
  });
} else {
  var path = require('path');
  var async = require('async');
  var sleep = require('sleep');
  var exec = require('child_process').exec;

  var excel = require('./lib/excel');
  var fsUtil = require('./lib/fsUtil');

  var AWS_BUCKET = 'collector-data';
  var AWS_BACKUP_BUCKET = 'collector-data-backup';
  var AWS_REPORT_PATH = ['collect_data/kdic/'];

  var WEBCRAWLING_RAW_PATH = '/raw';
  var WEBCRAWLING_ATTACH_PATH = '/attach';

  var BIGDATA_TOPICS_PATH = '/topic';
  var BIGDATA_TRENDS_PATH = '/trend';
  var BIGDATA_EMOTION_PATH = '/emotion';

  var BIGDATA_TARGET_TIME = '06';

  var project_seq = [176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 204];

  process.on('message', function(msg) {
    //var execute_date_path = msg.reportDay;
    //var execute_hour_path = msg.reportTime;
    var execute_date_paths = msg.reportDay.split(',');
    var execute_hour_paths = msg.reportTime.split(',');

    async.eachSeries(execute_date_paths, function(execute_date_path, callback) {
      async.eachSeries(execute_hour_paths, function(execute_hour_path, callback) {
        logger.info('[app][maker] Execute Report. ' + execute_date_path + '/' + execute_hour_path);

        // 임시 데이터 경로
        var web_data_temp_path = __dirname + '/save/webcrawling/' + execute_date_path + '/' + execute_hour_path;
        var big_data_temp_path = __dirname + '/save/bigdata/' + execute_date_path;

        // 데이터 경로
        var web_data_path = __dirname + '/report/webcrawling/' + execute_date_path + '/' + execute_hour_path;
        var big_data_path = __dirname + '/report/bigdata/' + execute_date_path;

        //zip파일명
        var web_zip_path = execute_date_path + '' + execute_hour_path + '.zip';
        var big_zip_path = execute_date_path + '.zip';

        logger.debug('>>> Report Maker Init Params');
        logger.debug(web_data_temp_path);
        logger.debug(big_data_temp_path);

        logger.debug(web_data_path + path.sep + web_zip_path);
        logger.debug(big_data_path + path.sep + big_zip_path);

        async.waterfall([
          //Step1. 화제어/연관어/감성분석 생성
          function(next) {
            logger.info('[app][maker] Step1. 빅데이터 생성 (화제어/연관어/감성분석)');
            if (execute_hour_path === BIGDATA_TARGET_TIME) { // 하루에 00시 한번만 수행.
              async.waterfall([
                //Step1-1. 화제어 생성
                function(callback) {
                  logger.info('[app][maker] Step1-1. 화제어 생성');
                  var child = exec(__dirname + '/bin/bigdata/topic_maker.sh ' + project_seq.join(',') + ' ' + big_data_temp_path + BIGDATA_TOPICS_PATH + ' ' + execute_date_path + ' ' + execute_hour_path, {
                    maxBuffer: 1024 * 1024 * 200
                  }, function(err, stdout, stderr) {
                    if (err) {
                      callback(err);
                    } else {
                      logger.info(stdout);
                      sleep.sleep(3); // 3 seconds
                      callback(null);
                    }
                  }); //exec
                  // callback(null);
                },
                //Step1-2. 연관검색어 생성
                function(callback) {
                  logger.info('[app][maker] Step1-2. 연관검색어 생성');
                  var child = exec(__dirname + '/bin/bigdata/trend_maker.sh ' + project_seq.join(',') + ' ' + big_data_temp_path + BIGDATA_TRENDS_PATH + ' ' + execute_date_path + ' ' + execute_hour_path, {
                    maxBuffer: 1024 * 1024 * 200
                  }, function(err, stdout, stderr) {
                    if (err) {
                      callback(err);
                    } else {
                      logger.info(stdout);
                      sleep.sleep(3); // 3 seconds
                      callback(null);
                    }
                  }); //exec
                  // callback(null);
                },
                //Step1-3. 감성분석 생성
                function(callback) {
                  logger.info('[app][maker] Step1-3. 감성분석 생성');
                  var child = exec(__dirname + '/bin/bigdata/emotion_maker.sh ' + project_seq.join(',') + ' ' + big_data_temp_path + BIGDATA_EMOTION_PATH + ' ' + execute_date_path + ' ' + execute_hour_path, {
                    maxBuffer: 1024 * 1024 * 200
                  }, function(err, stdout, stderr) {
                    if (err) {
                      callback(err);
                    } else {
                      logger.info(stdout);
                      sleep.sleep(3); // 3 seconds
                      callback(null);
                    }
                  }); //exec
                  // callback(null);
                }
              ], next);
            } else {
              next(null);
            }
          },
          //Step2. SNS/WEB/첨부파일 생성
          function(next) {
            logger.info('[app][maker] Step2. 웹크롤링 생성 (SNS/WEB/첨부파일)');
            async.waterfall([
              //Step2-1. SNS 원문 생성
              function(callback) {
                logger.info('[app][maker] Step2-1. SNS 원문 생성');
                var child = exec(__dirname + '/bin/webcrawling/doc_maker.sh ' + project_seq.join(',') + ' ' + web_data_temp_path + WEBCRAWLING_RAW_PATH + ' ' + execute_date_path + ' ' + execute_hour_path, {
                  maxBuffer: 1024 * 1024 * 200
                }, function(err, stdout, stderr) {
                  if (err) {
                    callback(err);
                  } else {
                    logger.info(stdout);
                    sleep.sleep(3); // 3 seconds
                    callback(null);
                  }
                }); //exec
              },
              //Step2-2. WEB 원문 생성
              function(callback) {
                logger.info('[app][maker] Step2-2. WEB 원문 생성');

                //현재시간
                var date = new Date();

                async.eachSeries(AWS_REPORT_PATH, function(path, callback) {
                  async.waterfall([
                    //Step2-2-1. 오브젝트 리스트 조회
                    function(callback) {
                      if (execute_date_path === moment(date).format('YYYYMMDD') && execute_hour_path === moment(date).format('HH')) {
                        logger.debug('[app][maker] Step2-2-1. 오브젝트 리스트 조회');
                        fsUtil.getObjects(AWS_BUCKET, path, function(err, objects) {
                          async.waterfall([
                            //Step#2-2-2 WEB 원문 다운로드 (AWS > JSON)
                            function(callback) {
                              logger.info('[app][maker] Step2-2-2. WEB 원문 다운로드 (AWS > JSON)');
                              fsUtil.downloadFiles(web_data_temp_path + WEBCRAWLING_RAW_PATH, AWS_BUCKET, objects, ['docCollector', 'linkDocCollector'], callback); // downloadFiles
                              // callback(null);
                            },
                            //Step#2-2-3. WEB 원문 다운로드 (AWS > FILES)
                            function(callback) {
                              logger.info('[app][maker] Step2-2-3. WEB 원문 다운로드 (AWS > FILES)');
                              fsUtil.downloadFiles(web_data_temp_path + WEBCRAWLING_ATTACH_PATH, AWS_BUCKET, objects, ['attachCollector'], callback); // downloadFiles
                              // callback(null);
                            },
                            //Step#2-2-4. 파일 백업
                            function(callback) {
                              logger.info('[app][maker] Step#2-2-4. 파일 백업');
                              fsUtil.moveObjects(AWS_BUCKET, AWS_BACKUP_BUCKET, objects, callback); // moveObjects
                            }
                          ], callback); // waterfall
                        }); // getObjects
                      } else {
                        logger.info('[app][maker] Step2-2-1. 오브젝트 리스트 조회 Skip (소급 리포트)');
                        callback(null);
                      }
                      // callback(null);
                    },
                    //Step#2-2-5. WEB 원문 생성 (JSON > XLSX)
                    function(callback) {
                      logger.info('[app][maker] Step2-2-3. WEB 원문 생성 (JSON > XLSX)');
                      excel.json2excel(web_data_temp_path + WEBCRAWLING_RAW_PATH, web_data_temp_path + WEBCRAWLING_RAW_PATH, execute_date_path, execute_hour_path, callback);
                    }
                  ], callback); // waterfall
                }, callback); // eachSeries
              }
            ], next);
          },
          //Step3. 압축 파일 생성
          function(next) {
            logger.info('[app][maker] Step3. 압축 파일 생성');
            async.waterfall([
              function(callback) {
                logger.info('[app][maker] Step3-1. 웹크롤링 압축 파일 생성 - SNS/WEB/첨부파일');
                var child = exec(__dirname + '/bin/excel_download.sh ' + web_data_temp_path + ' ' + web_data_path + ' ' + web_zip_path, {
                  maxBuffer: 1024 * 1024 * 200
                }, function(err, stdout, stderr) {
                  if (err) {
                    callback(err);
                  } else {
                    logger.info(stdout);
                    sleep.sleep(3); // 3 seconds
                    callback(null);
                  }
                }); //exec
              },
              function(callback) {
                logger.info('[app][maker] Step3-2. 웹크롤링 DM5 파일 생성');
                fsUtil.makeMD5(web_data_path + '/' + web_zip_path, callback);
              },
              function(callback) {
                if (execute_hour_path === BIGDATA_TARGET_TIME) { // 하루에 00시 한번만 수행.
                  logger.info('[app][maker] Step3-3. 빅데이터 압축 파일 생성 - 화제어/연관어/감성분석');
                  var child = exec(__dirname + '/bin/excel_download.sh ' + big_data_temp_path + ' ' + big_data_path + ' ' + big_zip_path, {
                    maxBuffer: 1024 * 1024 * 200
                  }, function(err, stdout, stderr) {
                    if (err) {
                      callback(err);
                    } else {
                      logger.info(stdout);
                      sleep.sleep(3); // 3 seconds
                      callback(null);
                    }
                  }); //exec
                } else {
                  callback(null);
                }
              },
              function(callback) {
                if (execute_hour_path === BIGDATA_TARGET_TIME) { // 하루에 00시 한번만 수행.
                  logger.info('[app][maker] Step3-4. 빅데이터 MD5 파일 생성');
                  fsUtil.makeMD5(big_data_path + '/' + big_zip_path, callback);
                } else {
                  callback(null);
                }
              }
            ], next);
          }
        ], function(err) {
          if (err) {
            logger.error(err);
            logger.info('[app][maker] Execute Hour Process Failed. ' + execute_date_path + '/' + execute_hour_path);
            sleep.sleep(10); // 10*60 seconds
            callback(null);
          } else {
            logger.info('[app][maker] Execute Hour Process Finished. ' + execute_date_path + '/' + execute_hour_path);
            //process.exit();
            sleep.sleep(10); // 10*60 seconds
            callback(null);
          }
        }); // waterfall
      }, callback); // execute hour eachSeries
    }, function done() {
      logger.info('[app][maker] All Process Finished.');
      process.send(DONE);
    }); // execute day eachSeries
  });
}
