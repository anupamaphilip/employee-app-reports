var createError = require('http-errors');
var express = require('express');
var path = require('path');
var cookieParser = require('cookie-parser');
var logger = require('morgan');
var XlsxTemplate = require('xlsx-template'),
fs = require('fs'),
path = require('path');
var Q = require("q");
var Promise = require("promise");

var indexRouter = require('./routes/routes');
var usersRouter = require('./routes/users');

var app = express();

//var port = 3001;

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

app.use(logger('dev'));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

app.use('/', indexRouter);
app.use('/users', usersRouter);

// catch 404 and forward to error handler
app.use(function(req, res, next) {
  next(createError(404));
});

// error handler
app.use(function(err, req, res, next) {
  // set locals, only providing error in development
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};

  // render the error page
  res.status(err.status || 500);
  res.render('error');
});

var timeZone = "Australia/Sydney";
var CronJob = require('cron').CronJob;
var job = new CronJob('30 10 * * *', function() {
	  console.log("CRON: Starting cron job to copy data from RedShift to MySQL");
	  var now1 = new Date();
	  console.log(now1);
	  utilsRedshift.data()
	  	.then(function(result) {
	      console.log("CRON: Completed cron job");
	      var now = new Date();
	  	  console.log(now);
	      //res.send(result);
	    });

  }, function () {
    console.log("CRON: cron job done")
  },
  true, /* Start the job right now */
  timeZone /* Time zone of this job. */
);

// app.listen(port, function(req,res) {
//     console.log("Server listening on port " + port);
// });

module.exports = app;
