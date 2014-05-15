var mysql   = require('mysql'),
    express = require('express'),
    _ = require('underscore')
    settings = require('./settings.js')
    sql = require('./sql.js');

// Connection Info
var dbConnectionInfo = settings.db;

// Server Info
var PORT = settings.port;

var app = express();
var db = mysql.createConnection(dbConnectionInfo);

db.connect(function(err) {
  if (err) {
    console.log(err);
  } else {
    console.log('Connected to db:' + dbConnectionInfo.host);
  }
});

var clearTempTable = function() {
  db.query(sql.drop_temp_table(), function() {});
};

var getObjectDetails = function(err, res, req) {
  if (err) {
    res.send(err);
  }

  var results = [];
  var current_id = null;
  var current_object = null;

  var query = db.query(sql.get_matching_objects());

  query
    .on('error', function(e) { res.send(e); })
    .on('result', function(row) {
      db.pause();

      if (row.object_id !== current_id && current_object) {
        results.push(current_object);
        current_object = {
          object_id: row.object_id,
          object_type: row.object_type
        };
        current_id = row.object_id;
      } else if(row.object_id !== current_id) {
        current_object = {
          object_id: row.object_id,
          object_type: row.object_type
        };
        current_id = row.object_id;
      }

      current_object[row['field']] = row['val'];

      db.resume();
    })
    .on('end', function() {
      results.push(current_object);

      var fullUrl = req.protocol + '://' + req.get('host') + '/';

      _.each(results, function(row) {
        if(_.isEmpty(row)) {
          return;
        }
        row.links = {};

        _.each(row, function(val, col) {
          if (col == 'links' || col == 'object_id' || col == 'object_type') {
            return;
          }

          row.links[col] = fullUrl + row['object_type'] + '?' + col + '=' + val;
        });

        row.url = fullUrl + 'object/' + row.object_id;
      });

      res.json({results:results});
    });
};

app.get('/', function(req, res) {
  db.query(sql.get_object_types(), function(err, rows) {

    if (err) {
      console.log(err);
      return false;
    }
    
    var fullUrl = req.protocol + '://' + req.get('host') + '/';

    _.each(rows, function(row) {
      row.url = fullUrl + row.object_type + '/';
    });

    res.json(rows);
  });
});

app.get('/object/:id', function(req, res) {
  clearTempTable();
  db.query(sql.create_temp_from_id(req.params.id), function(err) {
    getObjectDetails(err, res, req);
  });
});

app.get('/:type', function(req, res) {
  clearTempTable();

  var query = sql.create_temp_from_query(req.params.type, req.query);
  db.query(query, function(err) {
    if (err) {
      console.log(err);
      console.log(query);
    }
    
    getObjectDetails(err, res, req);
  });
});

app.listen(PORT);
console.log('Listening on ' + PORT);
