var mysql   = require('mysql'),
    express = require('express'),
    _ = require('underscore')
    settings = require('./settings.js');

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
    console.log('Connected to ' + dbConnectionInfo.host);
  }
});

var clearTempTable = function() {
  db.query('DROP TEMPORARY TABLE matching_object_ids', function() {});
};

var getObjectDetails = function(err, res, req) {
  if (err) {
    res.send(err);
  }

  var results = [];
  var current_id = null;
  var current_object = null;

  var query = db.query('SELECT i.*, object_type.string AS object_type, name.string AS field, value.string AS val FROM matching_object_ids AS i LEFT JOIN objects AS O ON i.object_id = O.object_id LEFT JOIN string_dim AS object_type ON O.type_key = object_type.string_id LEFT JOIN property_bridge AS pb ON O.object_id = pb.object_key LEFT JOIN property_dim AS pd ON pb.property_key = pd.property_id LEFT JOIN string_dim AS name ON pd.name_key = name.string_id LEFT JOIN string_dim AS value ON pd.value_key = value.string_id');

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
  db.query('SELECT DISTINCT O.type_key, type.string AS object_type FROM objects AS O LEFT JOIN string_dim AS type ON O.type_key = type.string_id', function(err, rows) {

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
  db.query('CREATE TEMPORARY TABLE matching_object_ids SELECT object_id FROM objects AS O WHERE object_id = ' + req.params.id, function(err) {
    getObjectDetails(err, res, req);
  });
});

app.get('/:type', function(req, res) {
  clearTempTable();
  var where = [];
  var sql = "CREATE TEMPORARY TABLE matching_object_ids SELECT object_id FROM objects AS O " +
    "LEFT JOIN string_dim AS object_type ON O.type_key = object_type.string_id ";

  where.push(" WHERE object_type.string = '" + req.params.type + "'");

  _.each(req.query, function(val, key) {
    var pb_table = '`pb' + key + '`';
    var pd_table = '`pd' + key + '`';
    var field_table = '`name' + key + '`';
    var value_table = '`value' + key + '`';
    sql += " LEFT JOIN property_bridge AS " + pb_table + " ON O.object_id = " + pb_table + ".object_key LEFT JOIN property_dim AS " + pd_table + " ON " + pb_table + ".property_key = " + pd_table + ".property_id LEFT JOIN string_dim AS " + field_table + " ON " + pd_table + ".name_key = " + field_table + ".string_id "

    sql += " LEFT JOIN string_dim AS " + value_table + " ON " + pd_table + ".value_key = " + value_table + ".string_id";
    where.push(field_table + ".string = '" + key + "'");
    where.push(value_table + ".string = '" + val + "'");
  });

  sql += where.join(' AND ');

  db.query(sql, function(err) {
    if (err) {
      console.log(err);
      console.log(sql);
    }
    
    getObjectDetails(err, res, req);
  });
});

app.listen(PORT);
console.log('Listening on ' + PORT);
