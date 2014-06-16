var mysql = require('mysql'),
    _     = require('underscore'),
    settings = require('./settings.js'),
    sql      = require('./sql.js');

var dbConnectionInfo = settings.db;
var db;

var handleConnection = function(err) {
  if (err) {
    console.log('db error', err);

    if(err.code === 'PROTOCOL_CONNECTION_LOST') {
      setTimeout(connect, 2000);
    }
  } else {
    console.log('Connected to db: ' + dbConnectionInfo.host);
  }
};

var connect = function() {
  db = mysql.createConnection(dbConnectionInfo);
  db.connect(handleConnection);
};

connect();

var clearTempTable = function() {
  db.query(sql.drop_temp_table());
};

var buildOptions = function(params) {
  var options = {};

  _.each(params, function(val, key) {
    if(key.substr(0, 1) == '_') {
      key = key.substr(1);
      options[key] = val;
    }
  });

  return options;
};

var buildIncludeColumns = function(colString) {
  if(_.isEmpty(colString)) {
    return null;
  }
  var colArray = colString.split(',');
  var cols = {};
  _.each(colArray, function(col) {
    cols[col] = true;
  });

  return cols;
};

var buildLinks = function(baseUrl, row) {
  var links = {};
  _.each(row, function(val, col) {
    if (col == 'links' || col == 'object_id' || col == 'object_type') {
      return;
    }

    links[col] = baseUrl + row['object_type'] + '/?' + col + '=' + val;
  });
};

var includeColumn = function(cols, field) {
  if(_.isEmpty(cols) || cols['*']) {
    return true;
  }
  return cols[field];
};

var getObjectDetails = function(err, res, req) {
  if (err) {
    res.send(err);
    return;
  }

  var results = [];
  var current_id = null;
  var current_object = null;

  var query = db.query(sql.get_matching_objects());
  
  query
    .on('error', function(e) { res.send(e); })
    .on('result', function(row) {
      db.pause();

      if (row.object_id !== current_id) {
        if(current_object) {
          results.push(current_object);
        }

        current_object = {
          object_id: row.object_id,
          object_type: row.object_type
        };
        current_id = row.object_id;
      }

      if (includeColumn(req.cols, row['field'])) {
        current_object[row['field']] = row['val'];
      }

      db.resume();
    })
    .on('end', function() {
      results.push(current_object);

      var baseUrl = req.protocol + '://' + req.get('host') + '/';

      _.each(results, function(row) {
        if(_.isEmpty(row)) {
          return;
        }

        if (req.links) {
          row.links = buildLinks(baseUrl, row);
        }

        row.object_url = baseUrl + 'object/' + row.object_id;
      });

      res.json({results: results});
    });
};

exports.getObjectTypes = function(req, res) {
  db.query(sql.get_object_types(), function(err, rows) {
    if(err) {
      console.log(err);
      return false;
    }

    var baseUrl = req.protocol + '://' + req.get('host') + '/';

    _.each(rows, function(row) {
      row.url = baseUrl + row.object_type + '/';
    });

    res.json(rows);
  });
};

exports.getObject = function(req, res) {
  clearTempTable();

  db.query(sql.create_temp_from_id(req.params.id), function(err) {
    getObjectDetails(err, res, req);
  });
};

exports.getObjects = function(req, res) {
  clearTempTable();

  var links = false;

  if (req.query.hasOwnProperty('links')) {
    req.links = req.query.links;

    if (req.links == 'false') {
      req.links = false;
    }

    delete req.query.links;
  }

  req.options = buildOptions(req.query);
  req.cols = buildIncludeColumns(req.options.return);

  var query = sql.create_temp_from_query(req.params.type, req.query, req.options);

  db.query(query, function(err) {
    if(err) {
      console.log(err);
      console.log(query);
    }

    getObjectDetails(err, res, req);
  });
};

