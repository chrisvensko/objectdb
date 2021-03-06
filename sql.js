var _ = require('underscore');

exports.drop_temp_table = function() {
  return "DROP TEMPORARY TABLE IF EXISTS matching_object_ids";
};

exports.get_matching_objects = function() {
  return "SELECT"
    + " i.*"
    + ", object_type.string AS object_type"
    + ", name.string AS field"
    + ", value.string AS val"
    + " FROM matching_object_ids AS i"
    + " LEFT JOIN objects AS O ON i.object_id = O.object_id"
    + " LEFT JOIN string_dim AS object_type ON O.type_key = object_type.string_id"
    + " LEFT JOIN property_bridge AS pb ON O.object_id = pb.object_key"
    + " LEFT JOIN property_dim AS pd ON pb.property_key = pd.property_id"
    + " LEFT JOIN string_dim AS name ON pd.name_key = name.string_id"
    + " LEFT JOIN string_dim AS value ON pd.value_key = value.string_id";
};

exports.get_object_types = function() {
  return "SELECT"
    + " DISTINCT O.type_key"
    + ", type.string AS object_type"
    + " FROM objects AS O"
    + " LEFT JOIN string_dim AS type ON O.type_key = type.string_id";
};

exports.create_temp_from_id = function(id) {
  return "CREATE TEMPORARY TABLE matching_object_ids"
    + " SELECT object_id"
    + " FROM objects AS O"
    + " WHERE object_id = '" + id + "'";
};

var build_where = function(col, val) {
  var operator = '=';

  if (val.indexOf('*') !== -1 || val.indexOf('.') !== -1) {
    val = val.replace(/\*/g, '%').replace(/\./g, '_');
    operator = 'LIKE';
  } else if(val.indexOf('>') === 0) {
    val = val.substr(1);
    operator = '>';
  }

  return col + " " + operator + " '" + val + "'";
};

var joinField = function(key, val, SQL) {
    SQL.fields[key] = true;
    var pb_table = '`pb' + key + '`';
    var pd_table = '`pd' + key + '`';
    var field_table = '`name' + key + '`';
    var value_table = '`value' + key + '`';

    SQL.joins.push('LEFT JOIN property_bridge AS ' + pb_table + ' ON O.object_id = ' + pb_table + '.object_key');
    SQL.joins.push('LEFT JOIN property_dim AS ' + pd_table + ' ON ' + pb_table + '.property_key = ' + pd_table + '.property_id');
    SQL.joins.push('LEFT JOIN string_dim AS ' + field_table + ' ON ' + pd_table + '.name_key = ' + field_table + '.string_id');
    SQL.joins.push('LEFT JOIN string_dim AS ' + value_table + ' ON ' + pd_table + '.value_key = ' + value_table + '.string_id');


    SQL.where.push(build_where(field_table + '.string', key));

    if (!_.isEmpty(val)) {
      SQL.where.push(build_where(value_table + '.string', val));
    }

    return SQL;
};

var buildSort = function(sortString, SQL) {
  var sortParts = sortString.split(',');
  _.each(sortParts, function(val) {
    // Split sort into col and dir
    var segments = val.split(' ');
    if (!SQL.fields[segments[0]]) {
      SQL = joinField(segments[0], null, SQL);
    }
    var sort = '`value' + segments[0] + '`.string';

    if (segments.length > 1) {
      sort += ' ' + segments[1];
    }
    SQL.order_by.push(sort);
  });

  return SQL;
};

exports.create_temp_from_query = function(type, filters) {
  var SQL = {
    fields: {},
    where: [],
    joins: [],
    order_by: []
  };

  var sql = "CREATE TEMPORARY TABLE matching_object_ids"
    + " SELECT object_id FROM objects AS O"
    + " LEFT JOIN string_dim AS object_type ON O.type_key = object_type.string_id";

  SQL.where.push(" WHERE object_type.string = '" + type + "'");

  _.each(filters, function(val, key) {
    if(key.substr(0,1) == '_') {
      return;
    }
    joinField(key, val, SQL);
  });

  if (!_.isEmpty(filters['_sort'])) {
    SQL = buildSort(filters['_sort'], SQL);
  }

  sql += ' ' + SQL.joins.join(' ');
  sql += SQL.where.join(' AND ');

  if(!_.isEmpty(SQL.order_by)) {
    sql += ' ORDER BY ' + SQL.order_by.join(', ');
  }
  return sql;
};
