var express = require('express'),
    settings = require('./settings.js'),
    API      = require('./api.js');

// Server Info
var PORT = settings.port;

var app = express();

app.get('/',           API.getObjectTypes);
app.get('/favicon.ico', function() {return '';});
app.get('/object/:id', API.getObject);
app.get('/:type',      API.getObjects);

app.listen(PORT);
console.log('Listening on ' + PORT);
