process.title = 'dasfest_measurements_emitter';
// Initialization 
// Config
const config = JSON.parse(Buffer.from(require('./config.js'), 'base64').toString());

// Settings
var broker = config.globalsettings.broker;
var logtopic = config.globalsettings.logtopic;
var listentopic = config.mynodeid;
var previousnode = config.previousnode;
var nextnode = config.nextnode;
var mynodeid = config.mynodeid;
var dbfile = config.appsettings.dbfile;
var rate_transmit = config.appsettings.rate_transmit;
var rate_sampling = config.appsettings.rate_sampling;

// Modules
const mqttmod = require('mqttmod');
const l = require('mqttlogger')(broker, logtopic, mqttmod);
const filter = require('./filter');

// Variables
var initobj = '{"node":"'+mynodeid+'","request":"send"}';
var orderedMeasurements = [];

// Functions
function filterResults(payload) {
	var results = JSON.parse(payload);
	l.info('Filtering '+results.length+' results.');
	Array.prototype.push.apply(orderedMeasurements,filter.order(results));
	l.debug('Ended up with ' + orderedMeasurements.length + ' ordered measurements.');
	let data = orderedMeasurements.splice(0,orderedMeasurements.length);
	sendData(data);
}

function sendData (results) {
	l.info('Sending data, array of '+JSON.stringify(results.length)+' results.');
	mqttmod.send(broker,nextnode,JSON.stringify(results));
}

// Begin execution
// Request data
mqttmod.send(broker,previousnode,initobj);

// Start recieving MQTT messages, upon getting them, relay them to the next node
mqttmod.receive(broker,listentopic,filterResults);	