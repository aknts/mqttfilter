/*
	A higher order filter that processes the data structure of the original measurements in order to 
	group them for the same user.
*/

//convenience object to log events
//var l = require('./logger');

var filter = {
	//groups measurements' batches by user id
	order: function(measurements){
	//	l.info('Clustering batch of measurements...');

		var measurementsKeyHolder = [];
		var orderedMeasurements = [];

		measurements.forEach(function(item){
			measurementsKeyHolder[item.uid] = measurementsKeyHolder[item.uid]||{};
			var obj = measurementsKeyHolder[item.uid];
			if(Object.keys(obj).length == 0){
				orderedMeasurements.push(obj);
				obj.uid = item.uid;
				obj.timestamp = item.timestamp;
			}

			obj.signalArray  = obj.signalArray||[];
			obj.signalArray.push({did:item.did, RSSI:item.RSSI});
		});
		return orderedMeasurements;
	},	
};

module.exports = filter;