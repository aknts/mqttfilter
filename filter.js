/*
	A higher order filter that processes the data structure of the original measurements in order to 
	group them for the same user.
*/

var filter = {
	//groups measurements' batches by user id
	order: function(measurements,l){
		l.info('Clustering batch of measurements...');

		var measurementsKeyHolder = [];
		var orderedMeasurements = [];
		for (var i=0, n=measurements.length; i < n; ++i ) {
			measurementsKeyHolder[measurements[i].uid] = measurementsKeyHolder[measurements[i].uid]||{};
			var obj = measurementsKeyHolder[measurements[i].uid];
			if(Object.keys(obj).length == 0){
				orderedMeasurements.push(obj);
				obj.uid = measurements[i].uid;
				obj.timestamp = measurements[i].timestamp;
			}
			obj.signalArray  = obj.signalArray||[];
			obj.signalArray.push({did:measurements[i].did, RSSI:measurements[i].RSSI});
		}
		return orderedMeasurements;
	}	
};

module.exports = filter;