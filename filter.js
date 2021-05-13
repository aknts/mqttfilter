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
	},
	removeDuplicates: function(measurements){
		for (var i=0, n=measurements.length; i < n; ++i ) {
    		let RPIKey = [];
      		let RPIArray = [];
      		for (var signalReading of measurements[i].signalArray){
        		if (RPIKey[signalReading.did]){
          			continue;
        		}
        		RPIKey[signalReading.did] = true;
        		RPIArray.push(signalReading);
      		}
      		measurements[i].signalArray = RPIArray;
    	}
    return measurements;
	},
	removeInvalid: function(measurements) {
		var uniqueMeasurements = [];
		for (var i=0, n=measurements.length; i < n; ++i ) {
			if (measurements[i].signalArray.length > 2) {
				uniqueMeasurements.push(measurements[i]);
		  	}
		}
	  return uniqueMeasurements;
	}	
};

module.exports = filter;