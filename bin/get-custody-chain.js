let file = cat(fileName);  // read the file
let originDeviceIDs = file.split('\n'); // create an array of words

const debug = false;

let dbs = [
  'dh_lakalle',
  'dh_reutilitzaupc',
  'dh_reciclanet',
  'dhbeta_reutilitzaupc',
  'dh_andromines',
  'dh_acsrecycling',
  'dh_tauorgar',
  'dh_alencop',
  'dh_donaloorg',
  'dh_iescastellbisbal',
  'dh_secondchancemodesto',
  'dh_ereuseorg',
  'dh_solidanca',
  'dh_circuitreutilitzacat',
  'dh_computeraid',
  'dh_engrunes',
  'dh_trinijove'
];



let monthlyEvents = [
  "devices:EraseFailure", 
  "devices:EraseSuccess", 
  "devices:Register", 
  "devices:Ready",
  "devices:Receive",
  "devices:FinalUser",
  "devices:Sell",
  "devices:ToDispose",
  "devices:Dispose"
];

let aggregatedEvents = [
  "devices:Registered", 
  "devices:NotRegistered"
];

/* for each event, sum if last event for timespan from beginning */
let statusEvents = [
  "devices:InPreparation", // in preparation / going to be ready: one of snapshot, register or dispose
  "devices:Ready",
  "devices:InReuse", // finaluser or sell
  "devices:Dispose"
];

let FIRSTDAYOFMONTH = 1;
let START_DATE = new Date(2017, 0, 1);
let END_DATE = new Date(2018, 2, 1);
let MONTH_NAMES = ["January", "February", "March", "April", "May", "June",
  "July", "August", "September", "October", "November", "December"
];

let ts = START_DATE;
let next_ts;
let tss = [];
(function setTimeSpans() {
  while(ts.getTime() <= END_DATE.getTime()) {
    if (ts.getMonth() == 11) {
      next_ts = new Date(ts.getFullYear() + 1, 0, FIRSTDAYOFMONTH);
    } else {
      next_ts = new Date(ts.getFullYear(), ts.getMonth() + 1, FIRSTDAYOFMONTH);
    }
    tss.push({
      quarter: parseInt(ts.getMonth() / 3 ) + 1,
      month: ts.getMonth(),
      year: ts.getFullYear(),
      eventCreatedAggregated : {
	$gt: START_DATE, $lte: next_ts
      },
      eventCreated : {
	$gt: ts, $lte: next_ts
      }
    });
    ts = next_ts;
  }
})();


function getLabels() {
  //labels.push("Non-components created (month)");
  let labels = [];
  labels = labels.concat(monthlyEvents.map((e) => { return 'Monthly:'+e.substring("devices:".length, e.length);}));
  labels.push('Registered (accumulated)');
  labels.push('Not registered (accumulated)');
  labels = labels.concat(statusEvents.map((e) => { return 'Status:'+e.substring("devices:".length, e.length);}));
  // labels.push('Status:Sum');
  return labels;
}


let originDeviceIDsNotFound = originDeviceIDs.slice();
let deviceIds = [];
let duplicateIDs = [];

function unique(array) {
  let arr = [];
  for(let i = 0; i < array.length; i++) {
    if(!arr.includes(array[i])) {
      arr.push(array[i]);
    }
  }
  return arr; 
}
originDeviceIDs.forEach(function(originID) {
  let foundDeviceIds = [];
  for(let i=0; i<dbs.length; i++) {
    db = db.getSiblingDB(dbs[i]);
    let foundDevices = db.getCollection('devices').find({$or: 
				      [ 
					{ pid: originID }, 
					{ gid: originID }
				      ]
				     }, { _id: 1 }).toArray();

    foundDeviceIds = foundDeviceIds.concat(foundDevices.map(function(d) {
      return d._id;
    }));
  }

  foundDeviceIds = unique(foundDeviceIds);
  if(foundDeviceIds.length == 0) {
    debug && print('WARN Could not find any device for origin id', originID, foundDeviceIds.length);
  } else {
    originDeviceIDsNotFound.splice(originDeviceIDsNotFound.indexOf(originID),1);
    deviceIds.push(foundDeviceIds);
  }
});

print('Found', deviceIds.length, 'of', originDeviceIDs.length, 'devices in', dbs.length,'databases');

print(",,,", "Per month", monthlyEvents.map((l) => { return ','; }), "Registered (accumulated),,", "Status at end of month");
print("quarter,month,year,",getLabels().join());
tss.forEach(function(ts) {
  let lastEventCountsAggregated = {};
  statusEvents.forEach(function(e) {
    lastEventCountsAggregated[e] = 0;
  });
  let monthlyEventCounts = {};
  monthlyEvents.forEach(function(e) {
    monthlyEventCounts[e] = 0;
  });


  //for each device increment count of last event
  deviceIds.forEach(function(ids) {
    let orDeviceID = [
      {
	"device": {
	  $in: ids
	}
      },
      {
	"devices": {
	  $in: ids
	}
      }
    ];

    //get all events in all databases for device
    let aggregatedDeviceEvents = [];
    for(let i=0; i<dbs.length; i++) {
      db = db.getSiblingDB(dbs[i]);

      // aggregated events
      for(let j=0; j<statusEvents.length; j++) {
	let statusEvent = statusEvents[j];
    	let query = {
    	  _created: ts.eventCreatedAggregated
    	};
	
	if(statusEvent === 'devices:InReuse') {
	  query['$and'] = [
	    { 
	      $or : [
		{
		  "@type": 'devices:Receive',
		  type: 'FinalUser'
		},
		{
		  "@type": 'devices:Sell'
		}
	      ]
	    },
	    { 
	      $or : orDeviceID
	    }
	  ];
	} else {
	  query['@type'] = statusEvent;
	  query['$or'] = orDeviceID;
	}
	
	if(statusEvent === 'devices:InPreparation') {
	  query['@type'] = {
	    $in: [ 'devices:Register', 'devices:ToPrepare', 'devices:Repair', 'devices:ToRepair', 'devices:ToDispose' ]
	  };
	}

    	let eventsFound = db.getCollection('events').find(query).toArray();

	// in case of multiple @types in query, we map all posible types to one
	eventsFound.forEach((event) => {
	  event['@type'] = statusEvent;
	});
	  
	if(eventsFound.length > 0) {
	  /* debug && print(
	    "Q"+ts.quarter+","+(ts.month+1)+","+ts.year+","
	      +"found "+eventsFound.length+"events of type"+statusEvent+" for device "+id); */
	}
    	aggregatedDeviceEvents = aggregatedDeviceEvents.concat(eventsFound);
      }


      // monthly events
      for(let j=0; j<monthlyEvents.length; j++) {
	let monthlyEvent = monthlyEvents[j];
    	let query = {
    	  "@type": monthlyEvent,
    	  _created: ts.eventCreated,
	  $or: orDeviceID
    	};
	if(monthlyEvent === 'devices:FinalUser') {
	  query["@type"] = 'devices:Receive';
	  query.type = 'FinalUser';
	} else if(monthlyEvent === 'devices:EraseFailure') {
	  query["@type"] = { 
	    $in : [ 
	      'devices:EraseBasic', 
	      'devices:EraseSectors'] 
	  };
	  query.success = false;
	} else if(monthlyEvent === 'devices:EraseSuccess') {
	  query["@type"] = { 
	    $in : [ 
	      'devices:EraseBasic', 
	      'devices:EraseSectors'] 
	  };
	  query.success = true;
	}
	
	let numMonthlyEvents = 0;
	if(monthlyEvent === 'devices:Register') {
	  query["@type"] = 'devices:Snapshot';
	  numMonthlyEvents = db.events.distinct('device', query).length;
	} else {
    	  numMonthlyEvents = db.getCollection('events').count(query);
	}
    	monthlyEventCounts[monthlyEvent] += numMonthlyEvents;
      }

      // aggregatedEvents TODO
      // for(let j=0; j<aggregatedEvents.length; j++) {
      // 	let aggregatedEvent = aggregatedEvents[j];
      // 	let query = {
      // 	  "@type": aggregatedEvent,
      // 	  _created: ts.eventCreated,
      // 	  $or: [
      // 	    {
      // 	      "device": id
      // 	    },
      // 	    {
      // 	      "devices": id
      // 	    }
      // 	  ]
      // 	};
	
      // 	let numAggregatedEvents = 0;
      // 	if(aggregatedEvent === 'devices:Registered') {
      // 	  query["@type"] = 'devices:Snapshot';
      // 	  numAggregatedEvents = db.events.distinct('device', query).length;
      // 	  monthlyEventCounts['devices:Registered'] += numAggregatedEvents;
      // 	  monthlyEventCounts['devices:NotRegistered'] += numAggregatedEvents;
      // 	} else if(aggregatedEvent === 'devices:NotRegistered') {
      // 	  // already in registered
      // 	} else {
      // 	  monthlyEventCounts[aggregatedEvent] += numAggregatedEvents;
      // 	}
      	
      // }
    }

    //increment count of last aggregated event
    if(aggregatedDeviceEvents.length > 0) {
      aggregatedDeviceEvents = aggregatedDeviceEvents.sort(function(a,b){
      	return new Date(b._created) - new Date(a._created);
      });
      let lastEvent = aggregatedDeviceEvents[0]["@type"];
      /* debug && print('count last event of type '+lastEvent); */
      lastEventCountsAggregated[lastEvent] += 1; //count last event only
    }
  });

  
  // convert hash maps to printable lists
  let monthlyEventCountsList = [];
  Object.keys(monthlyEventCounts).forEach(function(event) {
    let eventCount = monthlyEventCounts[event] || '';
    monthlyEventCountsList.push(eventCount);
  });
  let lastEventCountsAggregatedList = [];
  let lastEventCountsSum = 0;
  Object.keys(lastEventCountsAggregated).forEach(function(event) {
    let eventCountAggregated = lastEventCountsAggregated[event] || '';
    debug && print('lastEventCountsAggregated for event '+event+':'+eventCountAggregated);
    lastEventCountsAggregatedList.push(eventCountAggregated);
    if(eventCountAggregated) {
      lastEventCountsSum += parseInt(eventCountAggregated);
    }
  });

  // print
  print(("Q"+ts.quarter)+","+(ts.month+1)+","+ts.year+","
	+monthlyEventCountsList.join()
	+","
	+lastEventCountsSum // Registered
	+","
	+(originDeviceIDs.length - lastEventCountsSum) // Not registered
	+","
	+lastEventCountsAggregatedList.join()
	// +","
	// +lastEventCountsSum // Control, necessary?
       );
});

if(duplicateIDs.length > 0) {
  print();
  print(duplicateIDs.length,'devices with multiple ids:');
  print("Origin ID, IDs found in devicehub");
  duplicateIDs.forEach(function(d) {
    print(d.originDeviceID + ',' + d.devicehubIDs.join(' - '));
  });
}

print();
print(originDeviceIDsNotFound.length,'devices not found:');
print("Origin ID");
originDeviceIDsNotFound.forEach(function(d) {
  print(d);
});

