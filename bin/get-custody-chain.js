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

var events = [
  // "devices:Snapshot",
  // "devices:Ready",
  // "devices:Register",
  // "devices:Locate",
  // "devices:Allocate",
  "devices:Receive",
  "devices:FinalUser",
  "devices:Live",
  "devices:Dispose",
  "devices-Recycle",
  "devices:Sell"
];

var FIRSTDAYOFMONTH = 1;
var START_DATE = new Date(2017, 0, 1);
var END_DATE = new Date(2018, 2, 1);
var MONTH_NAMES = ["January", "February", "March", "April", "May", "June",
  "July", "August", "September", "October", "November", "December"
];

var ts = START_DATE;
var next_ts;
var tss = [];
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
      eventCreated : {
	$gt: ts, $lte: next_ts
      }
    });
    ts = next_ts;
  }
})();


var labels = [];
(function setLabels() {
  //labels.push("Non-components created (month)");
  labels = labels.concat(events.map((e) => { return e.substring("devices:".length, e.length);})).join(',');
})();


let originDeviceIDsNotFound = originDeviceIDs.slice();
let deviceIds = [];

function unique(array) {
  var arr = [];
  for(var i = 0; i < array.length; i++) {
    if(!arr.includes(array[i])) {
      arr.push(array[i]);
    }
  }
  return arr; 
}
originDeviceIDs.forEach(function(originID) {
  let foundDeviceIds = [];
  for(var i=0; i<dbs.length; i++) {
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
  if(foundDeviceIds.length > 1) {
    debug && print('ERROR Found more than one device for origin id', originID, foundDeviceIds.length);
  } else if(foundDeviceIds.length == 0) {
    debug && print('WARN Could not find any device for origin id', originID, foundDeviceIds.length);
  } else {
    originDeviceIDsNotFound.splice(originDeviceIDsNotFound.indexOf(originID),1);
    deviceIds.push(foundDeviceIds[0]);
  }
});

print('Found', deviceIds.length, 'of', originDeviceIDs.length, 'devices in', dbs.length,'databases');

print("quarter,month,year,",labels);
tss.forEach(function(ts) {
  let eventCounts = {};
  events.forEach(function(e) {
    eventCounts[e] = 0;
  });

  //for each device increment count of last event
  deviceIds.forEach(function(id) {

    //get all events in all databases for device
    let deviceEvents = [];
    for(let i=0; i<dbs.length; i++) {
      db = db.getSiblingDB(dbs[i]);
      for(let j=0; j<events.length; j++) {
    	var query = {
    	  "@type": events[j],
    	  _created: ts.eventCreated,
	  $or: [
	    {
	      "device": id
	    },
	    {
	      "devices": id
	    }
	  ]
    	};
	if(events[j] === 'devices:FinalUser') {
	  query["@type"] = 'devices:Receive';
	  query.type = 'FinalUser';
	}
    	let eventsFound = db.getCollection('events').find(query).toArray();
    	deviceEvents = deviceEvents.concat(eventsFound);
      }
    }

    //increment count of last event
    if(deviceEvents.length > 0) {
      deviceEvents = deviceEvents.sort(function(a,b){
      	return new Date(b._created) - new Date(a._created);
      });
      let lastEvent = deviceEvents[0]["@type"];
      eventCounts[lastEvent] += 1; //count last event only
    }
  });

  //print
  let eventCountsList = [];
  Object.keys(eventCounts).forEach(function(event) {
    let eventCount = eventCounts[event] || '';
    eventCountsList.push(eventCount);
  });
  print(("Q"+ts.quarter)+","+(ts.month+1)+","+ts.year+","
	+eventCountsList.join());
});

print();
print(originDeviceIDsNotFound.length,'not found:');
originDeviceIDsNotFound.forEach(function(d) {
  print(d);
});

