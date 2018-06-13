let file = cat(fileName);  // read the file
let originDeviceIDs = file.split('\n'); // create an array of words

const numberEvents = 5;

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

let originDeviceIDsNotFound = originDeviceIDs.slice();

function unique(array) {
  let arr = [];
  for(let i = 0; i < array.length; i++) {
    if(!arr.includes(array[i])) {
      arr.push(array[i]);
    }
  }
  return arr; 
}

let fields = [
  {
    key: "hid",
    name: "Giver ID"
  },
  {
    key: "_id",
    name: "Devicehub ID"
  },
  {
    key: "dbName",
    name: "Devicehub Inventory"
  },
  {
    key: "type",
    name: "Type"
  },
  {
    key: "serialNumber",
    name: "Serial number"
  },
  {
    key: "manufacturer",
    name: "Manufacturer"
  },
  {
    key: "model",
    name: "Model"
  }
];

let eventFields = [
  {
    key: "@type",
    name: "Event title"
  },
  {
    key: "type",
    name: "Event status"
  },
  {
    key: "label",
    name: "Event label"
  },
  {
    key: "_created",
    name: "Event date"
  }
];


print(fields.map((f) => f.name).join(',')
      +','
      +
      (
	eventFields
	  .map((f) => f.name)
	  .join(',')
	  +','
      ).repeat(numberEvents));

originDeviceIDs.forEach(function(originID) {
  let foundDevices = [];
  for(let i=0; i<dbs.length; i++) {
    let dbName = dbs[i];
    db = db.getSiblingDB(dbs[i]);
    let projection = {};
    fields.map((f) => {
      projection[f.key] = 1;
    });
    let foundDevicesInDB = db.getCollection('devices').find(
      {
	$or: 
	[ 
	  { pid: originID }, 
	  { gid: originID }
	]
      }, 
      projection
    ).toArray();

    foundDevices = foundDevices.concat(foundDevicesInDB.map(function(device) {
      // get last X events
      let query = {
	$or: [
	  {
	    "device": device._id
	  },
	  {
	    "devices": device._id
	  }
	]
      };
      let events = db.getCollection('events')
	  .find(query)
	  .sort({ '_created': -1 })
	  .limit(numberEvents)
	  .toArray();

      device.events = events;
      device.hid = originID;
      device.dbName = dbName;
      return device;
    }));
  }

  foundDevices = unique(foundDevices);
  if(foundDevices.length == 0) {
    debug && print('WARN Could not find any device for origin id', originID, foundDevices.length);
    // print device not found
    print(originID+',Not registered');
  } else {
    originDeviceIDsNotFound.splice(originDeviceIDsNotFound.indexOf(originID),1);
    foundDevices.forEach((device) => {
      // get 'normal' fields
      let out = '';
      fields.map((f) => f.key).forEach((prop) => {
	out += device[prop] + ',';
      });
      device.events.forEach(function(event) {
	eventFields.map((f) => f.key).forEach((key) => {
	  if(key === '@type' && event[key]) {
	    out += event[key].substr('devices:'.length);
	  } else {
	    out += event[key] || '';
	  }
	  out += ',';
	});
      });


      print(out);
    });

    
  }
});



