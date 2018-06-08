var dbs = [
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
  "devices:Snapshot",
  "devices:Ready",
  "devices:Locate",
  "devices:Allocate",
  "devices:Receive",
  "devices:Alive",
  "devices-Recycle",
  "devices:Dispose",
  "devices:Shell",
  "devices:Migrate",
];

var COLLECTION="events";
var FIRSTDAYOFMONTH = 1;
var START_DATE = new Date(2017, 9, 1);
var END_DATE = new Date();
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
  labels.push("Non-components created (month)");
  labels = labels.concat(events.map((e) => { return e.substring("devices:".length, e.length)+" (month)";}));
})();


print("device_hub,quarter,month,year,"+labels.join(","));
for(var i=0; i<dbs.length; i++) {
  db = db.getSiblingDB(dbs[i]);
  tss.forEach(function(ts) {
    
    var results = [];
    results.push(
      db[COLLECTION].distinct('device', {'@type': 'devices:Snapshot', '_created': ts.eventCreated }).length
    );
    results = results.concat(events.map(function(e) {
      var query = {
	"@type": e, 
	"_created": ts.eventCreated
      };
      return db[COLLECTION].count(query);
    }));

    print(dbs[i]+","+("Q"+ts.quarter)+","+(ts.month+1)+","+ts.year+","+results.join(","));
  });
}



