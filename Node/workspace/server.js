var express = require('express');
var app = express();
var bodyParser = require('body-parser');
var path = require('path');
var fs = require('fs');
var ejs = require('ejs');
var geohash = require("geohash").GeoHash;
var session = require('client-sessions');
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

var mongodb = require('mongodb');
var router = express.Router();
var url = 'mongodb://54.213.131.219:27018/smartagri';

//MongoDB connection URL - mongodb://host:port/dbName
var dbHost = "mongodb://54.213.131.219:27018/smartagri";

//DB Object
var dbObject;
app.use(session({
	cookieName: 'session',
	secret: 'eg[isfd-8yF9-7w2315df{}+Ijsli;;to8',
	duration: 30*60*1000,
	activeDuration: 5*60*1000
}));
//get instance of MongoClient to establish connection
app.use(function(req, res, next){
	
	var MongoClient = mongodb.MongoClient;
	
	MongoClient.connect(url, function(err, db){
		var collection = db.collection('authentication');
		if(req.session && req.session.user){
			collection.findOne({username:req.session.user.username},function(err, user){
				if(user){
					req.user = user;
					delete req.session.user.password;
					req.session.user = user;
					res.locals.user = user;
				}
				next();
			});
		}else{
			next();
		}
	});
});
var MongoClient = mongodb.MongoClient;


function getData(requestObj,responseObj){
	console.log(requestObj.body.sensorid);
	var ret = new Array();
	var element;
	var iSensor = parseInt(requestObj.body.sensorid);
	MongoClient.connect(dbHost, function(err, dbObject){
	console.log(typeof requestObj.body);
	if ( err ) { console.log("In connect error"); throw err; }
	else {
	  console.log("In connect else");
	 // dbObject.collection("Venkatesh").find({}).toArray(function(err, docs){
    dbObject.collection("venkatesh").find({"SensorId":iSensor}).sort({_id:1}).limit(1).toArray(function(err, docs){
	if ( err ) {console.log("In else err"); throw err;}
	console.log(docs);
	var index;
    for ( index in docs){		
      var doc = docs[index];
	  var latitude = doc['Latitude'];
	  //category array
      var longitude = doc['Longitude'];
      //series 1 values array
      var data = doc['Data'];
	  //element = doc['Sensorname']+","+JSON.parse(data);
	  element = doc['date'] + "~"+doc['Sensorname']+"~"+data;
	  responseObj.send(element);
	  var i = 0;  
	  for(var key in element)
	  {
		 if(element.hasOwnProperty(key))
		 {
			var val = element[key];
			ret[i++] = val;
			//console.log();	
		 }	
		  	
	  }		    
	  console.log(latitude + " " + longitude);
    }
	});
  }
	
});
	
}
//getData();

app.use('/', router);

app.set('view engine', 'ejs');
app.set('port' , process.env.PORT || 3000);
app.use("/css", express.static(__dirname + '/css'));
app.use("/css1", express.static(__dirname + '/css1'));
app.use("/js", express.static(__dirname + '/js'));
app.use("/js1", express.static(__dirname + '/js1'));
app.use("/images", express.static(__dirname + '/images'));
app.use("/plugins", express.static(__dirname + '/plugins'));
app.use("/dist", express.static(__dirname + '/dist'));
app.use("/build", express.static(__dirname + '/build'));
app.use("/bootstrap", express.static(__dirname + '/bootstrap'));
app.use(express.static(__dirname + '/views'));

function requireLogin(req, res, next){
	
	if(!req.user){
		console.log("require Login failed");
		res.redirect('/login');
	}else{
		console.log("require Login pass");
		next();
	}
}
app.post("/userdata", function(req, res){
  console.log(req.body);
  getData(req,res);
});


router.get('/register', function(req, res){
	res.render('register',{});
});

router.post('/register', function(req, res){
	var MongoClient = mongodb.MongoClient;
	
	MongoClient.connect(url, function(err, db){
		if(err){
			console.log("cannot connect to db in register");
		}else{
			var collection = db.collection('authentication');
			var userinfo = {username:req.body.username, password:req.body.password, email:req.body.email};
			
			collection.insert(userinfo, function(err, result){
				if(err){
					console.log("cannot insert into auth db");
				}else{
					console.log("successfully inserted into auth db");
					db.close();
					res.redirect('/login');
				}				
			});
		}
	});
});


router.post('/login', function(req, res){
	var MongoClient = mongodb.MongoClient;
	console.log("inside login post");
	
	MongoClient.connect(url, function(err, db){
		if(err){
			console.log("unable to connect to db-login", err);
		}else{
			console.log("connection successful inside login");	
			var collection = db.collection('authentication');
			collection.findOne({username: req.body.username}, function(err, user){
				if(!user){
					res.render('login');
				}else{					
					
					if(req.body.password === user.password){
						//set cookie
						req.session.user = user;
						//fot admin 
						res.redirect('/dashboard');
					}else{
						//console.log(req.session);						
						res.render('login');
					}
				}				
			});
		}
	});
});

router.get('/login', function(req, res){
	res.render('login');
});

router.get('/logout', function(req, res){
	req.session.reset();
	res.redirect('/');
});


router.get('/', function(req, res){
	res.render('home');
});
router.get('/sensormap', function(req, res){
	res.render('sensormap',{});
});


router.get('/monitor', requireLogin, function(req, res){
	res.render('monitor', {uservar:req.session.user.username});
});


router.get('/dashboard', requireLogin, function(req, res){
	res.render('dashboard', {uservar:req.session.user.username});
});

router.get('/list', function(req, res){
	var MongoClient = mongodb.MongoClient;
	MongoClient.connect(url, function (err, db) {
      if (err) {
        console.log('Unable to connect to the mongoDB server. Error:', err);
      } else {
        console.log('Connection established to', url);


        // Get the documents collection
        var collection = db.collection(req.session.user.username);
		
        // Query the collection    
        collection.find({},{},{ limit : 30 }).sort({'date':-1}).toArray(function (err, result) {
			console.log('inside query');
          if (err) {
            console.log(err);
          } else if (result.length) {
            console.log('Found:', result);
          } else {
			  console.log('Found:', result.length);
		/*	result=[{
      "name": "Jena Gaines",
      "designation": "Office Manager",
      "salary": "$90,560",
      "joining_date": "2008/12/19",
      "office": "London",
      "extension": "3814"
    },
    {
      "name": "Quinn Flynn",
      "designation": "Support Lead",
      "salary": "$342,000",
      "joining_date": "2013/03/03",
      "office": "Edinburgh",
      "extension": "9497"
			}];*/
			
            console.log('No document(s) found with defined "find" criteria!');
          }
		  
		  
          //Close connection
          db.close();
					res.render('list', {result:result, uservar:req.session.user.username});
        });
      }
   }); 
	//res.render('list',{});
});

router.get('/profile', requireLogin, function(req, res){
	res.render('profile',{uservar:req.session.user.username});
});

router.get('/manage', requireLogin, function(req, res){
	res.render('manage',{uservar:req.session.user.username});
});

router.get("/map",requireLogin,function (req,res)
    {
		var MongoClient = mongodb.MongoClient;
	MongoClient.connect(url, function (err, db) {
      if (err) {
        console.log('Unable to connect to the mongoDB server. Error:', err);
      } else {
        console.log('Connection established to', url);


        // Get the documents collection
        var collection = db.collection('user');
		
        // Query the collection    
        collection.find({username:req.session.user.username},{},{ limit : 30 }).sort({'date':-1}).toArray(function (err, result) {
			console.log('inside query');
          if (err) {
            console.log(err);
          } else if (result.length) {
            console.log('Found:', result);
          } else {
			  console.log('Found:', result.length);
		/*	result=[{
      "name": "Jena Gaines",
      "designation": "Office Manager",
      "salary": "$90,560",
      "joining_date": "2008/12/19",
      "office": "London",
      "extension": "3814"
    },
    {
      "name": "Quinn Flynn",
      "designation": "Support Lead",
      "salary": "$342,000",
      "joining_date": "2013/03/03",
      "office": "Edinburgh",
      "extension": "9497"
			}];*/
			
            console.log('No document(s) found with defined "find" criteria!');
          }
		  
		  
          //Close connection
          db.close();
		res.render('map', {result:result, uservar:req.session.user.username});
        });
      }
   }); 
	//res.render('list',{});
         
    });
 router.get('/sensorStatus',requireLogin, function(req, res){
	var MongoClient = mongodb.MongoClient;
	MongoClient.connect(url, function (err, db) {
      if (err) {
        console.log('Unable to connect to the mongoDB server. Error:', err);
      } else {
        console.log('Connection established to', url);


        // Get the documents collection
        var collection = db.collection('sensorstat');
		
        // Query the collection    
        collection.find({username:req.session.user.username},{},{ limit : 30 }).toArray(function (err, result) {
			console.log('inside query');
          if (err) {
            console.log(err);
          } else if (result.length) {
            console.log('Found:', result);
          } else {
			  console.log('Found:', result.length);
		/*	result=[{
      "name": "Jena Gaines",
      "designation": "Office Manager",
      "salary": "$90,560",
      "joining_date": "2008/12/19",
      "office": "London",
      "extension": "3814"
    },
    {
      "name": "Quinn Flynn",
      "designation": "Support Lead",
      "salary": "$342,000",
      "joining_date": "2013/03/03",
      "office": "Edinburgh",
      "extension": "9497"
			}];*/
			
            console.log('No document(s) found with defined "find" criteria!');
          }
		  
		  
          //Close connection
          db.close();
		res.render('sensorStatus', {result:result,uservar:req.session.user.username});
        });
      }
   }); 
	//res.render('list',{});
});
router.get('/billingStatus',requireLogin, function(req, res){
	var MongoClient = mongodb.MongoClient;
	MongoClient.connect(url, function (err, db) {
      if (err) {
        console.log('Unable to connect to the mongoDB server. Error:', err);
      } else {
        console.log('Connection established to', url);


        // Get the documents collection
        var collection = db.collection('user');
		
        // Query the collection    
        collection.find({username:req.session.user.username},{},{ limit : 30 }).toArray(function (err, result) {
			console.log('inside query');
          if (err) {
            console.log(err);
          } else if (result.length) {
            console.log('Found:', result);
          } else {
			  console.log('Found:', result.length);
		/*	result=[{
      "name": "Jena Gaines",
      "designation": "Office Manager",
      "salary": "$90,560",
      "joining_date": "2008/12/19",
      "office": "London",
      "extension": "3814"
    },
    {
      "name": "Quinn Flynn",
      "designation": "Support Lead",
      "salary": "$342,000",
      "joining_date": "2013/03/03",
      "office": "Edinburgh",
      "extension": "9497"
			}];*/
			
            console.log('No document(s) found with defined "find" criteria!');
          }
		  
		  
          //Close connection
          db.close();
		res.render('billingStatus', {result:result,uservar:req.session.user.username});
        });
      }
   }); 
	//res.render('list',{});
});
router.post('/manage', function(req,res){
	//console.log("Inside add post");
	var MongoClient = mongodb.MongoClient;
	
	MongoClient.connect(url, function(err, db){
		if(err){
			console.log("unable to connect", err);
		}else{
			//console.log("successful connection inside manage");
			 
			var collection = db.collection('user');
			var collection2 = db.collection('sensorstat');
			
			var sensor = {username: req.session.user.username, sensorId : parseInt(req.body.sensorId), sensorName : req.body.selectpicker, latitude:parseInt(req.body.latitude), longitude : parseInt(req.body.longitude), threshold:parseInt(req.body.threshold), frequency : parseInt(req.body.frequency)};
			
			collection.insert(sensor, function(err, result){
				if(err){
					console.log("couldn't insert into db", err);					
				}else{
					sensor = {sensorId : parseInt(req.body.sensorId), username:req.session.user.username, sensorStatus : "enabled"}
					collection2.insert(sensor, function(err, result){
						if(err){
						console.log("couldn't insert into db", err);					
						}else{
				
							console.log("successfully inserted");
							res.redirect('/manage');
							db.close();
						}
					});
					
				}
				
			});
		}
	});
});

router.post('/delete', function(req,res){
	//console.log("Inside delete");
	var MongoClient = mongodb.MongoClient;
	
	MongoClient.connect(url, function(err,db){
		if(err){
			console.log("unable to connect to mongo in delete", err);
		}else{
			//console.log("connection successful");
			
			var collection = db.collection('user');
			var collection2 = db.collection('sensorstat');
			
			var sensor = parseInt(req.body.sensorId,10);
			console.log(typeof sensor);
			collection.remove({sensorId:sensor}, function(err, result){
				if(err){
					console.log("unable to delete", err);
				}else{
					//console.log(typeof req.body.sensorId);
					//console.log("successful");	
					collection2.remove({sensorId:sensor}, function(err, result){
						if(err){
							console.log("unable to delete", err);
						}else{
							console.log("successful");
							db.close();
							res.redirect('/manage');							
						}
					});						
				}
			});	
		}
	});
});

router.post('/enable', function(req, res){
	var MongoClient = mongodb.MongoClient;
	
	MongoClient.connect(url, function(err,db){
		if(err){
			console.log("unable to connect in enable", err);
		}else{
			console.log("connection successful");
			
			var collection = db.collection("sensorstat");
			
			var sensor = req.body.sensorId;
			collection.update({sensorId:sensor}, {$set:{sensorStatus:"enabled"}}, function(err){
				if(err){
					console.log("update failed", err);
				}else{
					console.log("successfully updated");
					db.close();
					res.redirect('/manage');
				}
			});
		}
	});
});

router.post('/disable', function(req, res){
	var MongoClient = mongodb.MongoClient;
	
	MongoClient.connect(url, function(err,db){
		if(err){
			console.log("unable to connect in enable", err);
		}else{
			console.log("connection successful");
			
			var collection = db.collection("sensorstat");
			
			var sensor = req.body.sensorId;
			collection.update({sensorId:sensor}, {$set:{sensorStatus:"disabled"}}, function(err){
				if(err){
					console.log("update failed", err);
				}else{
					console.log("successfully updated");
					db.close();
					res.redirect('/manage');
				}
			});
		}
	});
});


app.listen(app.get('port'), function(){
	console.log('Express started on http://localhost:'+app.get('port'));
});

