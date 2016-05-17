/**
 * New node file
 */
var mongo = require("./mongo");
var mongoURL = "mongodb://54.213.131.219:27018/login";

exports.checkLogin = function(req,res){
	// These two variables come from the form on
	// the views/login.hbs page
	var username = req.param("username");
	var password = req.param("password");
	console.log(password +" is the object");
	var json_responses;

	mongo.connect(mongoURL, function(){
		console.log('Connected to mongo at: ' + mongoURL);
		var coll = mongo.collection('login');

		coll.findOne({username: username, password:password}, function(err, user){
			if (user) {
				// This way subsequent requests will know the user is logged in.
				req.session.username = user.username;
				console.log(req.session.username +" is the session");
				json_responses = {"statusCode" : 200};
				res.send(json_responses);

			} else {
				console.log("returned false");
				json_responses = {"statusCode" : 401};
				res.send(json_responses);
			}
		});
	});
};

//Redirects to the homepage
exports.redirectToHomepage = function(req,res)
{
	//Checks before redirecting whether the session is valid
	if(req.session.username)
	{
		//Set these headers to notify the browser not to maintain any cache for the page being loaded
		res.header('Cache-Control', 'no-cache, private, no-store, must-revalidate, max-stale=0, post-check=0, pre-check=0');
		res.render("homepage",{username:req.session.username});
	}
	else
	{
		res.redirect('/');
	}
};

//Logout the user - invalidate the session
exports.logout = function(req,res)
{
	req.session.destroy();
	res.redirect('/');
};

