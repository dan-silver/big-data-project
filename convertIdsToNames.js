// create output usernames.txt
// loop over all the data files
	// in batches of 100
		// map userid to name
		// write names to usernames.txt in format "name1 tab name2" where name1 is following name2

var Twit = require('twit'),
	fs = require('fs'),
	keys = require('./keys.js')

var T = new Twit({
  consumer_key: keys.consumer_key,
  consumer_secret: keys.consumer_secret,
  access_token: keys.access_token,
  access_token_secret: keys.access_token_secret
});

var DATA_DIRECTORY = 'data/'

var global_user_map = {}; // {userid:name}

fs.readdir(DATA_DIRECTORY, function(err, files) {
	if (err) throw err;
	for(var i=0; i<files.length; i++) {
		var file = files[i];
		var rl = require('readline').createInterface({
			input: require('fs').createReadStream(DATA_DIRECTORY + file)
		});

		(function(file) { //bind filename to a local var in the async callback
			rl.on('line', function (userId) {
				addUserIdToQueue(file, userId)
			});
		})(file);

		(function(i) { //bind i to a local var in the async callback
			rl.on('close', function () {
				// after the last file is read and the entries are added to the queue
				if (i == files.length -1) {
					processQueue();
				}
			});
		})(i);
	}
});


var userIdQueue = []

function addUserIdToQueue(file, userId) {
	userIdQueue.push({userId: userId, filename: file.replace('.txt', '')});
}

function sendBatchUserRequest() {
	// object format
	// {userId, filename}


	//create object with key=userId value=filename

	var userLookup = {}
	var ids = []
	var lines = []

	// if size(ids) < 98 add another id (or possibly two if filename != id)
	while (ids.length < 98)
		var user = userIdQueue.splice(0, 1);

		// check if we've already cached the id
		if (!(user.userId in global_user_map)) {
			ids.push(user.userId)
		}

		if (!(user.filename in global_user_map)) {
			ids.push(user.filename)
		}

		userLookup[user.userId] = {filename: user.filename, dataRow: true}
		
		ids = ids.filter(onlyUnique);
	}

	console.log("Looking up", ids.length, "users with Twitter API")

	// console.log(userLookup)

	T.get('users/lookup', { user_id: ids.join(',') },  function(error, data, response) {
		if (error) {
			//add the users back to the queue so it can be tried again
			userIdQueue = userIdQueue.concat(batchOfUsers);
			console.error(error);
			return;
		}

		for (var i=0; i<data.length; i++) {
			global_user_map[data[i].id] = data[i].screen_name;
		}


			// if (data[i].id in userLookup) {
			// 	userLookup[data[i].id].name = data[i].screen_name
			// } else {
			// 	userLookup[data[i].id] = {name: data[i].screen_name}
			// }
		}

		// console.log(userLookup)
		// for (var i=0; i<data.length; i++) {
		// 	if (data[i].id in userLookup && userLookup[data[i].id].dataRow) {
		// 		// console.log(data[i].id, data[i].screen_name, userLookup[data[i].id].filename, userLookup[userLookup[data[i].id].filename].name)
		// 		lines.push(userLookup[userLookup[data[i].id].filename].name + "\t" + data[i].screen_name)
		// 	}
		// }

		for (var i=0; i<batchOfUsers.length; i++) {
			var user = batchOfUsers[i];

			var followerId  = user.filename;
			var followingId = user.userId;

			var followerName  = global_user_map[followerId] ;
			var followingName = global_user_map[followingId];
			
			lines.push(followerName + "\t" + followinNgame);
		}



		fs.appendFile('userFollowingList.txt', lines.join('\n'), function (error) {
			if (error) throw error;
		});
	});
}


// helper method to remove duplicates from an array
function onlyUnique(value, index, self) { 
    return self.indexOf(value) === index;
}


function processQueue() {
	if (userIdQueue.length >= 90) {
		console.log("Popping queue")
		sendBatchUserRequest()
		setTimeout(processQueue, 30*1000)
	}
}