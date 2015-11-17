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
	var batchOfUsers = userIdQueue.splice(0, 90);

	// object format
	// {userId, filename}

	console.log("Looking up", batchOfUsers.length, "users")

	//create object with key=userId value=filename

	var userLookup = {}
	var ids = []

	for (var i=0; i<batchOfUsers.length; i++) {
		ids.push(batchOfUsers[i].userId)
		ids.push(batchOfUsers[i].filename)
		userLookup[batchOfUsers[i].userId] = {filename: batchOfUsers[i].filename, dataRow: true}
	}

	// console.log(userLookup)

	var ids = ids.filter(onlyUnique);

	T.get('users/lookup', { user_id: ids.join(',') },  function(error, data, response) {
		if (error) {
			//add the users back to the queue so it can be tried again
			userIdQueue = userIdQueue.concat(batchOfUsers);
			console.error(error);
			return;
		}

		for (var i=0; i<data.length; i++) {
			if (data[i].id in userLookup) {
				userLookup[data[i].id].name = data[i].screen_name
			} else {
				userLookup[data[i].id] = {name: data[i].screen_name}
			}
		}
		// console.log(userLookup)


		var lines = []
		for (var i=0; i<data.length; i++) {
			if (data[i].id in userLookup && userLookup[data[i].id].dataRow) {
				// console.log(data[i].id, data[i].screen_name, userLookup[data[i].id].filename, userLookup[userLookup[data[i].id].filename].name)
				lines.push(userLookup[userLookup[data[i].id].filename].name + "\t" + data[i].screen_name)
			}
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