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

var DATA_DIRECTORY = 'idsToConvert/'

fs.readdir(DATA_DIRECTORY, function(err, files) {
	if (err) throw err;
	for(var i=0; i<files.length; i++) {
		var file = files[i];
		var rl = require('readline').createInterface({
			input: require('fs').createReadStream(DATA_DIRECTORY + file)
		});

		rl.on('line', function (userId) {
			addUserIdToQueue(file, userId)
		});

		(function(i) {
			rl.on('close', function () {
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

	console.log("Looking up ", batchOfUsers.length, "users")

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
		if (error) throw error;

		for (var i=0; i<data.length; i++) {
			if (data[i].id in userLookup) {
				userLookup[data[i].id].name = data[i].name
			} else {
				userLookup[data[i].id] = {name: data[i].name}
			}
		}
		// console.log(userLookup)


		var lines = []
		for (var i=0; i<data.length; i++) {
			if (data[i].id in userLookup && userLookup[data[i].id].dataRow) {
				console.log(data[i].id, data[i].name, userLookup[data[i].id].filename, userLookup[userLookup[data[i].id].filename].name)
				lines.push(userLookup[userLookup[data[i].id].filename].name + "\t" + data[i].name)
			}
		}

		fs.appendFile('userFollowingList.txt', lines.join('\n'), function (error) {
			if (error) throw error;
		});
	});
}

function onlyUnique(value, index, self) { 
    return self.indexOf(value) === index;
}


function processQueue() {
	if (userIdQueue.length >= 80) {
		console.log("Popping queue")
		sendBatchUserRequest()
		setTimeout(processQueue, 15*1000)
	}
}