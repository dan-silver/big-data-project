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
	files.forEach(function(file) {
		var rl = require('readline').createInterface({
			input: require('fs').createReadStream(DATA_DIRECTORY + file)
		});

		rl.on('line', function (userId) {
			addUserIdToQueue(userId)
		});
	});
});


var userIdQueue = []
function addUserIdToQueue(userId) {
	userIdQueue.push(userId);
	if (userIdQueue.length >= 100) {
		sendBatchUserRequest()
	}
}

function sendBatchUserRequest() {
	var batchOfUsers = userIdQueue.slice(0, 100);

	T.get('users/lookup', { user_id: batchOfUsers.join(',')},  function(error, data, response) {
		if (error) {
			console.error(error)
			console.log('success')
			console.log(data)
			console.log(data[0])
			console.log('success')
		}
	});
}