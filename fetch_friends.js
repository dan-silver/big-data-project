var Twit = require('twit'),
	fs = require('fs'),
	keys = require('./keys.js')

var T = new Twit({
  consumer_key: keys.consumer_key,
  consumer_secret: keys.consumer_secret,
  access_token: keys.access_token,
  access_token_secret: keys.access_token_secret
});


var data_directory = 'data/'
var queue = []

function fetchFriendIds(user_id, cursor, limit, callback) {
	if (limit < 0) {
		console.log("limit", limit, "reached")
		return callback();
	}

	T.get('friends/ids', { user_id: user_id, count: 5000, cursor: cursor, stringify_ids: true},  function(error, data, response) {
		if (error) {
			console.error(error)
			return callback()
		}

		console.log(data.ids)
		appendLineSeparatedToFile(user_id + '.txt', data.ids)

		for (var i=0; i<data.ids.length; i++)
			queue.push(data.ids[i])

		// get next batch of ids if their is another page
		if (data['next_cursor'] == 0) {
			console.log('End of data reached for', user_id)
			callback()
		} else
			fetchFriendIds(user_id, data['next_cursor'], limit - data.ids.length)
	})
}

function appendLineSeparatedToFile(filename, array) {
	console.log('Adding ' + array.length + ' rows to ' + filename)
	
	fs.appendFile(data_directory + filename, array.join('\n')+'\n', function (error) {
		if (error) {
			console.error(error)
		}
	})
}

// screenname, twitter terminator, limit

//seed queue with 'mizzou'

function processQueue() {
	if (queue.length == 0)
		return

	var item = queue.splice(0,1)
	fetchFriendIds(item, -1, 20*1000, function() {
		processQueue()
	})
}

queue.push('23620660') //@mizzou
processQueue()