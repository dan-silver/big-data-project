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

function fetchFollowerIds(screen_name, cursor, limit) {
	if (limit < 0)
		return;

	T.get('followers/ids', { screen_name: screen_name, count: 5000, cursor: cursor, stringify_ids: true},  function(error, data, response) {
		if (error) {
			console.error(error)
			return
		}

		console.log(data.ids)
		appendLineSeparatedToFile(screen_name + '.txt', data.ids)

		// get next batch of ids
		if (data['next_cursor'] != 0)
			fetchFollowerIds(screen_name, data['next_cursor'], limit - data.ids.length)
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
fetchFollowerIds('mizzou', -1, 20*1000)