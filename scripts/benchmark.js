var exec = require('child_process').exec, child;

/**
 * Usage:
 * 	- adjust parameters in run() function, then run
 * 	- node benchmark.js
 */

const host = 'localhost'
const port = '5432'
const file = 'script.sql'

// map numClients --> averageLatency {1: '5.12ms', 2: '6ms', ...}
let resultMap = {}

function parseLatencyFromString(resultString) {
	let lines = resultString.split("\n")
	let statementLine = lines[lines.length-2].trim() //last line is empty
	let latency = statementLine.split(" ")[0]

	return latency
}

function runPgbench (numClients, transactions) {
	return new Promise((resolve, reject) => {
		let clients = numClients
		let threads = 10
		let transactionsPerClient = transactions

		let command = 'pgbench -h ' + host + 
							 ' -p ' + port + 
							 ' -c ' + clients +
							 ' -j ' + threads +
							 ' -t ' + transactionsPerClient +
							 ' -f ' + file +
							 ' -r -n' // --connect'

		child = exec(command, function (error, stdout, stderr) {
		    if (error !== null) {
		         console.log('exec error: ' + error);
		         reject(error)
		    }
		    // console.log('stderr: ' + stderr);
		    
		    latency = parseLatencyFromString(stdout)
		    console.log('Average latency for ' + clients + ' clients: ' + latency + 'ms');
		    resolve(latency)
		});
	})
}

async function run() {
	const maxRuns = 5
	const offset = 0
	for(let i= 1 + offset; i <= maxRuns + offset; i++) {
		let clients = i * 5
		let latency = await runPgbench(clients, 20)

		resultMap[clients.toString()] = latency
	}

	console.log(JSON.stringify(resultMap, null, 2))	
}

run()