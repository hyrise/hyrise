var exec = require('child_process').exec, child;

/**
 * This script can be used to benchmarks against the hyrise server.
 * pgbench is required to be preinstalled.
 *
 * Usage:
 *  - specify host, port and the benchmark sql script file
 * 	- adjust parameters in run() function 
 * 	- then run: node benchmark.js
 */

const host = 'localhost'
const port = '5432'
const file = 'server_benchmark_script.sql'

// map numClients --> tps {1: '14500.4324', 2: '15141.1234', ...}
let resultMap = {}

function parseLatencyFromString(resultString) {
	let lines = resultString.split("\n")
	let statementLine = lines[lines.length-2].trim() //last line is empty
	let latency = statementLine.split(" ")[0]

	console.log('Average latency is: ' + latency + 'ms');	

	return latency
}

function parseTpsFromString(resultString) {
	let lines = resultString.split("\n")
	let statementLine = lines[lines.length-2].trim() //last line is empty
	let tps = statementLine.split(" ")[2]

	console.log('Transactions per second: ' + tps);	

	return tps
}

function runPgbench (threads, numClients, transactionsPerClient, transactionsPerSec) {
	return new Promise((resolve, reject) => {

		let command = 'pgbench -h ' + host + 
							 ' -p ' + port + 
							 ' -c ' + numClients +
							 ' -j ' + threads +
							 ' -t ' + transactionsPerClient +
							 ' -f ' + file +
							 ' -R ' + transactionsPerSec
							 ' -r ' + // Report the average per-statement latency
							 ' -n ' // is necessary for custom test scenario without the standard tables pgbench_accounts, etc.

		child = exec(command, function (error, stdout, stderr) {
		    if (error !== null) {
		         console.log('exec error: ' + error);
		         reject(error)
		    }
		    
		    tps = parseTpsFromString(stdout)
		    resolve(tps)
		});
	})
}

async function run() {
	const maxRuns = 5
	const offset = 0
	const threads = 50
	const transactionsPerSec = 15000

	for(let i= 1 + offset; i <= maxRuns + offset; i++) {
		let clients = i * 10
		let tps = await runPgbench(threads, clients, 2500, transactionsPerSec)

		resultMap[clients.toString()] = tps
	}

	console.log(JSON.stringify(resultMap, null, 2))	
}

run()