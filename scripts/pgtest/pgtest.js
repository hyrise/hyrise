const { Pool } = require('pg')

// pools will use environment variables
// for connection information
const pool = new Pool()

pool.query("PREPARE x FROM 'INSERT INTO foo values(?)'", [0], (err, res) => {
    console.log(err, res)
pool.end()
})
