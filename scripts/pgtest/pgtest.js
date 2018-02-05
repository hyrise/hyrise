// const pg = require('pg');
//
// const config = {
//     host: 'localhost',
//     port: 5432
// };
//
// // pool takes the object above as parameter
// const pool = new pg.Pool(config);
//
//
// const connectionString = 'postgres://localhost:5432/postgres';
//
// pool.connect(connectionString, (err, client, done) => {
//
//     client.query('INSERT INTO items(text, complete) values($1, $2)', ['foo', 0]);
//
// });

const { Pool } = require('pg')

// pools will use environment variables
// for connection information
const pool = new Pool()

pool.query("PREPARE x FROM 'INSERT INTO foo values(?)'", [0], (err, res) => {
    console.log(err, res)
pool.end()
})
