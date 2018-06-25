// load system variables
require('dotenv').config();

// instantiate Graph DB connection
const neo4j = require('neo4j-driver').v1;

/* ===== QUERIES ===== */

/* =================== */

class DB {
    constructor() {
        const driver = neo4j.driver(
            process.env.NEO_URI,
            neo4j.auth.basic(process.env.NEO, process.env.NEO_PWD)
        );

        this.session = driver.session();
    }
}

module.exports = DB;