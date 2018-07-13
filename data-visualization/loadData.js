// load system variables
require('dotenv').config();

// instantiate Graph DB connection
const fs = require('fs');
const path = require('path');
const neo4j = require('neo4j-driver').v1;
const driver = neo4j.driver(process.env.NEO_URI, neo4j.auth.basic(process.env.NEO, process.env.NEO_PWD));

const session = driver.session();

function getFiles(dir) {
    return fs.readdirSync(dir).map((file) => {
        const fullPath = path.join(dir, file);
        if (fs.statSync(fullPath).isFile() && file.endsWith("\.csv")) {
            return fullPath;
        }
        else {
            return ""
        }
    }).filter(f => f.length > 0);
}

const insertData = (csv) =>
    `USING PERIODIC COMMIT 2000
    LOAD CSV FROM "file://${csv}" AS row
    MERGE (page1:Page {title: row[0]})
    MERGE (page2:Page {title: row[2]})
    CREATE (page1)-[l:IS_LINKED_TO {ts_from: row[1], ts_to: row[3], similarity : row[4]}]->(page2)`;

const index1 = "CREATE INDEX ON :Page(title)";

console.time("loadData");
console.log("Create index on page title...");
session.run(index1)
    .then((res1) => {
        console.log("Done!\nInsert nodes and relationships...");
        return Promise.all(
                getFiles(process.env.CSV_DATA)
                .map((f) => session.run(insertData(f)))
        )
    })
    .then((res2) => {
        console.log("Done!\n");
        console.timeEnd("loadData");
        console.log("\nStopping the driver...");
        session.close();
        driver.close();
    })
    .catch((error) => { console.log(error); });
