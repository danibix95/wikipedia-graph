// load system variables
require('dotenv').config();

// instantiate Graph DB connection
const fs = require('fs');
const path = require('path');
const neo4j = require('neo4j-driver').v1;
const driver = neo4j.driver(process.env.NEO_URI, neo4j.auth.basic(process.env.NEO, process.env.NEO_PWD));

const session = driver.session();

function walk(dir, fileList) {
    const files = fs.readdirSync(dir);
    // initialize if not already created
    files.forEach((file) => {
        const fullPath = path.join(dir, file);
        if (fs.statSync(fullPath).isDirectory()) {
            fileList = walk(fullPath, fileList);
        }
        else {
            if (file.endsWith("\.csv")) fileList.push(fullPath);
        }
    });
    return fileList;
}

// USING PERIODIC COMMIT 2000
// LOAD CSV FROM "file://${csv}" AS row
//
// MERGE (page1:Page {title: row[0], timestamp: row[1]})
// MERGE (page2:Page {title: row[2], timestamp: row[3]})
// MERGE (page1)-[l:IS_LINKED_TO]->(page2)
// ON CREATE SET l.timestamp = row[1], l.similarity = row[4];

const insertData = (csv) =>
    `USING PERIODIC COMMIT 2000 LOAD CSV FROM "file://${csv}" AS row MERGE (page1:Page {title: row[0], timestamp: row[1]}) MERGE (page2:Page {title: row[2], timestamp: row[3]}) MERGE (page1)-[l:IS_LINKED_TO]->(page2) ON CREATE SET l.timestamp = row[1], l.similarity = row[4];`;
const index1 = "CREATE INDEX ON :Page(title)";
const index2 = "CREATE INDEX ON :Page(timestamp)";

console.time("loadData");
console.log("Create index on page title...");
session.run(index1)
    .then((res1) => {
        console.log("Done!\nCreate index on page timestamp...");
        return session.run(index2);
    })
    .then((res1) => {
        console.log("Done!\nInsert nodes and relationships...");
        return Promise.all(
                walk(process.env.CSV_DATA, [])
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
