require('dotenv').config();

const createFS = require("nhdfs").createFS;

// instantiate Graph DB connection
const neo4j = require('neo4j-driver').v1;
const driver = neo4j.driver(process.env.NEO_URI_REMOTE, neo4j.auth.basic(process.env.NEO, process.env.NEO_PWD));
const session = driver.session();

const location = process.env.CSV_DATA_REMOTE;

const hdfsMaster = process.env.HDFS_HOST;
const hdfsPort = parseInt(process.env.HDFS_PORT);
const fs = createFS({ service: hdfsMaster, port: hdfsPort });

const insertData = (csv) =>
    `USING PERIODIC COMMIT 2000
    LOAD CSV FROM "${csv}" AS row
    MERGE (page1:Page {title: row[0]})
    MERGE (page2:Page {title: row[2]})
    CREATE (page1)-[l:IS_LINKED_TO {ts_from: row[1], ts_to: row[3], similarity : row[4]}]->(page2)`;

const index1 = "CREATE INDEX ON :Page(title)";

// fs.list(location)
//     .then((list) =>
//         Promise.all(
//             list.map((l) => `hdfs://${hdfsMaster}:${hdfsPort}${l.path}`)
//                 .filter((p) => p.endsWith("\.csv"))
//                 .map((p) => console.log(p))
//         )
//     )
//     .catch((e) => console.log(e));

console.time("loadData");
console.log("Create index on page title...");
session.run(index1)
    .then((res1) => {
        console.log("Done!\nInsert nodes and relationships...");
        return fs.list(location)
            .then((list) =>
                Promise.all(
                    list.map((l) => `hdfs://${hdfsMaster}:${hdfsPort}${l.path}`)
                        .filter((p) => p.endsWith("\.csv"))
                        .map((f) => session.run(insertData(f)))
                )
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
