require('dotenv').config();

const request = require('request-promise');
const HTMLParser = require('node-html-parser');

// instantiate Graph DB connection
const neo4j = require('neo4j-driver').v1;
const driver = neo4j.driver(process.env.NEO_URI, neo4j.auth.basic(process.env.NEO, process.env.NEO_PWD));
const session = driver.session();

const initialLink = process.env.CSV_DATA_REMOTE;
function getLinks(link) {
    return request(link)
        .then(function (htmlString) {
            const root = HTMLParser.parse(htmlString);
            return Promise.all(root.querySelectorAll("a")
               .map(a => {
                   let abs_link = link + a.attributes.href;
                   if (abs_link.endsWith("\.csv")) {
                       return abs_link;
                   }
                   if (abs_link.endsWith("/")) {
                       return getLinks(abs_link);
                   }
                   else {
                       return [];
                   }
               }));
        })
        .catch(function (err) {
            console.log(link);
            console.log(`Error: ${err}`);
            return [];
        });
}

const insertData = (csv) =>
    `USING PERIODIC COMMIT 2000
    LOAD CSV FROM "${csv}" AS row
    MERGE (page1:Page {title: row[0]})
    MERGE (page2:Page {title: row[2]})
    CREATE (page1)-[l:IS_LINKED_TO {ts_from: row[1], ts_to: row[3], similarity : row[4]}]->(page2)`;

const index1 = "CREATE INDEX ON :Page(title)";
const index2 = "CREATE INDEX ON :Page(timestamp)";


console.time("loadData");
console.log("Create index on page title...");
session.run(index1)
    .then((res0) => {
        console.log("Done!\nCreate index on page timestamp...");
        return session.run(index2);
    })
    .then((res1) => {
        console.log("Done!\nInsert nodes and relationships...");
        return getLinks(initialLink)
            .then((result) =>
                Promise.all(result.reduce((acc, arr) => acc.concat(arr))
                    .filter((list) => list.length > 0)
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