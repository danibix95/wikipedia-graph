"use strict";
// load system variables
require('dotenv').config();

// instantiate Graph DB connection
const neo4j = require("neo4j-driver").v1;
const objects = require("./objects.js");

/* ===== QUERIES ===== */
const neighbours = `match (n:Page {title: {whichPage} })-[:IS_LINKED_TO]->(m:Page)
return distinct m.title as pTitle`;
const oneNode = `match (n:Page {title: {whichPage} })-[r:IS_LINKED_TO]->(m:Page)
where datetime(r.ts_from) < datetime({upTo}) and datetime(r.ts_to) < datetime({upTo})
with m, max(r.ts_from) as mts1, max(r.ts_to) as mts2
match l=(n:Page {title: {whichPage} })-[r:IS_LINKED_TO {ts_from:mts1, ts_to:mts2}]->(m:Page)
return l`;

/* =================== */

// pageTimestamp : pageTimestamp

class DB {
    constructor() {
        this.driver = neo4j.driver(
            process.env.NEO_URI,
            neo4j.auth.basic(process.env.NEO, process.env.NEO_PWD)
        );
    }

    getNewSession() {
        return this.driver.session()
    };

    retrieveAdjacentEdges(pageTitle, pageTimestamp) {
        const session = this.getNewSession();

        return session.run(
            neighbours,
            {
                whichPage : pageTitle,
            }
        )
        .then((listOfNeighbours) => {
            return Promise.all([pageTitle]
                .concat(listOfNeighbours.records.map(r => r.toObject().pTitle))
                .map((page) => session.run(
                    oneNode,
                    {
                        whichPage : page,
                        upTo : pageTimestamp
                    }
                ))
            )
        })
        .then((results) => {
            session.close();
            // first collect needed information from query result
            const rawData = results.map((result) =>
                result.records.map((r) => {
                    const path = r.toObject();
                    const nodeFrom = new objects.Node(path.l.start.properties.title);
                    const nodeTo = new objects.Node(path.l.end.properties.title);
                    const relationship = new objects.Relationship(
                        path.l.start.properties.title,
                        path.l.end.properties.title,
                        path.l.segments[0].relationship.properties.ts_from,
                        path.l.segments[0].relationship.properties.ts_to,
                        parseFloat(path.l.segments[0].relationship.properties.similarity),
                    );
                    return {
                        "nodeFrom" : nodeFrom,
                        "nodeTo" : nodeTo,
                        "link" : relationship
                    };
                })
            )
            .filter((list) => list.length > 0)
            .reduce((acc, x) => acc.concat(x), []);

            // build the json data for later visualization
            let nodes = {};
            let edges = [];

            rawData.forEach((e) => {
                // ensure that nodes are unique
                if (!nodes[e.nodeFrom.title]) nodes[e.nodeFrom.title] = e.nodeFrom;
                if (!nodes[e.nodeTo.title]) nodes[e.nodeTo.title] = e.nodeTo;
                edges.push(e.link);
            });

            let tmpData = Object.entries(nodes).map(([k, v]) => v.toCY());
            return tmpData.concat(edges.map((e) => e.toCY()));
        })
        .catch((error) => {
            console.log(error);
            session.close();
            // since an error has happened,
            // it is safe to return an empty object
            return {};
        });
    }
}

module.exports = DB;