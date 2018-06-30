"use strict";
// load system variables
require('dotenv').config();

// instantiate Graph DB connection
const neo4j = require("neo4j-driver").v1;
const objects = require("./objects.js");

/* ===== QUERIES ===== */
const oneNode = `match (n:Page {title: 'Clock'})-[r:IS_LINKED_TO]->(m:Page)
where r.ts_from < '2006-08-28T23:19:26.000+02:00' and r.ts_to < '2006-08-28T23:19:26.000+02:00'
with m, max(r.ts_from) as mts1, max(r.ts_to) as mts2
match l=(n:Page {title: 'Clock'})-[r:IS_LINKED_TO {ts_from:mts1, ts_to:mts2}]->(m:Page)
return l
limit 20`;

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

    retrieveNeighbours(pageTitle, pageTimestamp) {
        const session = this.getNewSession();
        return session.run(
            oneNode,
            {
                whichPage : pageTitle
            }
        )
        .then((result) => {
            session.close();

            // first collect needed information from query result
            const rawData = result.records.map((r) => {
                const path = r.toObject();
                const nodeFrom = new objects.Node(path.l.start.properties.title);
                const nodeTo = new objects.Node(path.l.end.properties.title);
                const relationship = new objects.Relationship(
                    path.l.start.properties.title,
                    path.l.end.properties.title,
                    path.l.segments[0].relationship.properties.ts_from,
                    path.l.segments[0].relationship.properties.ts_to,
                    path.l.segments[0].relationship.properties.similarity,
                );
                return {
                    "nodeFrom" : nodeFrom,
                    "nodeTo" : nodeTo,
                    "link" : relationship
                }
            });

            // build the json data for later visualization
            let nodes = {};
            let edges = [];

            rawData.forEach((e) => {
                // ensure tha nodes are unique
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