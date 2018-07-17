/* Daniele Bissoli */
"use strict";

const path = require("path");
const express = require("express");
const minify = require("express-minify");
const compression = require("compression");

const server = require("./control.js");

process.title = "wikipedia-graph";
// process.env.NODE_ENV = "production";

const app = express();
app.set("title", "Wikipedia Graph Visualizer");
app.set("port", (process.env.PORT || 20001));
app.set("view engine", "pug");
// do not invert the following two lines
// app.use(compression());
// app.use(minify());
app.use(express.json());

/* =================== */
app.get("/", server.home);
app.get("/evolution", server.evolution);
app.post("/query", server.query);
app.post("/queryEv", server.graph);
/* =================== */
app.use("/", express.static(path.join(__dirname, "public"), {dotfiles: "deny"}));
app.use("/lib/pavilion", express.static(path.join(__dirname, "node_modules/pavilion/dist"), {dotfiles: "deny"}));
app.use("/lib/cytoscape", express.static(path.join(__dirname, "node_modules/cytoscape/dist"), {dotfiles: "deny"}));
app.use("/lib/cytoscape/cose-bilkent.js", express.static(path.join(__dirname, "node_modules/cytoscape-cose-bilkent/cytoscape-cose-bilkent.js"), {dotfiles: "deny"}));

app.listen(process.env.PORT || app.get("port"), function () {
    console.log("Website listening on port " + app.get("port"));
});
