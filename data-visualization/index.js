/* Daniele Bissoli */
"use strict";

const express = require("express");
const path = require("path");
const compression = require("compression");
const minify = require("express-minify");

const server = require("./control.js");

process.title = "wikipedia-graph";
// process.env.NODE_ENV = "production";

const app = express();
app.set("title", "Wikipedia Graph Visualizer");
app.set("port", (process.env.PORT || 20001));
app.set("view engine", "pug");
app.use(compression());
app.use(minify());

/* =================== */
app.get("/", server.home);
app.post("/query/", server.query);
/* =================== */
app.use("/", express.static(path.join(__dirname, "public"), {dotfiles: "deny"}));
app.use("/lib/pavilion", express.static(path.join(__dirname, "node_modules/pavilion/dist"), {dotfiles: "deny"}));

app.listen(process.env.PORT || app.get("port"), function () {
    console.log("Website listening on port " + app.get("port"));
});
