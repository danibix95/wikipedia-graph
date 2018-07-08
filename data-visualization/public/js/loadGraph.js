// Cose Demo from Cytoscape
"use strict";

// style used by Cytoscape to draw the graph
let graphStyle;

async function loadStyle() {
    try {
        const response = await fetch('/js/cy-style.json', {mode: 'no-cors'});
        graphStyle = response.json();
        console.log("Style has been retrieved!");
    }
    catch (error) {
        console.error(`Error retrieving graph style\n${error}`);
    }
}

async function loadData(requestData) {
    console.log("Data have been requested!");

    const headers = new Headers();
    headers.append("Accept", "application/json");
    headers.append("Content-Type", "application/json");

    try {
        const response = await fetch(
            "/query",
            {
                method: "POST",
                headers: headers,
                body: JSON.stringify(requestData),
            }
        );

        return response.json();
    }
    catch (error) {
        console.log(`Error retrieving graph data\n${error}`);
        return {};
    }
}

async function drawGraph(cached = false) {
    const requestData = {
        pageTitle : document.getElementById("w_page").value,
        pageTimestamp : document.getElementById("up_to").value
    };

    let graphData;
    if (cached) {
        graphData = JSON.parse(window.localStorage.getItem("cacheData"));
    }
    else {
        graphData = await loadData(requestData);
    }

    console.log("Data have been received!"/*, graphData*/);

    const cy = window.cy = cytoscape({
        container: document.getElementById("graph-container"),

        elements: graphData,
        style: graphStyle,

        layout: {
            name: 'cose',
            idealEdgeLength: 100,
            nodeOverlap: 32,
            refresh: 20,
            fit: true,
            padding: 30,
            randomize: true,
            componentSpacing: 200,
            nodeRepulsion: 400000,
            edgeElasticity: 120,
            nestingFactor: 5,
            gravity: 80,
            numIter: 1000,
            initialTemp: 200,
            coolingFactor: 0.95,
            minTemp: 1.0
        }
    });
    console.log("Graph has been drawn!");

    if (!cached) window.localStorage.setItem("cacheData", JSON.stringify(graphData));
}

window.onload = () => {
    loadStyle().then(() => {
        if (window.localStorage.getItem("cacheData") !== undefined) {
            drawGraph(true);
        }
    });


    document.getElementById("visualize")
            .addEventListener("click", (ev) => {
                drawGraph();
            });
};