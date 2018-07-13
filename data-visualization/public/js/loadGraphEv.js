// Cose Demo from Cytoscape
"use strict";

// style used by Cytoscape to draw the graph
let graphStyle;

async function loadStyle() {
    try {
        const response = await fetch('/js/graph-style.json', {mode: 'no-cors'});
        graphStyle = response.json();
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
            "/queryEv",
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

async function drawGraph(time) {
    const requestData = { time : time };

    const graphData = await loadData(requestData);

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
}

window.onload = () => {
    loadStyle();

    const range = document.getElementById("time");
    range.addEventListener("change", (ev) => {
            try {
                const timestamp = parseInt(range.value);
                drawGraph(timestamp);
            }
            catch (e) {
                console.error(`Impossible to parse given timestamp ${e}`);
            }
        });

    drawGraph(parseInt(range.value));
};