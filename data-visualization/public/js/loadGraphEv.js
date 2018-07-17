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

async function drawGraph(time, loadingElement) {
    loadingElement.style.display = "block";
    const requestData = { time : time };

    const graphData = await loadData(requestData);
    loadingElement.style.display = "none";

    const cy = window.cy = cytoscape({
        container: document.getElementById("graph-container"),

        elements: graphData,
        style: graphStyle,

        /*layout: {
            name: 'cose',
            idealEdgeLength: 100,
            nodeOverlap: 32,
            refresh: 20,
            fit: true,
            padding: 30,
            randomize: true,
            componentSpacing: 50,
            nodeRepulsion: 250000,
            edgeElasticity: 200,
            nestingFactor: 1.2,
            gravity: 200,
            numIter: 2000,
            initialTemp: 700,
            coolingFactor: 0.95,
            minTemp: 1.0
        }*/

        layout: {
            name: "cose-bilkent",
            nodeDimensionsIncludeLabels: false,
            nodeOverlap: 32,
            refresh: 30,
            fit: true,
            padding: 20,
            randomize: true,
            nodeRepulsion: 500000,
            idealEdgeLength: 80,
            edgeElasticity: 0.3,
            nestingFactor: 1.1,
            gravity: 80,
            numIter: 2500,
            initialTemp: 1000,
            coolingFactor: 0.95,
            minTemp: 1.0,
            animate: false,
            initialEnergyOnIncremental: 0.5
        }
    });
}

function printableDate(timestamp) {
    const date = new Date(timestamp);
    return `Current Date: ${date.getFullYear()}-${date.getMonth() + 1}-${date.getDate()}`;
}

window.onload = () => {
    loadStyle();

    const dateDisplay = document.getElementById("selected-date");
    const spinner = document.getElementById("spinner");

    const range = document.getElementById("time");
    range.addEventListener("change", (ev) => {
            try {
                const timestamp = parseInt(range.value);
                drawGraph(timestamp, spinner);
            }
            catch (e) {
                console.error(`Impossible to parse given timestamp ${e}`);
            }
        });
    range.addEventListener("input", (ev) => {
        try {
            dateDisplay.innerText = printableDate(parseInt(range.value));
        }
        catch (e) {
            console.error(`Impossible to parse given timestamp ${e}`);
        }
    });

    drawGraph(parseInt(range.value), spinner);
};
