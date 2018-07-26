// Daniele Bissoli - 197810
// This script is meant to load the visualization tool
// for showing the evolution of Wikipedia graph
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

        layout: {
            name: 'cose',
            idealEdgeLength: 100,
            nodeOverlap: 32,
            refresh: 20,
            fit: true,
            padding: 30,
            randomize: true,
            componentSpacing: 50,
            nodeRepulsion: 250000,
            edgeElasticity: 250,
            nestingFactor: 1.2,
            gravity: 80,
            numIter: 2000,
            initialTemp: 720,
            coolingFactor: 0.95,
            minTemp: 1.0
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
