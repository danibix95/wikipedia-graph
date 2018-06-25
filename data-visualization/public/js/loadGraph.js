// Cose Demo from Cytoscape
const cytoscape = require("cytoscape");

Promise.all([
    fetch('js/cy-style.json', {mode: 'no-cors'})
        .then(function(res) {
            return res.json()
        }),
    fetch('js/data.json', {mode: 'no-cors'})
        .then(function(res) {
            return res.json()
        })
])
.then(function(dataArray) {
    const cy = window.cy = cytoscape({
        container: document.getElementById("graph-container"),

        layout: {
            name: 'cose',
            idealEdgeLength: 100,
            nodeOverlap: 20,
            refresh: 20,
            fit: true,
            padding: 30,
            randomize: true,
            componentSpacing: 100,
            nodeRepulsion: 400000,
            edgeElasticity: 100,
            nestingFactor: 5,
            gravity: 80,
            numIter: 1000,
            initialTemp: 200,
            coolingFactor: 0.95,
            minTemp: 1.0
        },

        style: dataArray[0],

        elements: dataArray[1]
    });
});