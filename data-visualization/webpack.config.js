const path = require("path");

module.exports = {
  entry: "./public/js/loadGraph.js",
  mode: "production",
  output: {
    path: path.resolve(__dirname, 'public/js/'),
    filename: 'bundle.js'
  }
};