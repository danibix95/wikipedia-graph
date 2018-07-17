"use strict";
const model = new (require("./db.js"))();

class Control {
    static home(request, response) {
        response.render("home", {});
    }

    static query(request, response) {
        if (!request.body.pageTitle || request.body.pageTitle.length === 0) {
            // TODO: need to update notify user of wrong input
            response.redirect("/missing");
            return response.end();
        }
        const pageTitle = request.body.pageTitle.toString();

        const pageTimestamp = request.body.pageTimestamp
            ? new Date(request.body.pageTimestamp).toISOString()
            : new Date().toISOString();

        model.retrieveAdjacentEdges(pageTitle, pageTimestamp)
            .then((json_data) => {
                response.send(json_data);
            })
            .catch((error) => {
                console.log("Error: ", error);
                response.redirect("/");
            });
    }

    static graph(request, response) {
        const time = request.body.time
            ? new Date(request.body.time).toISOString()
            : new Date().toISOString();

        model.retrieveGraph(time)
            .then((json_data) => {
                response.send(json_data);
            })
            .catch((error) => {
                console.log("Error: ", error);
                response.redirect("/");
            });
    }

    static evolution(request, response) {
        model.retrieveRange()
            .then((data) => {
                response.render("evolution", data);
            })
            .catch((error) => {
                console.error(error);
                response.render("evolution", { msg: "Impossible to load range timestamp!"});
            })
    }
}

module.exports = Control;
