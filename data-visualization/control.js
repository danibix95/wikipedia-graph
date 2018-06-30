"use strict";
const model = new (require("./db.js"))();

class Control {
    static home(request, response) {
        response.render("home", {});
    }

    static query(request, response) {
        console.log("Request arrived");

        // if (!request.body.w_page) {
        //     // TODO: need to update notify user of wrong input
        //     response.redirect("/missing");
        //     return response.end();
        // }
        // const pageTitle = request.body.w_page[0].toUpperCase()
        //                 + request.body.w_page.slice(1);
        const pageTitle = "";

        let pageTimestamp = new Date();
        if (request.body.up_to) {
            console.log(1);
        }
        else {
            console.log(2);
        }

        model.retrieveNeighbours(pageTitle, pageTimestamp)
            .then((json_data) => {
                console.log("Result: ", json_data);
                response.redirect("/");
                response.render("home", { data: json_data });
            })
            .catch((error) => {
                console.log("Error: ", error);
                response.redirect("/");
            });
    }
}

module.exports = Control;
