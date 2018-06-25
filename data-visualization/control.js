const model = new (require("./db.js"))();

class Control {
    static home(request, response) {
        response.render("home", {});
    }

    static query(request, response) {
        response.render("home", {});
    }
}

module.exports = Control;
