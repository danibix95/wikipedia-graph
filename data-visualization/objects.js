"use strict";

class Node {
    constructor(title) {
        this.title = title;
    }

    equals(otherNode) {
        if (typeof otherNode !== Node) {
            return false;
        }
        return this.title === otherNode.title;
    }

    toCY() {
        return {
            "data" : {
                "id" : this.title,
            },
            "position": {},
            "group" : "nodes",
            "removed": false,
            "selected": false,
            "selectable": true,
            "locked": false,
            "grabbed": false,
            "grabbable": true,
            "classes": "nodes"
        }
    }
}

class Relationship {
    constructor(source, target, timestampFrom, timestampTo, similarity) {
        this.source = source;
        this.target = target;
        this.timestampFrom = timestampFrom;
        this.timestampTo = timestampTo;
        this.similarity = similarity;
    }

    toCY() {
        return {
            "data": {
                "source": this.source,
                "target": this.target,
                "tsFrom": this.timestampFrom,
                "tsTo": this.timestampTo,
                "similarity": this.similarity,
                "show_sim": this.similarity.toFixed(3)
            },
            "position": {},
            "group": "edges",
            "removed": false,
            "selected": false,
            "selectable": true,
            "locked": false,
            "grabbed": false,
            "grabbable": true,
            "classes": "edges"
        }
    }
}

module.exports = {
    Node : Node,
    Relationship : Relationship
};