var request = require("request"),
    es = require("elasticsearch"),
    EventEmitter = require("events").EventEmitter;

var esClient = es.Client({ host: "http://localhost:9200"});

var message_queue = new EventEmitter;

function parseCLArgs(argv) {
    var args = {};

    argv.forEach(function (arg) {
        var name,
            value;
        if (name = arg.match(/\-\-([a-zA-Z]+)/)) {
            name = name[1].toLowerCase();
            args[name] = "true";//for consistency keep this as a string

            if (value = arg.split("=")[1]) {
                args[name] = value;
            }
        }
    });

    return args;
}

args = parseCLArgs(process.argv);

esClient.search({
        index: "h2o",
        type: "tags",
        scroll: "15s",
        size: "20",
        q: "root_type:location"
    },
    parseTags
);


function parseTags(err, response, status) {
    var scrollId,
        location_tags = [];

    if (err) {
        throw err;
    }

    scrollId = response._scroll_id;

    parseTagsInner(err, response, status);

    function parseTagsInner (err, response, status) {
        if (status == 404) {
            message_queue.emit("tags processed", location_tags);
            return;
        }

        if (err) {
            throw err;
        }

        if (response.hits.hits.length > 0) {
            location_tags = location_tags.concat(response.hits.hits.map(function (hit) {
                return hit._source;
            }));

            esClient.scroll({scrollId: scrollId}, parseTagsInner);
        } else {
            message_queue.emit("tags processed", location_tags);
        }
    }
}




message_queue.on("tags processed", function (data) {
    console.log(data.length);
});