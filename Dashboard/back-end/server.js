var express = require('express');
var redis = require('redis');
var http = require('http');
rc = redis.createClient(6379, "localhost");
var app = require('express')();
var server = require('http').Server(app);
var io = require('socket.io')(server);
server.listen(3001);

rc.on("connect", function() {
    rc.subscribe("tweet_prediction");
    console.log("rc connect event");
});

rc.on("message", function (channel, message) {
    console.log("Sending: " + message);
    io.emit('tweet_prediction', message);
});