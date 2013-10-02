'use strict';

var boltbus = require('../lib/boltbus.js'),
    chai    = require('chai'),
    AWS     = require('aws-sdk');

chai.should();

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

describe('boltbus', function () {
  it('should be true', function (done) {
    var bus = boltbus('testapp1', AWS);

    bus.on('connection', done);
    bus.connect();
  });
});
