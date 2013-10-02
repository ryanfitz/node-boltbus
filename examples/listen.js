/*
 * boltbus
 * https://github.com/ryanfitz/boltbus
 *
 * Copyright (c) 2013 Ryan Fitzgerald
 * Licensed under the MIT license.
 */

'use strict';

var boltbus = require('../lib/boltbus'),
    AWS     = require('aws-sdk');

AWS.config.loadFromPath(process.env.HOME + '/.ec2/credentials.json');

var bus = boltbus('listenApp', AWS);

bus.on('error', function (err) {
  console.log('error', err);
});

bus.on('connection', function () {
  console.log('Listen example connected');
});

bus.on('user:created', function (data) {
  console.log('list app received event user:created', data);
});

bus.connect();
