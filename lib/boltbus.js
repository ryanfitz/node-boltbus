/*
 * boltbus
 * https://github.com/ryanfitz/boltbus
 *
 * Copyright (c) 2013 Ryan Fitzgerald
 * Licensed under the MIT license.
 */

'use strict';

var EventEmitter = require('events').EventEmitter,
    _ = require('lodash');

var internals = {};

internals.outgoingTopicName = function () {
  return 'boltbus-outgoing';
};

internals.incomingQueueName = function (appID) {
  return 'boltbus-incoming-' + appID;
};

internals.isOutgoingTopic = function (topic) {
  var name = _.last(topic.split(':'));

  return name === internals.outgoingTopicName();
};

internals.createOutgoingTopic = function (AWS, callback) {
  var sns = new AWS.SNS();

  sns.createTopic({Name: internals.outgoingTopicName()}, function (err, data) {
    if(err) {
      return callback(err);
    }

    return callback(null, data.TopicArn);
  });
};

internals.findOrCreateOutgoingSnsTopic = function (AWS, callback) {
  var sns = new AWS.SNS();

  sns.listTopics({}, function (err, data) {
    if(err) {
      return callback(err);
    }

    var topic = _.chain(data.Topics).pluck('TopicArn').find(internals.isOutgoingTopic);

    if(topic) {
      return callback(null, topic);
    } else {
      return internals.createOutgoingTopic(AWS, callback);
    }
  });
};

internals.getQueueArn = function (sqs, queueUrl, callback) {
  return sqs.getQueueAttributes({QueueUrl : queueUrl, AttributeNames: ['QueueArn']}, function (err, data) {
    if(err) {
      return callback(err);
    }

    return callback(null, data.Attributes.QueueArn);
  });
};

internals.createSqsQueue = function (appID, AWS, callback) {
  var sqs = new AWS.SQS();
  sqs.createQueue({QueueName : internals.incomingQueueName(appID)}, function (err, data) {
    if(err) {
      return callback(err);
    }

    return internals.getQueueArn(sqs, data.QueueUrl, function (err, queueArn) {
      if(err) {
        return callback(err);
      }

      return callback(null, data.QueueUrl, queueArn);
    });
  });
};

internals.subscribeQueueToTopic = function (AWS, topicArn, queueArn,  callback) {
  var sns = new AWS.SNS();

  sns.subscribe({
    TopicArn : topicArn,
    Protocol : 'sqs',
    Endpoint : queueArn
  }, function (err, data) {
    if(err) {
      return callback(err);
    }

    sns.getSubscriptionAttributes({SubscriptionArn: data.SubscriptionArn}, callback);
  });
};

internals.createAppSubscription = function (appID, AWS, topicArn, callback) {
  return internals.createSqsQueue(appID, AWS, function (err, queueUrl, queueArn) {
    if(err) {
      return callback(err);
    }

    internals.setQueueAccessPolicy(AWS, topicArn, queueUrl, queueArn, function () {
      return internals.subscribeQueueToTopic(AWS, topicArn, queueArn, callback);
    });
  });
};

internals.setQueueAccessPolicy = function (AWS, topicArn, queueUrl, queueArn, callback) {
  var policy = {
    'Statement': [{
      'Sid': 'BoltbusSendPolicy001',
      'Effect': 'Allow',
      'Principal': { 'AWS': '*' },
      'Action': 'sqs:SendMessage',
      'Resource': queueArn,
      'Condition': {
        'ArnEquals': {
          'aws:SourceArn': topicArn
        }
      }
    }
  ]};

  var sqs = new AWS.SQS();

  sqs.setQueueAttributes({
    QueueUrl : queueUrl,
    Attributes : { Policy: JSON.stringify(policy) }
  }, callback);
};

internals.findAppSubscription = function (appID, AWS, topicArn, callback) {
  var sns = new AWS.SNS();

  sns.listSubscriptionsByTopic({TopicArn : topicArn}, function (err, data) {
    if(err) {
      return callback(err);
    }

    var name = internals.incomingQueueName(appID);

    var subscription = _.find(data.Subscriptions, function (sub) {
      return sub.Protocol === 'sqs' && name === _.last(sub.Endpoint.split(':'));
    });

    if(subscription) {
      return callback(null, subscription);
    } else {
      return internals.createAppSubscription(appID, AWS, topicArn, function (err, subscription) {
        if(err) {
          return callback(err);
        } else {
          return callback(null, subscription.Attributes);
        }
      });
    }
  });
};

internals.getQueueUrl = function (AWS, subscription, callback) {
  var sqs = new AWS.SQS();

  sqs.getQueueUrl({QueueName : _.last(subscription.Endpoint.split(':'))}, function (err, data) {
    if(err) {
      return callback(err);
    }

    return callback(null, data.QueueUrl);
  });
};

internals.connect = function (bus, appID, AWS, localemit, emitFunc, pollFunc) {
  return function () {

    internals.findOrCreateOutgoingSnsTopic(AWS, function (err, topic) {
      internals.findAppSubscription(appID, AWS, topic, function (err, subscription) {
        if(err) {
          localemit.call(bus, 'error', err);
        } else if (subscription) {
          bus.emit = emitFunc(subscription.TopicArn);
          pollFunc(subscription);

          localemit.call(bus, 'connection');
        }
      });
    });
  };
};

internals.emitMessages = function (bus, emit, messages) {
  _.each(messages, function (messageData) {
    var body = JSON.parse(messageData.Body);
    var msg = JSON.parse(body.Message);

    var args = [msg.event].concat(msg.data);
    emit.apply(bus, args);
  });
};

internals.destroyMessages = function (sqs, queueUrl, messages) {
  var entries = _.map(messages, function (msg) {
    return {Id: msg.MessageId, ReceiptHandle: msg.ReceiptHandle};
  });

  if(entries && entries.length > 0) {
    sqs.deleteMessageBatch({QueueUrl: queueUrl, Entries: entries}, function () {});
  }
};

module.exports = function(appID, AWS, options) {
  options = options || {};

  var bus = new EventEmitter();

  var emit = bus.emit;

  bus.emit = function () {
    emit('clientError', 'not connected to sns');
  };

  var sns = new AWS.SNS();
  var sqs = new AWS.SQS();

  var emitFunc = function (topicArn) {
    return function (event /* , args */ ) {
      //var args = arguments;
      var msg = {event: event, data : _.rest(arguments)};

      sns.publish({
        TopicArn : topicArn,
        Message : JSON.stringify(msg)
      }, function (err) {
        if(err) {
          emit('error', err);
        } else {
          //emit.apply(bus, args);
        }
      });
    };
  };

  var pollFunc = function (subscription) {
    internals.getQueueUrl(AWS, subscription, function (err, queueUrl) {
      var poller = function () {
        sqs.receiveMessage({
          QueueUrl : queueUrl,
          MaxNumberOfMessages : 10,
          WaitTimeSeconds : 10
        }, function (err, data) {
          if(err) {
            emit('error', err);
          }

          internals.emitMessages(bus, emit, data.Messages);
          internals.destroyMessages(sqs, queueUrl, data.Messages);
          return poller();
        });
      };

      poller();
    });
  };

  bus.connect = internals.connect(bus, appID, AWS, emit, emitFunc, pollFunc);

  return bus;
};
