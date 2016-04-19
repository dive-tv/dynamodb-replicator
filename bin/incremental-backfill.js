#!/usr/bin/env node

var args = require('minimist')(process.argv.slice(2));
var s3urls = require('s3urls');
var backfill = require('../s3-backfill');

function usage() {
    console.error('');
    console.error('Usage: incremental-backfill <tableinfo> <s3url> [readspersec]');
    console.error(' - tableinfo: the table to backup from, specified as `region/tablename`');
    console.error(' - s3url: s3 folder into which the record should be backed up to');
    console.error(' - readspersec: optional read items limit per second for DynamoDB scan');
}

if (args.help) {
    usage();
    process.exit(0);
}

var table = args._[0];

if (!table) {
    console.error('Must provide table information');
    usage();
    process.exit(1);
}

table = table.split('/');

var s3url = args._[1];

if (!s3url) {
    console.error('Must provide an s3url');
    usage();
    process.exit(1);
}

s3url = s3urls.fromUrl(s3url);

var readspersec = args._[2];

if (!readspersec) {
    readspersec = 'unlimited';
}

var config = {
    region: table[0],
    table: table[1],
    backup: {
        bucket: s3url.Bucket,
        prefix: s3url.Key
    },
    readspersec: readspersec
};

backfill(config, function(err) {
    if (err) {
        console.error(err);
        process.exit(1);
    }
});
