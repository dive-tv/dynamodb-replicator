#!/usr/bin/env node

var args = require('minimist')(process.argv.slice(2));
var s3urls = require('s3urls');
var backfill = require('../s3-backfill');
var Dyno = require('dyno');
var queue = require('queue-async');

function usage() {
    console.error('');
    console.error('Usage: full-db-backup <region> <prefix> <s3url> [readspersec]');
    console.error(' - region: the dynamodb region with source tables');
    console.error(' - prefix: prefix of tables that should be backed up');
    console.error(' - s3url: s3 folder into which the tables should be backed up to');
    console.error(' - readspersec: optional read items limit per second for DynamoDB scan');
}

if (args.help) {
    usage();
    process.exit(0);
}

var region = args._[0];

if (!region) {
    console.error('Must provide dynamodb tables region');
    usage();
    process.exit(1);
}

var prefix = args._[1];

if (!prefix) {
    console.error('No prefix provided, backing all region tables up');
}

var s3url = args._[2];

if (!s3url) {
    console.error('Must provide an s3url');
    usage();
    process.exit(1);
}

s3url = s3urls.fromUrl(s3url);

var readspersec = args._[3];

if (!readspersec) {
    readspersec = 'unlimited';
}

var dyno = Dyno({
  region: region,
  table: 'NO-TABLE'
});

var queue = queue();

var dumpTable = (table) => {
    
    queue.defer((next) => {
        console.log("Dumping " + table);
        backfill({
            region: region,
            table: table,
            backup: {
                bucket: s3url.Bucket,
                prefix: s3url.Key
            },
            readspersec: readspersec
        }, (err) => {
            if (err)
                console.error(err);
            console.log("Finished " + table + " backup");
            next();
        });
    });    
}

var listTables = (prefix, lastEvaluatedTableName) => {
    
    dyno.listTables({
        Limit: 100,
        ExclusiveStartTableName: lastEvaluatedTableName
    }, (err, data) => {
        if (err) {
            console.log(err, err.stack);
        } else {
            
            data.TableNames.map((table) => {
                if (!prefix || table.startsWith(prefix))
                    dumpTable(table);
            });
            
            if(data.LastEvaluatedTableName) {
                listTables(prefix, data.LastEvaluatedTableName);
            } else {
                queue.awaitAll((err, data) => { 
                    if (err) console.log(err);
                    console.log("Backup finished");
                });
            }
        }
    });
};

listTables(prefix, null);
