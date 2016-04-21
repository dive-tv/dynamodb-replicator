#!/usr/bin/env node

"use strict"

var args = require('minimist')(process.argv.slice(2))
var s3urls = require('s3urls')
var snapshot = require('../s3-snapshot')
var queue = require('queue-async')
var AWS = require('aws-sdk')
var dateformat = require('dateformat')
var fs = require('fs')

const S3_SEP = '/'
const CONCURRENCY = 1

function usage() {
    console.error('')
    console.error('Usage: full-db-snapshot <s3srcbucket> <s3dstbucket> [tablefile] [snapshottag]')
    console.error(' - s3srcbucket: s3 source bucket where tables are backed up')
    console.error(' - s3dstbucket: s3 destination bucket where snapshot will be stored')
    console.error(' - tablefile: optional file path with a list of tables to be processed')
    console.error(' - snapshottag: optional tag for the snapshot. "yyyymmdd" by default')
}

if (args.help) {
    usage()
    process.exit(0)
}

var s3src = args._[0]
if (!s3src) {
    console.error('Must provide an s3srcbucket')
    usage()
    process.exit(1)
}
s3src = s3urls.fromUrl(s3src)

var s3dst = args._[1]
if (!s3dst) {
    console.error('Must provide an s3dstbucket')
    usage()
    process.exit(1)
}
s3dst = s3urls.fromUrl(s3dst)

var tablesfile = args._[2]

var snapshottag = args._[3]
if (!snapshottag) {
    snapshottag = dateformat(new Date(), "yyyymmdd")
    console.log(`Using default snapshot tag: ${snapshottag}`)
}

var queue = queue(CONCURRENCY)

var snapshotTable = (table) => {
    
    queue.defer((next) => {
        let tag = [snapshottag, table].join(S3_SEP)
        if (tag.endsWith(S3_SEP))
            tag = tag.slice(0, -1)
        console.log(`Creating snapshot ${tag}`)
        snapshot({
            //log: '',
            //logger: '',
            source: {
                bucket: s3src.Bucket,
                prefix: table
            },
            destination: {
                bucket: s3dst.Bucket,
                key: tag
            },
            maxRetries: 1000
        }, (err) => {
            if (err)
                console.error(err)
            console.log(`Finished ${table} snapshot`)
            next()
        })
    })    
}

var listTablesFromFile = (fileName) => {
    
    fs.readFile(fileName, (err, data) => {
        
        if(err)  {
            console.log(err, err.stack)
        } else {
            data.toString().split('\n').map(table => {
                table = table.trim()
                if (table.length > 0)
                    snapshotTable(table)
            })
        }
    })
}

var s3 = new AWS.S3()
var listTablesFromS3 = (lastKey) => {
    
    s3.listObjects({
        Bucket: s3src.Bucket,
        Delimiter: S3_SEP,
        EncodingType: 'url',
        Marker: lastKey,
        MaxKeys: 100,
        Prefix: ''
    }, (err, data) => {
        if (err) {
            console.log(err, err.stack)
        } else {
            data.CommonPrefixes.map((prefix) => { 
                snapshotTable(prefix.Prefix)
            })
            if (data.IsTruncated) {
                listTablesFromS3(data.NextMarker)
            }
        }
    })
}

if (tablesfile)
    listTablesFromFile(tablesfile)
else
    listTablesFromS3(null)

queue.awaitAll((err, data) => { 
    if (err) console.error(err)
})