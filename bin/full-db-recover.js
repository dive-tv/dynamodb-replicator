#!/usr/bin/env node

"use strict"

var args = require('minimist')(process.argv.slice(2))
var s3urls = require('s3urls')
var recover = require('../s3-recover')
var fs = require('fs')
var https = require('https')
var AWS = require('aws-sdk')
var Dyno = require('dyno')
var queue = require('queue-async')

const S3_SEP = '/'

function usage() {
    console.error('')
    console.error('Usage: full-db-recover <s3src> <dstregion> [-p prefix | -f tablesfile [-s]]')
    console.error(' - s3src: s3 bucket where the DB backup is stored')
    console.error(' - dstregion: the dynamodb region with destination tables')
    console.error(' - prefix: optional prefix to filter tables loaded from S3')
    console.error(' - tablesfile: optional file with list of tables')
    console.error('           -s: optional flag, indicates that file tables are loaded from a snapshot')
}

if (args.help) {
    usage()
    process.exit(0)
}

var s3url = args._[0]

if (!s3url) {
    console.error('Must provide an s3src')
    usage()
    process.exit(1)
}
var s3src = s3urls.fromUrl(s3url)

var region = args._[1]

if (!region) {
    console.error('Must provide dynamodb tables region')
    usage()
    process.exit(1)
}

var tablesfile = args.f
if (tablesfile) tablesfile = tablesfile.trim()

var prefix = args.p
if (prefix) prefix = prefix.trim()

if (tablesfile && prefix) {
    console.error('At most one of -t and -p can be provided')
    usage()
    process.exit(1)
}

var issnapshot = args.s

// https://github.com/aws/aws-sdk-js/issues/862
var ddbAgent = new https.Agent({
    rejectUnauthorized: true,
    keepAlive: true,
    ciphers: 'ALL',
    secureProtocol: 'TLSv1_method'
})

var dyno = Dyno({
    region: region,
    table: 'NO-TABLE',
    httpOptions: {
        agent: ddbAgent
    }
})

var queue = queue()
var waitForCompletion = () => {
    queue.awaitAll((err, data) => { 
        if (err) {
            console.error(err)
            process.exit(1)
        } else {
            process.exit(0)
        }
    })
}

var recoverTable = (prefix) => {
    prefix = prefix.trim()
    if (prefix.length > 0) {
        var table
        if (prefix.endsWith('/'))
            table = prefix.slice(0, -1)
        else
            table = prefix
        if (issnapshot)
            table = table.slice(table.indexOf('/') + 1, table.length)
        console.log(`Recovering ${table}`)
        queue.defer((next) => {
            recover({
                region: region,
                table: table,
                backup: {
                    bucket: s3src.Bucket,
                    prefix: prefix
                },
                gzipped: issnapshot,
                httpOptions: {
                    agent: ddbAgent
                }
            }, (err) => {
                if (err)
                    console.error(err)
                console.log(`Finished ${table} recovery`)
                next()
            })
        })
    }
}

var listTablesFromFile = (fileName) => {
    
    let data = fs.readFileSync(fileName)
    data.toString().split('\n').map(table => recoverTable(table))
    waitForCompletion()
}

var s3 = new AWS.S3()
var listTablesFromS3 = (prefix, lastKey) => {
    
    s3.listObjects({
        Bucket: s3src.Bucket,
        Delimiter: S3_SEP,
        EncodingType: 'url',
        Marker: lastKey,
        MaxKeys: 100,
        Prefix: prefix
    }, (err, data) => {
        if (err) {
            console.log(err, err.stack)
        } else {
            data.CommonPrefixes.map(cp => recoverTable(cp.Prefix))
            if (data.IsTruncated) listTablesFromS3(prefix, data.NextMarker)
            else waitForCompletion()
        }
    })
}

if (tablesfile)
    listTablesFromFile(tablesfile)
else
    listTablesFromS3(prefix, null)
