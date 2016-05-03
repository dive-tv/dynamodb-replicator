#!/usr/bin/env node

"use strict"

var args = require('minimist')(process.argv.slice(2))
var s3urls = require('s3urls')
var AWS = require('aws-sdk')
var fs = require('fs')
var spawn = require('child_process').spawn
var async = require('async')
var os = require('os')
var d3 = require('d3-queue')

const S3_SEP = '/'

var exitCode = 0
var startTime = Date.now()

function usage() {
    console.error('')
    console.error('Usage: parallel-recover <region> <s3srcbucket> [-s snapshotdate]')
    console.error(' - region: dynamodb region where tables will be restored')
    console.error(' - s3srcbucket: s3 source bucket where tables are backed up')
    console.error(' - snapshotdate: optional snapshot date to recover from, as "yyyymmdd"')
}

if (args.help) {
    usage()
    process.exit(0)
}

var region = args._[0]
if (!region) {
    console.error('Must provide a DynamoDB region')
    usage()
    process.exit(1)
}

var s3src = args._[1]
if (!s3src) {
    console.error('Must provide an s3srcbucket')
    usage()
    process.exit(1)
}
var s3srcbucket = s3urls.fromUrl(s3src)

var snapshot = args.s
if (snapshot) snapshot = snapshot.toString().trim() + S3_SEP

var dynamoDB = new AWS.DynamoDB({ region: region })

var launchRecover = (tableList, snapshot) => {
    
    var cpus = os.cpus()
    var queue = d3.queue(cpus.length)
    async.forEachOf(cpus, (cpu, procNr, done) => {
        let procTables = []
        for (let i in tableList)
            if (i % cpus.length === procNr)
                procTables.push(tableList[i])
        
        let fileName = `/tmp/pr${procNr.toString()}`
        let fileStream = fs.createWriteStream(fileName, {'flags': 'w'})
        procTables.map(t => fileStream.write(`${t.name}\n`))
        fileStream.end()
        
        console.log(`Launching P${procNr} for ${procTables.length} tables from ${fileName}`)
        
        var args = [s3src, region, '-f', fileName]
        if (snapshot) args.push('-s')
        let proc = spawn('./bin/full-db-recover.js', args)
        
        proc.on('exit', (code, signal) => {
            if (code !== null) {
                exitCode += code
                console.log(`P${procNr} finished with code ${code}`)
            }
            if (signal !== null) {
                console.log(`P${procNr} killed by signal ${signal}`)
            }
            done()
        })
        proc.stdout.on('data', data => console.log(`P${procNr}: ${data}`))
        proc.stderr.on('data', data => console.error(`P${procNr} ERR: ${data}`))
    }, (err) => {
        if (err) console.error(err)
        console.log(`All processes finished with code ${exitCode}`)
        console.log(`Took ${(Date.now() - startTime) / 1000} seconds`)
        process.exit(exitCode)
    })
}

var tableList = []

var s3 = new AWS.S3()
var listTablesFromS3 = (lastKey) => {
    
    s3.listObjects({
        Bucket: s3srcbucket.Bucket,
        Delimiter: S3_SEP,
        EncodingType: 'url',
        Marker: lastKey,
        MaxKeys: 100,
        Prefix: snapshot
    }, (err, data) => {
        if (err) {
            console.log(err, err.stack)
        } else {
            if (snapshot)
                data.Contents.map(item => tableList.push({ name: item.Key, weight: 0 }))
            else
                data.CommonPrefixes.map(prefix => tableList.push({ name: prefix.Prefix, weight: 0 }))
            
            if (data.IsTruncated)
                listTablesFromS3(data.NextMarker)
            else
                launchRecover(tableList, snapshot)
        }
    })
}

listTablesFromS3(null)
