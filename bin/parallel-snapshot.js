#!/usr/bin/env node

"use strict"

var args = require('minimist')(process.argv.slice(2))
var s3urls = require('s3urls')
var queue = require('queue-async')
var AWS = require('aws-sdk')
var fs = require('fs')
var spawn = require('child_process').spawn
var AWS = require('aws-sdk')

const S3_SEP = '/'
const DDB_REGION = 'eu-west-1'

function usage() {
    console.error('')
    console.error('Usage: parallell-snapshot <s3srcbucket> <s3dstbucket>')
    console.error(' - s3srcbucket: s3 source bucket where tables are backed up')
    console.error(' - s3dstbucket: s3 destination bucket where snapshot will be stored')
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
var s3srcbucket = s3urls.fromUrl(s3src)

var s3dst = args._[1]
if (!s3dst) {
    console.error('Must provide an s3dstbucket')
    usage()
    process.exit(1)
}

var dynamoDB = new AWS.DynamoDB({ region: DDB_REGION })

var readItemCount = (table) => {
    dynamoDB.describeTable({ TableName: table.name.slice(0, -1) }, (err, data) => {
        if (err) console.error(err)
        if(data) table.weight = parseInt(data.Table.ItemCount)
        else table.weight = 0
    })
}

var launchSnapshot = (tableList) => {
    
    // map item count to each table
    tableList.map(t => { readItemCount(t) })
    
    // wait up to 2 secs for async requests
    setTimeout(() => {
        
        tableList.sort((a, b) => a.weight - b.weight)
        
        var cpuNr = 4
        if (tableList.length < cpuNr)
            cpuNr = tableList.length
        
        for (let proc = 0; proc < cpuNr; proc++) {
            
            let procTables = []
            for (let i in tableList)
                if (i % cpuNr === proc)
                    procTables.push(tableList[i])
            
            let fileName = `/tmp/tl${proc.toString()}`
            let fileStream = fs.createWriteStream(fileName, {'flags': 'w'})
            procTables.map(t => fileStream.write(`${t}\n`))
            fileStream.end()
            
            console.log(`Launching P${proc} for ${procTables.length} tables from ${fileName}`)
            
            let process = spawn('./bin/full-db-snapshot.js', [s3src, s3dst, fileName])
            
            process.on('close', code => console.log(`P${proc} finished with code ${code}`))
            process.stdout.on('data', data => console.log(`P${proc}: ${data}`))
            process.stderr.on('data', data => console.error(`P${proc} ERR: ${data}`))
        }
    }, 2000)
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
        Prefix: ''
    }, (err, data) => {
        if (err) {
            console.log(err, err.stack)
        } else {
            data.CommonPrefixes.map((prefix) => {
                tableList.push({ name: prefix.Prefix, weight: 0 })
            })
            if (data.IsTruncated)
                listTablesFromS3(data.NextMarker)
            else
                launchSnapshot(tableList)
        }
    })
}

listTablesFromS3(null)
