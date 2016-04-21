#!/usr/bin/env node

"use strict"

var args = require('minimist')(process.argv.slice(2))
var s3urls = require('s3urls')
var queue = require('queue-async')
var AWS = require('aws-sdk')
var fs = require('fs')
var spawn = require('child_process').spawn

const S3_SEP = '/'
const cpuNr = 4

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

var launchSnapshot = (tableList) => {
    
    let tableNr = tableList.length
    
    if (tableNr < cpuNr)
        cpuNr = tableNr
    let tablesPerCpu = Math.floor(tableNr / cpuNr)
    
    for (let cpu = 0; cpu < cpuNr; cpu++) {
        
        let commandTableNr
        if (cpu == cpuNr - 1)
            commandTableNr = tableNr - (tablesPerCpu * cpu)
        else
            commandTableNr = tablesPerCpu
        
        let tables = tableList.slice(cpu * tablesPerCpu, (cpu * tablesPerCpu) + commandTableNr)
        
        let fileName = `/tmp/ss${cpu.toString()}`
        let fileStream = fs.createWriteStream(fileName, {'flags': 'w'})
        tables.map(t => fileStream.write(`${t}\n`))
        fileStream.end()
        
        console.log(`Launching P${cpu} for ${tables.length} tables from ${fileName}`)
        
        let process = spawn('./bin/full-db-snapshot.js', [s3src, s3dst, fileName])
        
        process.on('close', code => console.log(`P${cpu} finished with code ${code}`))
        process.stdout.on('data', data => console.log(`P${cpu}: ${data}`))
        process.stderr.on('data', data => console.error(`P${cpu} ERR: ${data}`))
    }
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
                tableList.push(prefix.Prefix)
            })
            if (data.IsTruncated)
                listTablesFromS3(data.NextMarker)
            else
                launchSnapshot(tableList)
        }
    })
}

listTablesFromS3(null)
