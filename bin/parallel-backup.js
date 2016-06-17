#!/usr/bin/env node

"use strict"

var args = require('minimist')(process.argv.slice(2))
var fs = require('fs')
var spawn = require('child_process').spawn
var async = require('async')
var os = require('os')
var AWS = require('aws-sdk')

var exitCode = 0
var startTime = Date.now()

function usage() {
    console.error('')
    console.error('Usage: parallel-snapshot <region> <prefix> <s3dstbucket>')
    console.error(' - region: the dynamodb region with source tables')
    console.error(' - prefix: prefix of tables that should be backed up')
    console.error(' - s3dstbucket: s3 bucket into which the tables should be backed up to')
}

if (args.help) {
    usage()
    process.exit(0)
}

var region = args._[0]

if (!region) {
    console.error('Must provide dynamodb tables region')
    usage()
    process.exit(1)
}

var prefix = args._[1]

if (!prefix) {
    console.error('No prefix provided, backing all region tables up')
}

var s3dst = args._[2]
if (!s3dst) {
    console.error('Must provide an s3dstbucket')
    usage()
    process.exit(1)
}

var dynamoDB = new AWS.DynamoDB({ region: region })

var launchBackup = (tableList) => {
    
    async.forEach(tableList, (table, callback) => dynamoDB.describeTable(
        { TableName: table.name },
        (err, data) => {
            if(data) {
                table.weight = parseInt(data.Table.ItemCount)
                table.rps = Math.round(data.Table.ProvisionedThroughput.ReadCapacityUnits / 2)
            }
            callback(err)
        }), 
        err =>  {
            if (err) console.error(err)
            
            tableList.sort((a, b) => a.weight - b.weight)
            
            var cpus = os.cpus()
            async.forEachOf(cpus, (cpu, procNr, done) => {
                let procTables = []
                for (let i in tableList)
                    if (i % cpus.length === procNr)
                        procTables.push(tableList[i])
                
                let fileName = `/tmp/pb${procNr.toString()}`
                let fileStream = fs.createWriteStream(fileName, {'flags': 'w'})
                procTables.map(t => fileStream.write(`${t.name},${t.rps}\n`))
                fileStream.end()
                
                fileStream.on('finish', () => {
                    console.log(`Launching P${procNr} for ${procTables.length} tables from ${fileName}`)
                    
                    let proc = spawn('./bin/full-db-backup.js', [region, prefix, s3dst, fileName])
                    
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
                })
            }, (err) => {
                if (err) console.error(err)
                console.log(`All processes finished with code ${exitCode}`)
                console.log(`Took ${(Date.now() - startTime) / 1000} seconds`)
                process.exit(exitCode)
            })
    })
}

var tableList = []

var listTablesFromDB = (prefix, lastEvaluatedTableName) => {
    
    dynamoDB.listTables({
        Limit: 100,
        ExclusiveStartTableName: lastEvaluatedTableName
    }, (err, data) => {
        if (err) {
            console.log(err)
        } else {
            
            data.TableNames.map((table) => {
                if (!prefix || table.startsWith(prefix))
                    tableList.push({ name: table, weight: 0, rps: 1 })
            })
            
            if(data.LastEvaluatedTableName) {
                listTablesFromDB(prefix, data.LastEvaluatedTableName)
            } else {
                launchBackup(tableList)
            }
        }
    })
}

listTablesFromDB(prefix, null)
