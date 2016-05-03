var Dyno = require('dyno')
var AWS = require('aws-sdk')
var s3scan = require('s3scan')
var stream = require('stream')
var queue = require('queue-async')
var AgentKeepAlive = require('agentkeepalive');
var zlib = require('zlib')
var JSONStream = require('JSONStream')

const ONE_SEC = 1000

module.exports = function(config, done) {
    var table = Dyno(config)
    
    if (config.backup)
        if (!config.backup.bucket)
            return done(new Error('Must provide a data backup bucket'))

    table.describeTable((err, data) => {
        if (err) return done(err)
        
        var writespersec = Math.round(data.Table.ProvisionedThroughput.WriteCapacityUnits / 2)
        
        var count = 0
        var starttime = Date.now()
        
        console.log(`Table ${config.table} from bucket ${config.backup.bucket}/${config.backup.prefix} is gzipped ${config.gzipped}`)
        var uri = ['s3:/', config.backup.bucket, config.backup.prefix].join('/')
        
        var scanner = s3scan.Scan(uri, { 
            s3: new AWS.S3({
                httpOptions: {
                    timeout: 1000,
                    agent: new AgentKeepAlive.HttpsAgent({
                        keepAlive: true,
                        maxSockets: 256,
                        keepAliveTimeout: 60000
                    })
                }
            }) 
        }).on('error', err => done(err))
        
        var extractor = new stream.Transform()
        extractor._writableState.objectMode = true
        extractor._transform = (data, enc, callback) => {
            if (!data) return callback()
            callback(null, data.Body)
        }
        
        var unzipper = zlib.createUnzip().on('error', err => done(err))
        
        var parser = JSONStream.parse()
        
        var throttler = new stream.Transform({ objectMode: true })
        
        throttler.enabled = !isNaN(writespersec)
        throttler.writesPerSec = writespersec
        throttler.window = []
        
        throttler._transform = (data, encoding, callback) => {
            if (throttler.enabled) {
                var now = Date.now()
                throttler.window.push(now)
                
                var oldest = throttler.window[0]
                while (now - oldest > ONE_SEC)
                    oldest = throttler.window.shift()
                
                if (throttler.window.length >= throttler.writesPerSec) {
                    var elapsed = now - throttler.window[0]
                    scanner.pause()
                    setTimeout(() => {scanner.resume()}, ONE_SEC - elapsed)
                }
            }
            
            callback(null, data)
        }
        
        var count = 0
        var writer = new stream.Writable({ objectMode: true, highWaterMark: throttler.writesPerSec })

        writer.items = {}
        writer.items[config.table] = []
        
        var writeItems = (callback) => table.batchWriteItem({
            RequestItems: writer.items
        }, (err, data) => {
            if (err)
                writer.emit('error', err)
            else
                writer.items[config.table] = []
            callback(err)
        })
        
        writer.queue = queue()
        writer.pending = 0
        writer._write = function(record, enc, callback) {
            if (writer.pending > throttler.writesPerSec)
                return setImmediate(writer._write.bind(writer), record, enc, callback)
            
            if (!record) return callback()
            
            writer.drained = false
            writer.pending++
            
            console.log("writing " + JSON.stringify(record))
            
            var items = writer.items[config.table]
            items.push({
                PutRequest: {
                    Item: Dyno.deserialize(JSON.stringify(record))
                }
            })
            
            var itemNr = items.length
            if(itemNr > throttler.writesPerSec) writer.queue.defer(next => {
                console.log(`Write batch of ${itemNr} items`)
                writer.pending -= itemNr
                count += itemNr
                process.stdout.write('\r\033[K' + count + ' - ' + (count / ((Date.now() - starttime) / 1000)).toFixed(2) + '/s')
                writeItems(next)
            })
            
            callback()
        }

        writer.once('error', done)

        var end = writer.end.bind(writer);
        writer.end = () => {
            writer.queue.awaitAll(err => {
                if(!err && writer.items[config.table].length > 0) {
                    console.log(`Write remaining ${writer.items[config.table].length}`)
                    writeItems(end)
                }
                if (err) end(err)
            })
        }

        var piping = scanner.on('error', next)
                       .pipe(extractor)
        
        if (config.gzipped) piping = piping.pipe(unzipper)
        
        piping.pipe(parser)
              .pipe(throttler)
              .pipe(writer)
                .on('error', next)
                .on('finish', next)
                .on('end', next)

        function next(err) {
            if (err) return done(err)
            done(null, { count: count })
        }
    })
}
