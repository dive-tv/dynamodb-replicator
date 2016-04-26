var Dyno = require('dyno');
var AWS = require('aws-sdk');
var s3 = new AWS.S3({
    maxRetries: 1000,
    httpOptions: {
        timeout: 1000,
        agent: require('streambot').agent
    }
});
var stream = require('stream');
var queue = require('queue-async');
var crypto = require('crypto');

const ONE_SEC = 1000
 
module.exports = function(config, done) {
    var primary = Dyno(config);

    if (config.backup)
        if (!config.backup.bucket)
            return done(new Error('Must provide a bucket for incremental backups'));

    primary.describeTable(function(err, data) {
        if (err) return done(err);

        var keys = data.Table.KeySchema.map(function(schema) {
            return schema.AttributeName;
        });

        var count = 0;
        var starttime = Date.now();

        var scanner = primary.scanStream();
        
        var throttler = new stream.Transform({ objectMode: true });
        
        throttler.enabled = !isNaN(config.readspersec);
        throttler.readsPerSec = config.readspersec;
        throttler.window = [];
        
        throttler._transform = (data, encoding, callback) => {
            
            if (throttler.enabled) {
                var now = Date.now();
                throttler.window.push(now);
                
                var oldest = throttler.window[0];
                while (now - oldest > ONE_SEC)
                    oldest = throttler.window.shift();
                
                if (throttler.window.length >= throttler.readsPerSec) {
                    var elapsed = now - throttler.window[0];
                    scanner.pause();
                    setTimeout(() => {scanner.resume()}, ONE_SEC - elapsed);
                }
            }
            
            callback(null, data);
        };
        
        var writer = new stream.Writable({ objectMode: true, highWaterMark: throttler.readsPerSec });

        writer.queue = queue();
        writer.queue.awaitAll(function(err) { if (err) done(err); });
        writer.pending = 0;

        writer._write = function(record, enc, callback) {
            if (writer.pending > throttler.readsPerSec)
                return setImmediate(writer._write.bind(writer), record, enc, callback);
            
            var key = keys.reduce(function(key, k) {
                key[k] = record[k];
                return key;
            }, {});

            var id = crypto.createHash('md5')
                .update(Dyno.serialize(key))
                .digest('hex');

            var keyFields = [];
            if (config.backup.prefix)
                keyFields.push(config.backup.prefix);
            keyFields.push(config.table, id);
            var params = {
                Bucket: config.backup.bucket,
                Key: keyFields.join('/'),
                Body: Dyno.serialize(record)
            };

            writer.drained = false;
            writer.pending++;
            writer.queue.defer(function(next) {
                s3.putObject(params, function(err) {
                    count++;
                    process.stdout.write('\r\033[K' + count + ' - ' + (count / ((Date.now() - starttime) / 1000)).toFixed(2) + '/s');
                    writer.pending--;
                    if (err) writer.emit('error', err);
                    next();
                });
            });
            callback();
        };

        writer.once('error', done);

        var end = writer.end.bind(writer);
        writer.end = function() {
            writer.queue.awaitAll(end);
        };

        scanner
            .on('error', next)
          .pipe(throttler)
          .pipe(writer)
            .on('error', next)
            .on('finish', next);

        function next(err) {
            if (err) return done(err);
            done(null, { count: count });
        }
    });
};
