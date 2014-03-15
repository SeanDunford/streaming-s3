# Streaming-S3

[![Build Status](https://travis-ci.org/mindblaze/streaming-s3.png?branch=master)](https://travis-ci.org/mindblaze/streaming-s3)
[![Dependency Status](https://www.versioneye.com/user/projects/5323a411ec13758e7d000109/badge.png)](https://www.versioneye.com/user/projects/5323a411ec13758e7d000109)

A simple and light-weight S3 upload streaming module for NodeJS.


## Benefits & Features
* Super fast and super easy to use
* Low memory usage
* Nothing is written to disk during upload
* Parallel part uploading
* No need to know total size of the object
* Implicit retry mechanism for failed part uploads
* Tons of configurable options
* Simple interface (Asynchronous and evented)
* Downloading and uploading statistics (U/D speed and U/D time)
* Proper usage of streams and graceful error handling
* Production ready (Used and tested on production environments, uploading gigabytes of files to S3)
* Uses official AWS SDK


## Installation

```
$ npm install streaming-s3
```


## Example Usage


### Example 1: Uploading local file with callback.

```js
var Streaming-S3 = require('streaming-s3'),
    fs = require('fs');

var fStream = fs.CreateReadStream(__dirname + '/video.mp4');
var uploader = new Streaming-S3(fStream, 'accessKey', 'secretKey',
  {
    Bucket: 'example.streaming-s3.com',
    Key: 'video.mp4',
    ContentType: 'video/mp4'
  },  function (err, resp, stats) {
  if (err) return console.log('Upload error: ', e);
  console.log('Upload stats: ', stats);
  console.log('Upload successful: ', resp);
  }
);
```

### Example 2: Uploading local file without callback.

```js
var Streaming-S3 = require('streaming-s3'),
    fs = require('fs');

var fStream = fs.CreateReadStream(__dirname + '/video.mp4');
var uploader = new Streaming-S3(fStream, 'accessKey', 'secretKey',
  {
    Bucket: 'example.streaming-s3.com',
    Key: 'video.mp4',
    ContentType: 'video/mp4'
  }
);
  
uploader.begin(); // important if callback not provided.

uploader.on('data', function (bytesRead) {
  console.log(bytesRead, ' bytes read.');
});

uploader.on('part', function (number) {
  console.log('Part ', number, ' uploaded.');
});

// All parts uploaded, but upload not yet acknowledged.
uploader.on('uploaded', function (stats) {
  console.log('Upload stats: ', stats);
});

uploader.on('finished', function (resp, stats) {
  console.log('Upload finished: ', resp);
});

uploader.on('error', function (e) {
  console.log('Upload error: ', e);
});
```


### Example 2: Uploading remote file without callback and options

```js
var Streaming-S3 = require('streaming-s3'),
    request = require('request');

var rStream = request.get('http://www.google.com');
var uploader = new Streaming-S3(rStream, 'accessKey', 'secretKey',
  {
    Bucket: 'example.streaming-s3.com',
    Key: 'google.html',
    ContentType: 'text/html'
  },
  {
    concurrentParts: 2,
    waitTime: 10000,
    retries: 1,
    maxPartSize: 10*1024*1024,
  }
);
  
uploader.begin(); // important if callback not provided.

uploader.on('data', function (bytesRead) {
  console.log(bytesRead, ' bytes read.');
});

uploader.on('part', function (number) {
  console.log('Part ', number, ' uploaded.');
});

// All parts uploaded, but upload not yet acknowledged.
uploader.on('uploaded', function (stats) {
  console.log('Upload stats: ', stats);
});

uploader.on('finished', function (resp, stats) {
  console.log('Upload finished: ', resp);
});

uploader.on('error', function (e) {
  console.log('Upload error: ', e);
});
```

## Defaults and Configurables

* **concurrentParts** (Default: 5) - Parts that are uploaded simultaneously.
* **waitTime** (Default: 1 min (60000 ms)) - Time to wait for verification from S3 after uploading parts.
* **retries** (Default: 5) - Number of times to retry uploading a part, before failing.
* **maxPartSize** (Default: 5 MB) - Maximum size of each part.

## History

* v0.1.6 (2014-03-15) -- Added upload, download time and speed statistics.


## License
The MIT License (MIT)

Copyright (c) 2014 Talha Asad

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.