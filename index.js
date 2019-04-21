
const { EventEmitter } = require('events');
const asynccb = require('async');
const AWS = require('aws-sdk');

const ONE_MEGABYTE = 1024 * 1024;
const defaultOptions = {
  concurrentParts: 3, // Concurrent parts that will be uploaded to s3 (if read stream is fast enough)
  waitTime: 0, // In seconds (Only applies once all parts are uploaded, used for acknowledgement), 0 = Unlimited
  retries: 5, // Number of times to retry a part.
  sdkRetries: 6, // Passed onto the underlying aws-sdk.
  maxPartSize: 5 * ONE_MEGABYTE, // In bytes, will also consume this much buffering memory.
};

class StreamingS3 extends EventEmitter {
  constructor(stream, { s3AccessKey, s3SecretKey, s3Params, options = {} }, cb) {
    super();
    this.options = Object.assign({}, defaultOptions, options);
    this.stream = stream;
    this.s3AccessKey = s3AccessKey;
    this.s3SecretKey = s3SecretKey;
    this.s3Params = s3Params;

    this.initialized = false;

    // Lets hook our error event in the start so we can easily emit errors.
    this.on('error', this.error);
  }

  initialize() {
    if (this.initialized) return;
    this.initialized = true;
    this.bucketName = s3Params.Bucket;

    // States
    this.waiting = false;
    this.initiated = false;
    this.failed = false;
    this.reading = false;
    this.finished = false;

    // Stats
    this.stats = { downloadSpeed: 0, uploadSpeed: 0, downloadTime: 0, uploadTime: 0, size: 0 };
    this.uploadStart = 0;
    this.downloadStart = 0;
    this.totalDldBytes = 0;
    this.dldPercentage = 0;

    // Chunking and buffering
    this.buffer = Buffer.alloc(0);
    this.chunks = [];
    this.chunkNumber = 0;
    this.totalChunks = 0;
    this.uploadedChunks = {}; // We just store ETags of all parts, not the actual buffer.

    this.uploadId = null;

    // Timers
    this.waitingTimer = false;
    this.acknowledgeTimer = false;
    this.setupAws();
  }

  error(e) {
    if (this.failed || this.finished) return;
    this.waitingTimer && clearTimeout(this.waitingTimer);
    this.acknowledgeTimer && clearTimeout(this.acknowledgeTimer);
    this.reading = false;
    this.waiting = false;
    this.failed = true;

    // Remove our event handlers if any.
    if (this.stream) {
      this.streamErrorHandler && this.stream.removeListener('error', this.streamErrorHandler);
      this.streamDataHandler && this.stream.removeListener('data', this.streamDataHandler);
      this.streamEndHandler && this.stream.removeListener('end', this.streamEndHandler);
    }

    if (this.uploadId) {
      // THIS WILL FAIL IF IT CONTAINS ACL ¯\_(ツ)_/¯
      const abortMultipartUploadParams = Object.assign({ UploadId: this.uploadId }, this.s3Params);
      // THIS WILL FAIL IF IT CONTAINS ACL ¯\_(ツ)_/¯

      this.s3Client.abortMultipartUpload(abortMultipartUploadParams, (multipart_err, _data) => {
        if (multipart_err) {
          // We can't do anything if aborting fails :'(
          console.error("Aborting the multipart upload failed.");
          console.error(multipart_err.message);
          console.trace(multipart_err.stack);
          // We should still return the original error
        }
        this.cb && this.cb(e);
        this.cb = null;
      });
    } else {
      this.cb && this.cb(e);
      this.cb = null;
    }
  }

  setupAws() {
    const credentials = new AWS.SharedIniFileCredentials({ profile: 'wasabi' });
    AWS.config.credentials = credentials;
    const s3Params = {
      accessKeyId: this.s3AccessKey,
      secretAccessKey: this.secretAccessKey,
    }
    this.s3Client = new AWS.S3();
    if(options.wasabi) this.s3Client
  }

  async uploadStream(stream, fileName) {
    return new Promise((resolve, reject) => {
      this.initialize();
      this.cb = (err, res) => {
        err ? reject(err) : resolve(res)
      };

      this.s3Params =  {
        Bucket: this.bucketName,
        Key: fileName,
      };
      this.s3ObjectParams = Object.assign({}, this.s3Params, {
        ACL: 'public-read'
      });

      if (!this.s3ObjectParams.Bucket || !this.s3ObjectParams.Key) {
        return this.emit('error', new Error('Bucket and Key parameters for S3 Object are required!'));
      }

      this.stream = stream;
      this.stream.pause();
      this.begin();
    });
  }

  begin() {
    if (this.initiated || this.finished) return;

    this.streamErrorHandler = (err) => this.emit('error', err);
    this.streamDataHandler = (chunk) => {
      this.reading = true;
      if (!this.downloadStart) this.downloadStart = Date.now();
      if (typeof chunk === 'string') chunk = Buffer.from(chunk, 'utf-8');
      this.totalDldBytes += chunk.length;

      this.buffer = Buffer.concat([this.buffer, chunk]);
      this.emit('data', chunk.length);
      if (this.buffer.length >= this.options.maxPartSize) {
        this.flushChunk();
      }
    };
    this.streamEndHandler = () => {
      this.reading = false;
      if (this.downloadStart) {
        this.stats.downloadTime = Math.round((Date.now() - this.downloadStart) / 1000, 3);
        this.stats.downloadSpeed = Math.round(this.totalDldBytes / (this.stats.downloadTime / 1000), 2);
      }
      this.flushChunk();
    };
    this.s3Client.createMultipartUpload(this.s3ObjectParams, (err, data) => {
      if (err) return this.emit('error', err);
      // Assert UploadId presence.
      if (!data.UploadId) return callback(new Error('AWS SDK returned invalid object! Expecting UploadId.'));
      console.log("MultipartUpload initiated");
      this.uploadId = data.UploadId;
      this.initiated = true;
      this.stream.on('error', this.streamErrorHandler);
      this.stream.on('data', this.streamDataHandler);
      this.stream.on('end', this.streamEndHandler);
      this.stream.resume();
    });
  }

  flushChunk() {
    console.log("Flush Chunk called.");
    console.log(`initiated: ${this.initiated}, uploadId: ${this.uploadId}`);
    if (!this.initiated || !this.uploadId) return;
    var newChunk;
    if (this.buffer.length > this.options.maxPartSize) {
      newChunk = this.buffer.slice(0, this.options.maxPartSize);
      this.buffer = Buffer.from(this.buffer.slice(this.options.maxPartSize));
    } else {
      newChunk = this.buffer.slice(0, this.options.maxPartSize);
      this.buffer = Buffer.alloc(0);
    }

    // Add useful properties to each chunk.
    newChunk.uploading = false;
    newChunk.finished = false;
    newChunk.number = ++this.chunkNumber;
    newChunk.retries = 0;
    this.chunks.push(newChunk);
    this.totalChunks++;

    // Edge case
    if (this.reading === false && this.buffer.length) {
      newChunk = this.buffer.slice(0, this.buffer.length);
      this.buffer = null;
      newChunk.uploading = false;
      newChunk.finished = false;
      newChunk.number = ++this.chunkNumber;
      newChunk.retries = 0;
      this.chunks.push(newChunk);
      this.totalChunks++;
    }

    this.sendToS3();
  }

  sendToS3() {
    console.log('Sending to s3');
    if (!this.uploadId || this.waiting) return;

    if (!this.uploadStart) this.uploadStart = Date.now();

    const uploadChunk = (chunk, next) => {
      if (!this.uploadId || !this.initiated || this.failed || chunk.uploading || chunk.finished || chunk.number < 0) return next();

      chunk.uploading = true;

      var partS3Params = {
        UploadId: this.uploadId,
        PartNumber: chunk.number,
        Body: chunk
      };

      partS3Params = Object.assign({}, partS3Params, this.s3Params);
      this.s3Client.uploadPart(partS3Params, (err, data) => {
        if (err) {
          if (err.code === 'RequestTimeout' || err.code === 'RequestTimeTooSkewed') {
            if (chunk.retries >= this.options.retries) return next(err);
            else {
              chunk.uploading = false;
              chunk.retries++;
              return uploadChunk(chunk, next);
            }
          } else {
            chunk.finished = true;
            return next(err);
          }
        } else {
          // Assert ETag presence.
          if (!data.ETag) return next(new Error('AWS SDK returned invalid object when part uploaded! Expecting Etag.'));

          // chunk.number starts at 1, while array starts at 0.
          this.uploadedChunks[chunk.number] = data.ETag;
          chunk.finished = true;
          this.totalUldBytes += chunk.length;
          this.emit('part', chunk.number);
          return next();
        }
      });
    }

    // Remove finished chunks, save memory :)
    this.chunks = this.chunks.filter((chunk) => chunk.finished === false);
    if (this.chunks.length) {
      asynccb.eachLimit(
        this.chunks,
        this.options.concurrentParts,
        uploadChunk,
        (e) => this.eachLimitCb(e));
    }
  }

  eachLimitCb(err) {
    if (err) return this.emit('error', err);

    // Remove finished chunks, save memory :)
    this.chunks = this.chunks.filter((chunk) => chunk.finished === false);
    if (this.chunks.length === 0 && !this.waiting && !this.reading && this.totalChunks === Object.keys(this.uploadedChunks).length) {
      if (this.uploadStart) {
        this.stats.uploadTime = Math.round((Date.now() - this.uploadStart) / 1000, 3);
        this.stats.uploadSpeed = Math.round(this.totalDldBytes / (this.stats.uploadTime / 1000), 2);
      }
      this.stats.size = this.totalDldBytes;
      this.emit('uploaded', this.stats);
      this.waiting = true;

      // Give S3 some breathing time, before we check if the upload succeeded
      this.acknowledgeTimer = setTimeout(() => { this.finish(); }, 500);
    }
  }

  finish() {
    if (!this.uploadId || this.failed || this.finished) {
      return this.acknowledgeTimer && clearTimeout(this.acknowledgeTimer);
    }

    this.acknowledgeTimer && clearTimeout(this.acknowledgeTimer);

    const listPartsParams = Object.assign({ UploadId: this.uploadId, MaxParts: this.totalChunks}, this.s3Params);
    this.s3Client.listParts(listPartsParams, (err, data) => {
      if (err) return this.emit('error', err);

      if (!this.acknowledgeTimer) this.acknowledgeTimer = setTimeout(() => { this.finish(); }, 1000);

      // Assert Parts presence.
      if (!data.Parts) return this.emit('error', new Error('AWS SDK returned invalid object! Expecting Parts.'));
      if (data.Parts.length !== this.totalChunks) return;

      // S3 has all parts Lets send ETags.
      let completeMultipartUploadParams = {
        UploadId: this.uploadId,
        MultipartUpload: {
          Parts: [],
        },
      };

      for (const key in this.uploadedChunks) {
        completeMultipartUploadParams.MultipartUpload.Parts.push({ ETag: this.uploadedChunks[key], PartNumber: key });
      }

      completeMultipartUploadParams = Object.assign({}, completeMultipartUploadParams, this.s3Params);
      this.s3Client.completeMultipartUpload(completeMultipartUploadParams, (err, data) => {
        if (err) return this.emit('error', err);

        // Assert File ETag presence.
        if (!data.ETag) return this.emit('error', new Error('AWS SDK returned invalid object! Expecting file Etag.'));
        this.waitingTimer && clearTimeout(this.waitingTimer);
        this.acknowledgeTimer && clearTimeout(this.acknowledgeTimer);
        this.initiated = false;
        this.waiting = false;
        this.finished = true;
        this.emit('finished', data, this.stats);
        this.cb && this.cb(null, data, this.stats); // Done :D
        // prevent any further callback calls.
        this.cb = null;
      });
    });

    // Make sure we don't keep checking for parts forever.
    if (!this.waitingTimer && this.options.waitTime) {
      this.waitingTimer = setTimeout(() => {
        if (this.waiting && !this.finished) {
          const errStr = 'AWS did not acknowledge all parts within specified timeout of ';
          return this.emit('error', new Error(errStr + (this.options.waitTime / 1000) + ' seconds.'));
        }
      }, this.options.waitTime);
    }
  }
}

module.exports = StreamingS3;
