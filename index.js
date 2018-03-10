const { S3 } = require('aws-sdk');
const Semaphore = require('simple-semaphore');
const Fastqueue = require('fastqueue');

const CreateS3Client = ({
  accessKeyId = process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY,
  region = 'us-east-1',
  ...params
} = {}) => new S3({ accessKeyId, secretAccessKey, region, params });

class KeyFetcher {
  constructor ({
    s3client = CreateS3Client(),
    bucket,
    prefix,
    regex,
    limit,
    concurrency = 200,
    Bucket = bucket,
    Prefix = prefix,
    MaxKey = concurrency,
    prebuffer = MaxKey * 3,
    ...rest
  }) {
    this._s3 = s3client;
    this._metas = {
      concurrency,
      regex,
      limit,
      scanned: 0,
      fetched: 0,
      params: { Bucket, Prefix, MaxKey, ...rest },
      fetchSem: new Semaphore(prebuffer),
      itemSem: new Semaphore(0),
      inventory: new Fastqueue(),
    };

    this._fetchAll();
  }

  async _fetch () {
    await this._metas.fetchSem.wait(this._metas.concurrency);
    const { Contents, IsTruncated, NextContinuationToken } = await this._s3.listObjectsV2(
      this._metas.params,
    );
    this._metas.params.ContinuationToken = NextContinuationToken;

    const keys = Contents.map(({ Key }) => Key).filter(
      k => (this._metas.regex instanceof RegExp ? this._metas.regex.test(k) : k),
    );
    keys.forEach(k => this._metas.inventory.push(k));

    this._metas.scanned += Contents.length;
    this._metas.fetched += keys.length;

    this._metas.itemSem.signal(keys.length);

    return IsTruncated;
  }

  async _fetchAll () {
    while (await this._fetch()) {}
    return this._metas.fetched;
  }

  async fetchOne () {
    await this._metas.itemSem.wait();
    this._metas.fetchSem.signal();
    return this._metas.inventory.shift();
  }

  async fetchN (n = 1) {
    await this._metas.itemSem.wait(n);
    const list = [];
    while (n--) {
      list.push(this._metas.inventory.shift());
      this._metas.fetchSem.signal();
    }
    return list;
  }
}

module.exports = {CreateS3Client, KeyFetcher};
