var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
import { M_DIR, M_FILE, logger, MobilettoError, MobilettoNotFoundError, readStream, } from "mobiletto-base";
import { dirname } from "path";
import { S3Client, ListObjectsCommand, HeadObjectCommand, GetObjectCommand, DeleteObjectCommand, DeleteObjectsCommand, NoSuchKey, } from "@aws-sdk/client-s3";
import { Readable } from "stream";
import { Upload } from "@aws-sdk/lib-storage";
const DEFAULT_REGION = "us-east-1";
const DEFAULT_PREFIX = "";
const DEFAULT_DELIMITER = "/";
const DELETE_OBJECTS_MAX_KEYS = 1000;
export const S3Info = {
    driver: "s3",
    scope: "global",
};
class StorageClient {
    constructor(key, secret, opts) {
        // noinspection JSUnusedGlobalSymbols -- called by driver init
        this.testConfig = () => __awaiter(this, void 0, void 0, function* () { return yield this._list("", false, undefined, { MaxKeys: 1 }); });
        this.info = () => (Object.assign({ canonicalName: () => `s3:${this.bucket}` }, S3Info));
        this.stripPrefix = (name) => (name.startsWith(this.prefix) ? name.substring(this.prefix.length) : name);
        this.nameToObj = (name) => {
            const relName = this.stripPrefix(name);
            return {
                name: relName,
                type: relName.endsWith(this.delimiter) ? M_DIR : M_FILE,
            };
        };
        this.normalizeKey = (path) => {
            const p = path.startsWith(this.prefix)
                ? path
                : this.prefix + (path.startsWith(this.delimiter) ? path.substring(1) : path);
            return p.replace(this.normalizeRegex, this.delimiter);
        };
        this.denormalizeKey = (key) => {
            return key.startsWith(this.prefix) ? key.substring(this.prefix.length) : key;
        };
        if (!key || !secret || !opts || !opts.bucket) {
            throw new MobilettoError("s3.StorageClient: key, secret, and opts.bucket are required");
        }
        this.bucket = opts.bucket;
        const delim = (this.delimiter = opts.delimiter || DEFAULT_DELIMITER);
        this.normalizeRegex = new RegExp(`${this.delimiter}{2,}`, "g");
        this.prefix = opts.prefix || DEFAULT_PREFIX;
        if (!this.prefix.endsWith(delim)) {
            this.prefix += delim;
        }
        this.region = opts.region || DEFAULT_REGION;
        const credentials = {
            accessKeyId: key,
            secretAccessKey: secret,
        };
        this.client = new S3Client({ region: this.region, credentials });
    }
    list(path = "", recursiveOrOpts, visitor) {
        return __awaiter(this, void 0, void 0, function* () {
            const recursive = recursiveOrOpts === true || (typeof recursiveOrOpts === "object" && recursiveOrOpts.recursive);
            try {
                return yield this._list(path, recursive, visitor);
            }
            catch (e) {
                if (e instanceof MobilettoNotFoundError && !recursive && path.includes(this.delimiter)) {
                    // are we trying to list a single file?
                    const objects = yield this._list(dirname(path), false);
                    const found = objects.find((o) => o.name === path);
                    if (found) {
                        return [found];
                    }
                    throw e;
                }
            }
        });
    }
    _list(path, recursive = false, visitor, params = {}) {
        return __awaiter(this, void 0, void 0, function* () {
            const logPrefix = `_list(path=${path})`;
            // Declare truncated as a flag that the while loop is based on.
            let truncated = true;
            const Prefix = this.prefix +
                (path.startsWith(this.delimiter) ? path.substring(0) : path) +
                (path.length === 0 || path.endsWith(this.delimiter) ? "" : this.delimiter);
            const bucketParams = Object.assign({}, params, {
                Region: this.region,
                Bucket: this.bucket,
                Prefix,
            });
            if (!recursive) {
                bucketParams.Delimiter = this.delimiter;
            }
            const objects = [];
            let objectCount = 0;
            logger.debug(`${logPrefix} bucketParams=${JSON.stringify(bucketParams)}`);
            // while loop that runs until 'response.truncated' is false.
            while (truncated) {
                try {
                    const response = yield this.client.send(new ListObjectsCommand(bucketParams));
                    const hasContents = typeof response.Contents !== "undefined";
                    if (hasContents) {
                        for (const item of response.Contents || []) {
                            if (!item.Key)
                                continue;
                            const obj = this.nameToObj(item.Key);
                            if (visitor) {
                                yield visitor(obj);
                            }
                            objects.push(obj);
                            objectCount++;
                        }
                    }
                    const hasCommonPrefixes = typeof response.CommonPrefixes !== "undefined";
                    if (hasCommonPrefixes) {
                        for (const item of response.CommonPrefixes || []) {
                            if (!item.Prefix)
                                continue;
                            const obj = this.nameToObj(item.Prefix);
                            if (visitor) {
                                yield visitor(obj);
                            }
                            objects.push(obj);
                            objectCount++;
                        }
                    }
                    truncated = response.IsTruncated || false;
                    // If truncated is true, advance the marker
                    if (truncated) {
                        bucketParams.Marker = response.NextMarker;
                    }
                    else if (!hasContents && !hasCommonPrefixes) {
                        if (path === "") {
                            break;
                        }
                        throw new MobilettoNotFoundError(path);
                    }
                }
                catch (err) {
                    if (err instanceof MobilettoNotFoundError) {
                        throw err;
                    }
                    throw new MobilettoError(`${logPrefix} Error: ${err}`);
                }
            }
            if (recursive && objectCount === 0 && path !== "") {
                throw new MobilettoNotFoundError(path);
            }
            const filtered = objects.filter((o) => o.name !== path);
            return filtered;
        });
    }
    s3error(err, key, path, method) {
        return err instanceof MobilettoError || err instanceof MobilettoNotFoundError
            ? err
            : err instanceof NoSuchKey || (err.name && err.name === "NotFound")
                ? new MobilettoNotFoundError(this.denormalizeKey(key))
                : new MobilettoError(`${method}(${path}) error: ${err}`, err);
    }
    metadata(path) {
        return __awaiter(this, void 0, void 0, function* () {
            const Key = this.normalizeKey(path);
            const bucketParams = {
                Region: this.region,
                Bucket: this.bucket,
                Key,
                Delimiter: this.delimiter,
            };
            try {
                const head = yield this.client.send(new HeadObjectCommand(bucketParams));
                const meta = {
                    name: this.stripPrefix(path),
                    size: head.ContentLength,
                    type: path.endsWith(this.delimiter) ? M_DIR : M_FILE,
                };
                if (head.LastModified) {
                    meta.mtime = Date.parse(head.LastModified.toString());
                }
                return meta;
            }
            catch (err) {
                throw this.s3error(err, Key, path, "metadata");
            }
        });
    }
    write(path, generator) {
        return __awaiter(this, void 0, void 0, function* () {
            const Key = this.normalizeKey(path);
            const bucketParams = {
                Region: this.region,
                Bucket: this.bucket,
                Key,
                Body: Readable.from(generator),
                Delimiter: this.delimiter,
            };
            const uploader = new Upload({
                client: this.client,
                params: bucketParams,
                queueSize: 1,
                partSize: 1024 * 1024 * 5,
                leavePartsOnError: false, // optional manually handle dropped parts
            });
            let total = 0;
            uploader.on("httpUploadProgress", (progress) => {
                logger.debug(`write(${bucketParams.Key}) ${JSON.stringify(progress)}`);
                total += progress.loaded || 0;
            });
            const response = yield uploader.done();
            if (response.Key === Key) {
                return total;
            }
            throw new MobilettoError(`s3.write: after writing, expected Key=${Key} but found response.Key=${response.Key}`);
        });
    }
    read(path, callback, endCallback) {
        return __awaiter(this, void 0, void 0, function* () {
            const Key = this.normalizeKey(path);
            logger.debug(`read: reading Key: ${path} - ${Key}`);
            const bucketParams = {
                Region: this.region,
                Bucket: this.bucket,
                Key,
                Delimiter: this.delimiter,
            };
            try {
                const data = yield this.client.send(new GetObjectCommand(bucketParams));
                return yield readStream(data.Body, callback, endCallback);
            }
            catch (err) {
                throw this.s3error(err, Key, path, "read");
            }
        });
    }
    remove(path, optsOrRecursive, quiet) {
        return __awaiter(this, void 0, void 0, function* () {
            const recursive = optsOrRecursive === true || (optsOrRecursive && optsOrRecursive.recursive);
            if (recursive) {
                const removed = [];
                let objects = yield this._list(path, true, undefined, {
                    MaxKeys: DELETE_OBJECTS_MAX_KEYS,
                });
                while (objects && objects.length > 0) {
                    const Delete = {
                        Objects: objects.map((obj) => {
                            return { Key: this.normalizeKey(obj.name) };
                        }),
                        Quiet: quiet || false,
                    };
                    const bucketParams = {
                        Bucket: this.bucket,
                        Delete,
                    };
                    logger.debug(`remove(${path}) deleting objects: ${JSON.stringify(objects)}`);
                    const response = yield this.client.send(new DeleteObjectsCommand(bucketParams));
                    const statusCode = response.$metadata.httpStatusCode;
                    const statusClass = statusCode ? Math.floor(statusCode / 100) : -1;
                    if (statusClass !== 2) {
                        throw new MobilettoError(`remove(${path}) DeleteObjectsCommand returned HTTP status ${statusCode}`);
                    }
                    if (!quiet && response.Errors && response.Errors.length > 0) {
                        throw new MobilettoError(`remove(${path}) DeleteObjectsCommand returned Errors: ${JSON.stringify(response.Errors)}`);
                    }
                    if (response.Deleted) {
                        removed.push(...response.Deleted.map((del) => del.Key ? this.denormalizeKey(del.Key) : "?del.Key undefined?"));
                    }
                    try {
                        objects = yield this._list(path, true, undefined, { MaxKeys: DELETE_OBJECTS_MAX_KEYS });
                    }
                    catch (e) {
                        if (!(e instanceof MobilettoNotFoundError)) {
                            throw e instanceof MobilettoError
                                ? e
                                : new MobilettoError(`remove(${path}) error listing: ${e}`);
                        }
                        objects = null;
                    }
                }
                return removed;
            }
            else {
                const Key = this.normalizeKey(path);
                const bucketParams = {
                    Region: this.region,
                    Bucket: this.bucket,
                    Key,
                };
                try {
                    // DeleteObjectCommand silently succeeds and returns HTTP 204 even for non-existent Keys
                    // Thus, if quiet is false, we must check metadata explicitly, which will fail with
                    // MobilettoNotFoundError, which is the correct behavior
                    if (!quiet) {
                        yield this.metadata(path);
                    }
                    const response = yield this.client.send(new DeleteObjectCommand(bucketParams));
                    const statusCode = response.$metadata.httpStatusCode;
                    const statusClass = statusCode ? Math.floor(statusCode / 100) : -1;
                    if (statusClass !== 2) {
                        throw new MobilettoError(`remove: DeleteObjectCommand returned HTTP status ${statusCode}`);
                    }
                }
                catch (err) {
                    throw this.s3error(err, Key, path, "remove");
                }
                return path;
            }
        });
    }
}
export const storageClient = (key, secret, opts) => {
    if (!key || !secret || !opts || !opts.bucket) {
        throw new MobilettoError("s3.storageClient: key, secret, and opts.bucket are required");
    }
    return new StorageClient(key, secret, opts);
};
