import {
    M_DIR,
    M_FILE,
    logger,
    MobilettoError,
    MobilettoNotFoundError,
    readStream,
    MobilettoOptions,
    MobilettoVisitor,
    MobilettoMetadata,
    MobilettoWriteSource,
    MobilettoListOptions,
    MobilettoRemoveOptions,
    MobilettoDriverInfo,
    MobilettoDriverScope,
    MobilettoListOutput,
    MobilettoListPaging,
} from "mobiletto-base";

import { dirname } from "path";

import {
    S3Client,
    ListObjectsV2Command,
    ListObjectsRequest,
    HeadObjectCommand,
    GetObjectCommand,
    DeleteObjectCommand,
    DeleteObjectsCommand,
    NoSuchKey,
    ListObjectsV2CommandOutput,
    CompleteMultipartUploadCommandOutput,
    DeleteObjectsRequest,
    ListObjectsV2CommandInput,
    ListObjectsCommandOutput,
    ListObjectsCommand,
} from "@aws-sdk/client-s3";

import { Readable } from "stream";
import { Progress, Upload } from "@aws-sdk/lib-storage";
import { S3ClientConfig } from "@aws-sdk/client-s3/dist-types/S3Client";

const DEFAULT_REGION = "us-east-1";
const DEFAULT_PREFIX = "";
const DEFAULT_DELIMITER = "/";

const DELETE_OBJECTS_MAX_KEYS = 1000;

export type S3Options = MobilettoOptions & {
    bucket?: string;
    prefix?: string;
    delimiter?: string;
    region?: string;
};

export type S3InfoType = {
    driver: string;
    scope: MobilettoDriverScope;
};

export const S3Info: S3InfoType = {
    driver: "s3",
    scope: "global",
};

/**
 * For extend internal Aws request for delete and etc.
 */
type ListAdditionalParams = Partial<{
    MaxKeys: number;
}>;

class StorageClient {
    private client: S3Client;
    private region: string;
    private bucket: string;
    private prefix: string;
    private delimiter: string;
    private normalizeRegex: RegExp;
    constructor(key: string, secret: string, opts: S3Options) {
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
        this.client = new S3Client({ region: this.region, credentials } as S3ClientConfig);
    }

    // noinspection JSUnusedGlobalSymbols -- called by driver init
    testConfig = async () => await this._list("", false, undefined, undefined, { maxItems: 1});

    info = (): MobilettoDriverInfo => ({
        canonicalName: () => `s3:${this.bucket}`,
        ...S3Info,
    });

    stripPrefix = (name: string) => (name.startsWith(this.prefix) ? name.substring(this.prefix.length) : name);

    nameToObj = (name: string): MobilettoMetadata => {
        const relName = this.stripPrefix(name);
        return {
            name: relName,
            type: relName.endsWith(this.delimiter) ? M_DIR : M_FILE,
        };
    };

    async list(path = "", recursiveOrOpts: MobilettoListOptions | boolean, visitor?: MobilettoVisitor) {
        let recursive = false;
        let params: ListAdditionalParams = {};
        let paging: MobilettoListPaging | undefined;
        if (typeof recursiveOrOpts === "object") {
            recursive = recursiveOrOpts.recursive ?? recursive;
            if (typeof recursiveOrOpts.paging === "object") {
                paging = recursiveOrOpts.paging;
            }
        } else if (typeof recursiveOrOpts === "boolean") {
            recursive = recursiveOrOpts;
        }

        try {
            return await this._list(path, recursive, visitor, params, paging);
        } catch (e) {
            if (e instanceof MobilettoNotFoundError && !recursive && path.includes(this.delimiter)) {
                // are we trying to list a single file?
                const output = await this._list(dirname(path), false);
                const found = output.objects.find((o) => o.name === path);
                if (found) {
                    return { objects: [found] };
                }
                throw e;
            }
        }
    }

    async _list(
        path: string,
        recursive = false,
        visitor?: MobilettoVisitor,
        params: ListAdditionalParams = {},
        paging?: MobilettoListPaging
    ): Promise<MobilettoListOutput> {
        const logPrefix = `_list(path=${path})`;

        // Declare truncated as a flag that the while loop is based on.
        let truncated = true;

        const Prefix =
            this.prefix +
            (path.startsWith(this.delimiter) ? path.substring(0) : path) +
            (path.length === 0 || path.endsWith(this.delimiter) ? "" : this.delimiter);

        const bucketParams: ListObjectsV2CommandInput = {
            ...params,
            Bucket: this.bucket,
            Prefix,
        };
        if (!recursive) {
            bucketParams.Delimiter = this.delimiter;
        }
        const output: MobilettoListOutput = { objects: [] };

        let { continuationToken } = paging ?? {};
        let objectCount = 0;

        logger.debug(`${logPrefix} bucketParams=${JSON.stringify(bucketParams)}`);

        let MaxKeys = paging?.maxItems ? paging.maxItems : params.MaxKeys;
        // while loop that runs until 'response.truncated' is false.

        while (truncated) {
            try {
                const response: ListObjectsV2CommandOutput = await this.client.send(
                    new ListObjectsV2Command({
                        ...bucketParams,
                        ContinuationToken: continuationToken,
                        MaxKeys,
                    })
                );
                const hasContents = typeof response.Contents !== "undefined";
                if (hasContents) {
                    for (const item of response.Contents || []) {
                        if (!item.Key) continue;
                        const obj = this.nameToObj(item.Key);
                        if (visitor) {
                            await visitor(obj);
                        }
                        output.objects.push(obj);
                        objectCount++;
                    }
                }
                const hasCommonPrefixes = typeof response.CommonPrefixes !== "undefined";
                if (hasCommonPrefixes) {
                    for (const item of response.CommonPrefixes || []) {
                        if (!item.Prefix) continue;
                        const obj = this.nameToObj(item.Prefix);
                        if (visitor) {
                            await visitor(obj);
                        }
                        output.objects.push(obj);
                        objectCount++;
                    }
                }
                truncated = response.IsTruncated || false;

                 // we have paging
                 if (paging && objectCount === paging.maxItems) {
                    // we are done
                    output.nextPageToken = response.NextContinuationToken;
                    output.previousPageToken = paging?.continuationToken;
                    break;
                }

                // If truncated is true, advance the marker
                if (truncated) {
                    continuationToken = response.NextContinuationToken;
                } else if (!hasContents && !hasCommonPrefixes) {
                    if (path === "") {
                        break;
                    }
                    throw new MobilettoNotFoundError(path);
                }
            } catch (err) {
                if (err instanceof MobilettoNotFoundError) {
                    throw err;
                }
                throw new MobilettoError(`${logPrefix} Error: ${err}`);
            }
        }
        if (recursive && objectCount === 0 && path !== "") {
            throw new MobilettoNotFoundError(path);
        }
        output.objects = output.objects.filter((o) => o.name !== path);

        return output;
    }

    normalizeKey = (path: string) => {
        const p = path.startsWith(this.prefix)
            ? path
            : this.prefix + (path.startsWith(this.delimiter) ? path.substring(1) : path);
        return p.replace(this.normalizeRegex, this.delimiter);
    };
    denormalizeKey = (key: string) => {
        return key.startsWith(this.prefix) ? key.substring(this.prefix.length) : key;
    };

    s3error(err: any, key: string, path: string, method: string) {
        return err instanceof MobilettoError || err instanceof MobilettoNotFoundError
            ? err
            : err instanceof NoSuchKey || (err.name && err.name === "NotFound")
            ? new MobilettoNotFoundError(this.denormalizeKey(key))
            : new MobilettoError(`${method}(${path}) error: ${err}`, err);
    }

    async metadata(path: string): Promise<MobilettoMetadata> {
        const Key = this.normalizeKey(path);
        const bucketParams = {
            Region: this.region,
            Bucket: this.bucket,
            Key,
            Delimiter: this.delimiter,
        };
        try {
            const head = await this.client.send(new HeadObjectCommand(bucketParams));
            const meta: MobilettoMetadata = {
                name: this.stripPrefix(path),
                size: head.ContentLength,
                type: path.endsWith(this.delimiter) ? M_DIR : M_FILE,
            };
            if (head.LastModified) {
                meta.mtime = Date.parse(head.LastModified.toString());
            }
            return meta;
        } catch (err) {
            throw this.s3error(err, Key, path, "metadata");
        }
    }

    async write(path: string, generator: MobilettoWriteSource) {
        const Key = this.normalizeKey(path);
        const bucketParams = {
            Region: this.region,
            Bucket: this.bucket,
            Key,
            Body: Readable.from(generator as Iterable<any> | AsyncIterable<any>),
            Delimiter: this.delimiter,
        };

        const uploader = new Upload({
            client: this.client,
            params: bucketParams,
            queueSize: 1, // optional concurrency configuration
            partSize: 1024 * 1024 * 5, // optional size of each part, in bytes, at least 5MB
            leavePartsOnError: false, // optional manually handle dropped parts
        });
        let total = 0;
        uploader.on("httpUploadProgress", (progress: Progress) => {
            logger.debug(`write(${bucketParams.Key}) ${JSON.stringify(progress)}`);
            total += progress.loaded || 0;
        });
        const response: CompleteMultipartUploadCommandOutput = await uploader.done();
        if (response.Key === Key) {
            return total;
        }
        throw new MobilettoError(`s3.write: after writing, expected Key=${Key} but found response.Key=${response.Key}`);
    }

    async read(path: string, callback: (data: any) => void, endCallback?: () => unknown) {
        const Key = this.normalizeKey(path);
        logger.debug(`read: reading Key: ${path} - ${Key}`);
        const bucketParams = {
            Region: this.region,
            Bucket: this.bucket,
            Key,
            Delimiter: this.delimiter,
        };
        try {
            const data = await this.client.send(new GetObjectCommand(bucketParams));
            return await readStream(data.Body, callback, endCallback);
        } catch (err) {
            throw this.s3error(err, Key, path, "read");
        }
    }

    async remove(path: string, optsOrRecursive?: MobilettoRemoveOptions | boolean, quiet?: boolean) {
        const recursive = optsOrRecursive === true || (optsOrRecursive && optsOrRecursive.recursive);
        if (recursive) {
            const removed = [];
            let objects: MobilettoMetadata[] | null = (
                await this._list(path, true, undefined, {
                    MaxKeys: DELETE_OBJECTS_MAX_KEYS,
                })
            ).objects;
            while (objects && objects.length > 0) {
                const Delete = {
                    Objects: objects.map((obj) => {
                        return { Key: this.normalizeKey(obj.name) };
                    }),
                    Quiet: quiet || false,
                };
                const bucketParams: DeleteObjectsRequest = {
                    Bucket: this.bucket,
                    Delete,
                };
                logger.debug(`remove(${path}) deleting objects: ${JSON.stringify(objects)}`);
                const response = await this.client.send(new DeleteObjectsCommand(bucketParams));
                const statusCode = response.$metadata.httpStatusCode;
                const statusClass = statusCode ? Math.floor(statusCode / 100) : -1;
                if (statusClass !== 2) {
                    throw new MobilettoError(`remove(${path}) DeleteObjectsCommand returned HTTP status ${statusCode}`);
                }
                if (!quiet && response.Errors && response.Errors.length > 0) {
                    throw new MobilettoError(
                        `remove(${path}) DeleteObjectsCommand returned Errors: ${JSON.stringify(response.Errors)}`
                    );
                }
                if (response.Deleted) {
                    removed.push(
                        ...response.Deleted.map((del) =>
                            del.Key ? this.denormalizeKey(del.Key) : "?del.Key undefined?"
                        )
                    );
                }
                try {
                    objects = (await this._list(path, true, undefined, { MaxKeys: DELETE_OBJECTS_MAX_KEYS })).objects;
                } catch (e) {
                    if (!(e instanceof MobilettoNotFoundError)) {
                        throw e instanceof MobilettoError
                            ? e
                            : new MobilettoError(`remove(${path}) error listing: ${e}`);
                    }
                    objects = null;
                }
            }
            return removed;
        } else {
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
                    await this.metadata(path);
                }
                const response = await this.client.send(new DeleteObjectCommand(bucketParams));
                const statusCode = response.$metadata.httpStatusCode;
                const statusClass = statusCode ? Math.floor(statusCode / 100) : -1;
                if (statusClass !== 2) {
                    throw new MobilettoError(`remove: DeleteObjectCommand returned HTTP status ${statusCode}`);
                }
            } catch (err) {
                throw this.s3error(err, Key, path, "remove");
            }
            return path;
        }
    }
}

export const storageClient = (key: string, secret: string, opts: S3Options) => {
    if (!key || !secret || !opts || !opts.bucket) {
        throw new MobilettoError("s3.storageClient: key, secret, and opts.bucket are required");
    }
    return new StorageClient(key, secret, opts);
};
