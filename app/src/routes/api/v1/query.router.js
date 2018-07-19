const Router = require('koa-router');
const logger = require('logger');
const fs = require('fs-promise');
const request = require('request');
const requestPromise = require('request-promise');
const s3 = require('s3');
const QueryService = require('services/query.service');
const passThrough = require('stream').PassThrough;
const ctRegisterMicroservice = require('ct-register-microservice-node');
const JSONAPIDeserializer = require('jsonapi-serializer').Deserializer;

const router = new Router();

const S3_ACCESS_KEY_ID = process.env.S3_ACCESS_KEY_ID;
const S3_SECRET_ACCESS_KEY = process.env.S3_SECRET_ACCESS_KEY;
const S3Client = s3.createClient({
    maxAsyncS3: 20, // this is the default
    s3RetryCount: 3, // this is the default
    s3RetryDelay: 1000, // this is the default
    multipartUploadThreshold: 20971520, // this is the default (20 MB)
    multipartUploadSize: 15728640, // this is the default (15 MB)
    s3Options: {
        accessKeyId: S3_ACCESS_KEY_ID,
        secretAccessKey: S3_SECRET_ACCESS_KEY,
        region: 'us-east-1'
    }
});


function getHeadersFromResponse(response) {
    const validHeaders = {};
    const keys = Object.keys(response.headers);
    for (let i = 0, length = keys.length; i < length; i++) {
        validHeaders[keys[i]] = response.headers[keys[i]];
    }
    logger.debug('Valid-headers', validHeaders);
    return validHeaders;
}

const deserializer = (obj) =>
    new Promise((resolve, reject) => {
        new JSONAPIDeserializer({
            keyForAttribute: 'camelCase'
        }).deserialize(obj, (err, data) => {
            if (err) {
                reject(err);
                return;
            }
            resolve(data);
        });
    });

class QueryRouter {

    static async uploadFileToS3(filePath, fileName) {
        try {
            logger.info('[SERVICE] Uploading to S3');
            const key = `freeze/${fileName}`;
            const params = {
                localFile: filePath,
                s3Params: {
                    Bucket: 'wri-api-backups',
                    Key: key,
                    ACL: 'public-read'
                }
            };
            const uploader = S3Client.uploadFile(params);
            await new Promise((resolve, reject) => {
                uploader.on('end', data => resolve(data));
                uploader.on('error', err => reject(err));
            });
            const s3file = s3.getPublicUrlHttp(params.s3Params.Bucket, params.s3Params.Key);
            return s3file;
        } catch (err) {
            throw err;
        }
    }

    static async freeze(options) {
        const nameFile = `${Date.now()}.json`;
        try {
            logger.debug('Obtaining data');
            const data = await requestPromise(options);
            await fs.writeFile(`/tmp/${nameFile}`, JSON.stringify(data));
            const storage = new Storage();
            // uploading
            logger.debug('uploading file');
            const url = await QueryRouter.uploadFileToS3(`/tmp/${nameFile}`, nameFile)
            return url;
        } catch (err) {
            logger.error(err);
            throw err;
        } finally {
            try {
                const exists = await fs.stat(`/tmp/${nameFile}`);
                if (exists) {
                    await fs.unlink(`/tmp/${nameFile}`);
                }
            } catch (err) {}
        }
    }

    static async query(ctx) {
        logger.info('Doing query');

        const options = await QueryService.getTargetQuery(ctx);
        logger.debug('Doing request to adapter', options);
        if (!ctx.query.freeze || ctx.query.freeze !== 'true') {
            const req = request(options);
            req.on('response', (response) => {
                ctx.response.status = response.statusCode;
                ctx.set(getHeadersFromResponse(response));
            });
            ctx.body = req.on('error', ctx.onerror).pipe(passThrough());
        } else {
            let loggedUser = ctx.request.body.loggedUser;
            if (!loggedUser && ctx.query.loggedUser) {
                loggedUser = JSON.parse(ctx.query.loggedUser);
            }
            if (!loggedUser) {
                ctx.throw(401, 'Not authenticated');
                return;
            }
            try {
                const url = await QueryRouter.freeze(options);
                ctx.body = {
                    url
                };
            } catch (err) {
                if (err.statusCode) {
                    ctx.throw(err.statusCode, err.details);
                    return;
                }
                ctx.throw(500, 'Internal server error');
                return;
            }
        }
    }

}

const datasetValidationMiddleware = async(ctx, next) => {
    logger.info(`[QueryRouter] Validating dataset sandbox prop`);
    let dataset;

    try {
        const datasetId = QueryService.getTableOfSql(ctx);
        dataset = await ctRegisterMicroservice.requestToMicroservice({
            uri: `/dataset/${datasetId}`,
            json: true,
            method: 'GET'
        });
        logger.debug('Dataset obtained correctly', dataset);
        dataset = await deserializer(dataset);
    } catch (err) {
        ctx.throw(403, 'Forbidden');
    }

    if (!dataset) {
        ctx.throw(404, 'Dataset not found');
    }

    if (!dataset.sandbox) {
        if (!ctx.request.body.loggedUser && !ctx.query.loggedUser) {
            ctx.throw(403, 'You need an API key to access this dataset. Visit https://apihighways.org/ to get yours.');
        }
        const loggedUser = ctx.request.body.loggedUser || JSON.parse(ctx.query.loggedUser);
        if (loggedUser.role === 'USER') {
            ctx.throw(403, 'Forbidden');
        }
    }

    await next();
};


router.post('/query', datasetValidationMiddleware, QueryRouter.query);
router.post('/download', datasetValidationMiddleware, QueryRouter.query);

module.exports = router;
