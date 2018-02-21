const Router = require('koa-router');
const logger = require('logger');
const fs = require('fs-promise');
const request = require('request');
const requestPromise = require('request-promise');
const Storage = require('@google-cloud/storage');
const QueryService = require('services/query.service');
const passThrough = require('stream').PassThrough;
const ctRegisterMicroservice = require('ct-register-microservice-node');
const JSONAPIDeserializer = require('jsonapi-serializer').Deserializer;

const router = new Router();

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

    static async freeze(options) {
        const nameFile = `${Date.now()}.json`;
        try {
            logger.debug('Obtaining data');
            const data = await requestPromise(options);
            await fs.writeFile(`/tmp/${nameFile}`, JSON.stringify(data));

            const storage = new Storage();
            // uploading
            logger.debug('uploading file');
            await storage
            .bucket(process.env.BUCKET_FREEZE)
            .upload(`/tmp/${nameFile}`);
            logger.debug('making public');
            const file = await storage
            .bucket(process.env.BUCKET_FREEZE)
            .file(nameFile)
            .makePublic();
            logger.debug('file', file);
            return `https://storage.googleapis.com/${process.env.BUCKET_FREEZE}/${nameFile}`;
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
    try {
        const loggedUser = ctx.request.body.loggedUser || JSON.parse(ctx.query.loggedUser);
        const datasetId = QueryService.getTableOfSql(ctx);
        let dataset = await ctRegisterMicroservice.requestToMicroservice({
            uri: `/dataset/${datasetId}`,
            json: true,
            method: 'GET'
        });
        logger.debug('Dataset obtained correctly', dataset);
        dataset = await deserializer(dataset);
        if (!dataset.sandbox) {
            if (loggedUser.role === 'USER') {
                throw new Error('Not in the sandbox');
            }
        }
    } catch (err) {
        ctx.throw(403, 'Forbidden');
    }
    await next();
};


router.post('/query', datasetValidationMiddleware, QueryRouter.query);
router.post('/download', datasetValidationMiddleware, QueryRouter.query);

module.exports = router;
