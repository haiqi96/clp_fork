import {QUERY_JOB_TYPE} from "../DbManager.js";


// eslint-disable-next-line no-magic-numbers
const EXTRACT_IR_TARGET_UNCOMPRESSED_SIZE = 128 * 1024 * 1024;
// eslint-disable-next-line no-magic-numbers
const EXTRACT_JSON_TARGET_CHUNK_SIZE = 100 * 1000;

/**
 * Creates query routes.
 *
 * @param {import("fastify").FastifyInstance | {dbManager: DbManager}} fastify
 * @param {import("fastify").FastifyPluginOptions} options
 * @return {Promise<void>}
 */
const routes = async (fastify, options) => {
    fastify.post("/query/extract-stream", async (req, resp) => {
        const {extractJobType, logEventIdx, streamId} = req.body;
        const sanitizedLogEventIdx = Number(logEventIdx);
        let streamMetadata = await fastify.dbManager.getExtractedStreamFileMetadata(
            streamId,
            sanitizedLogEventIdx
        );

        if (null === streamMetadata) {
            let jobConfig;
            if (QUERY_JOB_TYPE.EXTRACT_IR === extractJobType) {
                jobConfig = {
                    file_split_id: null,
                    msg_ix: sanitizedLogEventIdx,
                    orig_file_id: streamId,
                    target_uncompressed_size: EXTRACT_IR_TARGET_UNCOMPRESSED_SIZE,
                };
            } else if (QUERY_JOB_TYPE.EXTRACT_JSON === extractJobType) {
                jobConfig = {
                    archive_id: streamId,
                    target_chunk_size: EXTRACT_JSON_TARGET_CHUNK_SIZE,
                };
            } else {
                const err = new Error(`Unsupported Job type: ${extractJobType}`);
                err.statusCode = 400;
                throw err;
            }
            const extractResult = await fastify.dbManager.submitAndWaitForExtractStreamJob(
                jobConfig,
                extractJobType
            );

            if (null === extractResult) {
                const err = new Error("Unable to extract stream with " +
                    `streamId=${streamId} at logEventIdx=${sanitizedLogEventIdx}`);

                err.statusCode = 400;
                throw err;
            }
            streamMetadata = await fastify.dbManager.getExtractedStreamFileMetadata(
                streamId,
                logEventIdx
            );
        }

        return streamMetadata;
    });
};

export default routes;
