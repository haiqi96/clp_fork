import fastify from "fastify";
import * as path from "node:path";
import process from "node:process";

import {fastifyStatic} from "@fastify/static";

import DbManager from "./DbManager.js";
import exampleRoutes from "./routes/examples.js";


/**
 * Creates the Fastify app with the given options.
 *
 * @param {Object} options - The options for creating the Fastify app.
 * @param {import("fastify").FastifyServerOptions} options.fastifyOptions - The Fastify server options.
 * @param {string} options.dbPass - The MySQL database password.
 * @param {string} options.dbUser - The MySQL database user.
 * @param {string} options.clientDir Absolute path to the client directory to serve when in running in a
 * production environment.
 * @return {Promise<import("fastify").FastifyInstance>}
 **/
const app = async (options = {}) => {
    const server = fastify(options.fastifyOptions);
    const clientDir = options.clientDir;
    if ("production" === process.env.NODE_ENV) {
        // In the development environment, we expect the client to use a separate webserver that
        // supports live reloading.
        if (false === path.isAbsolute(clientDir)) {
            throw new Error("`clientDir` must be an absolute path.");
        }

        await server.register(fastifyStatic, {
            prefix: "/",
            root: clientDir,
        });
    }
    await server.register(exampleRoutes);
    await server.register(DbManager, {
        mysqlConfig: {
            host: "127.0.0.1",
            database: "clp-db",
            user: options.dbUser,
            password: options.dbPass,
            port: 3306,
            queryJobsTableName: "query_jobs",
        },
        mongoConfig: {
            host: "127.0.0.1",
            port: 27017,
            database: "clp-search",
            statsCollectionName: "stats",
        },
    });

    return server;
};

export default app;
