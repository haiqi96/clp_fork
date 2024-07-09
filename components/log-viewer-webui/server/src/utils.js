import dotenv from "dotenv";


/**
 * Parses environment variables into config values for the application.
 *
 * @return {{HOST: string, PORT: string, CLP_DB_USER: string, CLP_DB_PASS: string, CLP_DB_PORT: string, CLP_DB_HOST: string, RESULTS_CACHE_HOST: string, RESULTS_CACHE_PORT: string}}
 * @throws {Error} if any required environment variable is undefined.
 */
const parseEnvVars = () => {
    dotenv.config({
        path: ".env",
    });

    const {
        HOST, PORT, CLP_DB_USER, CLP_DB_PASS, CLP_DB_PORT, CLP_DB_HOST, RESULTS_CACHE_HOST, RESULTS_CACHE_PORT
    } = process.env;
    const envVars = {
        HOST, PORT, CLP_DB_USER, CLP_DB_PASS, CLP_DB_PORT, CLP_DB_HOST, RESULTS_CACHE_HOST, RESULTS_CACHE_PORT
    };

    // Check for mandatory environment variables
    for (const [key, value] of Object.entries(envVars)) {
        if ("undefined" === typeof value) {
            throw new Error(`Environment variable ${key} must be defined.`);
        }
    }

    return envVars;
};

export {parseEnvVars};
