import _ from 'lodash';
import Joi from 'joi';
import Promise from 'bluebird';
import performanceNow from 'performance-now';
import MySql from 'mysql';
import ValidationError from 'humane-node-commons/lib/ValidationError';
import PipelineProcessor from '../pipeline/PipelineProcessor';
import * as BuilderUtils from './../pipeline/BuilderUtils';
// import FileStorage from 'lowdb/lib/file-sync';
// import lowDB from 'lowdb';

// const PROCESS_NEXT_EVENT = 'PROCESS_NEXT';

export const name = 'sql';

// connection settings = db, host, port = 3306, user, password, database, connectTimeout = 10000, ssl = {ca: } or Amazon RDS
// query
// query params
// mode: full or incremental
// incrementalModeSettings: {method: date or id, column: [columnA, columnB], pollEvery: 2 minutes}
class SqlInputProcessor extends PipelineProcessor {

    databaseDriver(params) {
        const db = _.get(params, ['connectionSettings', 'dbType']);
        if (db === 'mysql') {
            return MySql;
        }

        throw new ValidationError(`Unrecognised db in connection settings: ${db}`);
    }

    _defaultHost() {
        return 'localhost';
    }

    _defaultPort(params) {
        const db = _.get(params, ['connectionSettings', 'dbType']);
        if (db === 'mysql') {
            return 3306;
        }

        return null;
    }

    _defaultUser(params) {
        const db = _.get(params, ['connectionSettings', 'dbType']);
        if (db === 'mysql') {
            return 'root';
        }

        return null;
    }

    _defaultPassword(params) {
        const db = _.get(params, ['connectionSettings', 'dbType']);
        if (db === 'mysql') {
            return '';
        }

        return null;
    }

    _defaultConnectTimeout(/*params*/) {
        return 10000;
    }

    _connection(params) {
        const connection = this.databaseDriver(params).createConnection({
            host: _.get(params, ['connectionSettings', 'host'], this._defaultHost(params)),
            port: _.get(params, ['connectionSettings', 'port'], this._defaultPort(params)),
            user: _.get(params, ['connectionSettings', 'user'], this._defaultUser(params)),
            password: _.get(params, ['connectionSettings', 'password'], this._defaultPassword(params)),
            database: _.get(params, ['connectionSettings', 'database']),
            connectTimeout: _.get(params, ['connectionSettings', 'connectTimeout'], this._defaultConnectTimeout(params)),
            ssl: _.get(params, ['connectionSettings', 'ssl'])
        });

        return new Promise((resolve, reject) => {
            connection.connect((error) => {
                if (error) {
                    console.error('<< ERROR >> connecting', error, error.stack);
                    reject(`ERROR: Connection Error: ${error.message}`);
                }

                resolve(connection);
            });
        });
    }

    _endConnection(connection) {
        return new Promise((resolve, reject) => {
            connection.end(error => {
                if (error) {
                    // show error as warning
                    console.error('<< ERROR >> Closing connection', error, error.stack);
                    reject(`ERROR: Connection Close Error: ${error.message}`);
                }
            });

            resolve(true);
        });
    }

    _query(params) {
        // build query from query and query params
        const compiledQuery = _.template(params.query);

        const queryParams = _.defaults({}, params.queryParams, params);
        const escapedParams = _.mapValues(queryParams, value => {
            if (_.isString(value)) {
                return MySql.escape(value);
            }

            return value;
        });

        return compiledQuery(escapedParams);
    }

    _runQuery(connection, params) {
        return connection.query(this._query(params))
          .stream({highWaterMark: 5});
    }

    _fetch(params, resolve, reject) {
        const startTime = performanceNow();

        let connection = null;

        this._connection(params)
          .then(arg => {
              connection = arg;
              return this._runQuery(connection, params);
          })
          .then(queryStream => {
              const promises = this.runPipeline(queryStream);

              return Promise.all(promises);
              
                // .then(() => {
                //     // console.log(`Completed processing in: ${(performanceNow() - startTime).toFixed(3)}ms`);
                //
                //     // if (params._watch) {
                //     //     this.running = false;
                //     //     this.eventEmitter.emit(PROCESS_NEXT_EVENT);
                //     // }
                //
                //     return true;
                // });
          })
          .then(() => this._endConnection(connection))
          .then(() => resolve(true))
          .catch(error => {
              console.error('<< ERROR >>', error, error.stack);
              reject(error);
          });
    }

    run() {
        const params = this.resolveSettings(this.settings(), this.args());

        const that = this;

        // if (!params || !params.input) {
        //     throw new ValidationError('Must pass input path!');
        // }
        //
        // if (params.watch) {
        //     return this._watch(params);
        // }

        // TODO: if it is poll... then poll as per configuration
        // TODO: let poller execute the fetch


        return new Promise((resolve, reject) => that._fetch(params, resolve, reject));
    }
}

const ConnectionSettingsSchema = Joi.object().keys({
    dbType: Joi.string().required(),
    host: Joi.string().optional(),
    port: Joi.number().integer().optional(),
    user: Joi.string().optional(),
    password: Joi.string().optional(),
    database: Joi.string().optional().required(),
    connectTimeout: Joi.number().integer().optional(),
    ssl: Joi.alternatives([Joi.string(), Joi.object().keys({ca: Joi.string()})]).optional()
});

const IncrementalModeSettingsSchema = Joi.object().keys({
    method: Joi.string().required().valid('date', 'id'),
    column: Joi.alternatives([Joi.string(), Joi.array().items(Joi.string())]).required(),
    pollEvery: Joi.number().integer().optional()
}).optional();

export function builder(buildKey, connectionSettings, query, queryParams, mode = 'full', incrementalModeSettings) {
    BuilderUtils.validateSettingsWithSchema(buildKey, connectionSettings, ConnectionSettingsSchema, 'connectionSettings');
    BuilderUtils.validateSettingsWithSchema(buildKey, query, Joi.string().required(), 'query');
    BuilderUtils.validateSettingsWithSchema(buildKey, mode, Joi.string().required().valid('full', 'incremental'), 'mode');
    BuilderUtils.validateSettingsWithSchema(buildKey, incrementalModeSettings, IncrementalModeSettingsSchema, 'incrementalModeSettings');

    return {
        settings: {connectionSettings, query, queryParams, mode, incrementalModeSettings},
        inputProcessor: (rootPipeline, params, args) => new SqlInputProcessor(rootPipeline, params, args)
    };
}