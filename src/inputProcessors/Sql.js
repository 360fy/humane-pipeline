import _ from 'lodash';
import Joi from 'joi';
import Promise from 'bluebird';
import performanceNow from 'performance-now';
import MySql from 'mysql';
import ValidationError from 'humane-node-commons/lib/ValidationError';
import PipelineProcessor from '../pipeline/PipelineProcessor';
import * as BuilderUtils from './../pipeline/BuilderUtils';

export const name = 'sql';

// connection settings = db, host, port = 3306, user, password, database, connectTimeout = 10000, ssl = {ca: } or Amazon RDS
// query
// query params
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
        const query = this._query(params);

        console.log('>> Executing query: ', query);

        return connection.query(query)
          .stream({highWaterMark: 5});
    }

    _fetch(params, resolve, reject) {
        let connection = null;

        this._connection(params)
          .then(arg => {
              connection = arg;
              return this._runQuery(connection, params);
          })
          .then(queryStream => this.runPipeline(queryStream))
          .then(() => this._endConnection(connection))
          .then(() => resolve(true))
          .catch(error => {
              console.error('<< ERROR >>', error, error.stack);
              reject(error);
          });
    }

    run() {
        const that = this;

        return new Promise((resolve, reject) => that._fetch(that.params(), resolve, reject));
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

export function builder(buildKey, connectionSettings, query, queryParams, LAST_RUN_TIME) {
    BuilderUtils.validateSettingsWithSchema(buildKey, connectionSettings, ConnectionSettingsSchema, 'connectionSettings');
    BuilderUtils.validateSettingsWithSchema(buildKey, query, Joi.string().required(), 'query');

    return {
        settings: {connectionSettings, query, queryParams, LAST_RUN_TIME},
        inputProcessor: (rootPipeline, params, args) => new SqlInputProcessor(rootPipeline, params, args)
    };
}