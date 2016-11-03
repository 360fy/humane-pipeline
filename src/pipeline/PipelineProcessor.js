import _ from 'lodash';
import Promise from 'bluebird';
import {EventEmitter} from 'events';
import highland from 'highland';
import ValidationError from 'humane-node-commons/lib/ValidationError';
import * as Pipeline from './Pipeline';
// import Joi from 'joi';

export default class PipelineProcessor {
    constructor(rootPipeline, settings, args) {
        // Joi.assert(rootPipeline, Joi.object().type(RootPipeline));

        this._rootPipeline = rootPipeline;
        this._settings = settings;
        this._args = args;

        this._params = _.defaults(this.resolveSettings(this.settings(), this.args()), this.args());

        this.eventEmitter = new EventEmitter();
    }

    rootPipeline() {
        return this._rootPipeline;
    }

    settings() {
        return this._settings;
    }

    args() {
        return this._args;
    }

    params() {
        return this._params;
    }

    resolveFromArg(setting, arg, argGroup) {
        // resolve it
        if (_.has(arg, setting.name())) {
            return _.get(arg, setting.name());
        }

        if (setting.required() && _.isUndefined(setting.defaultValue())) {
            throw new ValidationError(`Required ${argGroup} ${setting.name()} not provided`);
        }

        return setting.defaultValue();
    }

    mapSettingWithArgs(setting, args) {
        if (setting instanceof Pipeline.PipelineArg) {
            return this.resolveFromArg(setting, args, 'arg');
        } else if (setting instanceof Pipeline.PipelineEnvArg) {
            return this.resolveFromArg(setting, process.env, 'env');
        } else if ((_.isPlainObject(setting) && !_.isFunction(setting)) || _.isArray(setting)) {
            return this.resolveSettings(setting, args);
        }

        return setting;
    }

    resolveSettings(settings, args) {
        // for all params if any one is of type arg
        // fetch value from args and set it
        if (_.isArray(settings)) {
            return _.map(settings, setting => this.mapSettingWithArgs(setting, args));
        } else if (_.isPlainObject(settings) && !_.isFunction(settings)) {
            return _.mapValues(settings, setting => this.mapSettingWithArgs(setting, args));
        }

        return settings;
    }

    /*else if (childPipeline instanceof Pipeline.ExtDataMapPipeline) {
     // for join operation all fork needs to be collected
     buildStreamPipeline(highland(stream).fork(), childPipeline.pipelines());
     } */

    runPipeline(rootStream) {
        const promises = [];

        const that = this;

        function buildStreamPipeline(stream, pipelines) {
            _.forEach(pipelines, pipeline => {
                if (_.isArray(pipeline)) {
                    _.forEach(pipeline, childPipeline => {
                        if (childPipeline instanceof Pipeline.OutputPipeline) {
                            let settings = that.resolveSettings(childPipeline.settings(), that.args());

                            settings = _.defaults(settings, that.args());

                            promises.push(new Promise(resolve => childPipeline.outputProcessor()(childPipeline.key(), highland(stream).fork(), _.defaultsDeep({resolve}, settings))));
                        } else if (childPipeline instanceof Pipeline.ChildPipeline) {
                            // for join operation all fork needs to be collected
                            buildStreamPipeline(highland(stream).fork(), childPipeline.pipelines());
                        } else {
                            throw new ValidationError('Multi child pipeline can be either output or fork only');
                        }
                    });
                } else if (pipeline instanceof Pipeline.OutputPipeline) {
                    let settings = that.resolveSettings(pipeline.settings(), that.args());

                    settings = _.defaults(settings, that.args());

                    promises.push(new Promise(resolve => pipeline.outputProcessor()(pipeline.key(), highland(stream).fork(), _.defaultsDeep({resolve}, settings))));
                } else if (pipeline instanceof Pipeline.TransformPipeline) {
                    let settings = that.resolveSettings(pipeline.settings(), that.args());

                    settings = _.defaults(settings, that.args());

                    stream = pipeline.transformProcessor()(pipeline.key(), stream, settings);
                } else if (!(pipeline instanceof Pipeline.InputPipeline)) {
                    console.error('Pipeline: ', pipeline);
                    throw new ValidationError('Multi child pipeline can be either output or fork only');
                }
            });
        }

        try {
            buildStreamPipeline(rootStream, this.rootPipeline().pipelines());
        } catch (error) {
            return Promise.reject(error);
        }

        return Promise.all(promises);
    }

}