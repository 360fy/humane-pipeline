/* eslint-disable no-unused-expressions, no-use-before-define */

import _ from 'lodash';
import pipelineRegistry from '../PipelineRegistry';
import * as Pipeline from './Pipeline';
import BuilderError from './BuilderError';
import PipelineTypes from './PipelineTypes';
import {extMapperWriterBuilder} from './../transforms/ExtMapperTransform';

const DefaultPipelineParams = {memorySize: 2048, gcInterval: 50, concurrency: 1};

class HasPath {

    constructor() {
        this._path = null;
        this._parentPath = null;
    }

    _setPath(name) {
        if (!this._path) {
            if (this._parentPath) {
                this._path = `${this._parentPath}=>${name}`;
            } else {
                this._path = name;
            }
        } else {
            this._path = `${this._path}/${name}`;
        }
    }
}

class BasePipelineBuilder extends HasPath {
    constructor(parent, type, settings) {
        super();

        this._type = type;

        this._parent = parent;
        this._parentPath = !!this._parent ? this._parent._path : null;
        this._path = null;
        this._key = null;
        this._name = null;
        this._terminates = false;
        this._settings = settings;
    }

    batch(builderFn) {
        let target = null;
        if (this instanceof OutputPipelineBuilder) {
            if (!this._supportsBatch) {
                throw new BuilderError('Does not support batch', this._key);
            }

            target = this;
        } else if (this instanceof PipelineBuilder) {
            const lastPipelineBuilder = _.last(this._pipelineBuilders);
            if (!lastPipelineBuilder) {
                throw new BuilderError('No pipeline to batch', lastPipelineBuilder._key);
            } else if (!lastPipelineBuilder._supportsBatch) {
                throw new BuilderError('Does not support batch', lastPipelineBuilder._key);
            }

            target = lastPipelineBuilder;
        }

        if (target._splitter || target._batcher) {
            throw new BuilderError('Splitter or batcher already defined', target._key);
        }

        this._setPath('batch');

        if (!builderFn || !_.isFunction(builderFn)) {
            throw new BuilderError('Must be a proper builder function', this._path);
        }

        const batcherBuilder = new BatcherBuilder(this._path);

        builderFn(batcherBuilder);

        target._batcher = batcherBuilder.build();

        return this;
    }

    split(builderFn) {
        let target = null;
        if (this instanceof OutputPipelineBuilder) {
            if (!this._supportsSplit) {
                throw new BuilderError('Does not support split', this._key);
            }

            target = this;
        } else if (this instanceof PipelineBuilder) {
            const lastPipelineBuilder = _.last(this._pipelineBuilders);
            if (!lastPipelineBuilder || !(lastPipelineBuilder instanceof TransformPipelineBuilder)) {
                throw new BuilderError('No pipeline to split', lastPipelineBuilder._key);
            } else if (!lastPipelineBuilder._supportsSplit) {
                throw new BuilderError('Does not support split', lastPipelineBuilder._key);
            }

            target = lastPipelineBuilder;
        }

        if (target._splitter || target._batcher) {
            throw new BuilderError('Splitter or batcher already defined', target._key);
        }

        this._setPath('split');

        if (!builderFn || !_.isFunction(builderFn)) {
            throw new BuilderError('Must be a proper builder function', this._path);
        }

        const splitterBuilder = new SplitterBuilder(this._path);

        builderFn(splitterBuilder);

        target._splitter = splitterBuilder.build();

        return this;
    }

    build() {
        // do nothing...
    }
}

function extendWithPipelineDef(target, key, pipelineDef, settings) {
    target._name = key;

    if (_.has(pipelineDef, 'defaultArgs')) {
        target._defaultArgs = pipelineDef.defaultArgs();
    }

    if (!settings) {
        settings = target._defaultArgs;
    } else if (target._defaultArgs) {
        settings = _.defaultsDeep(settings, target._defaultArgs);
    }

    target._settings = settings;
    target._supportsSplit = pipelineDef.supportsSplit && pipelineDef.supportsSplit() || false;
    target._supportsBatch = pipelineDef.supportsBatch && pipelineDef.supportsBatch() || false;
}

class OutputPipelineBuilder extends BasePipelineBuilder {
    constructor(parent) {
        super(parent, PipelineTypes.output);
        this._terminates = true;
        this._outputProcessor = null;

        this._extendWithPlugins();
    }

    _extendWithPlugins() {
        _.forEach(pipelineRegistry.outputPipelines(), (pipelineDef, key) => {
            this[key] = (...settings) => {
                this._setPath(key);

                const obj = pipelineDef.builder(this._path, ...settings);

                extendWithPipelineDef(this, key, pipelineDef, obj.settings);

                this._outputProcessor = obj.outputProcessor;

                return this;
            };
        });
    }

    build(pathKey, parentPipeline) {
        let key = pathKey;

        if (!this._outputProcessor) {
            throw new BuilderError('Output method must be defined.', key);
        }

        key = `${key}=>${this._name}`;

        if (this._settings && !_.isObject(this._settings)) {
            throw new BuilderError('Settings must be key-value map', key);
        }

        this._settings = _.assign(this._settings || {}, _.pick(this, '_splitter', '_batcher'));

        return new Pipeline.OutputPipeline(key, parentPipeline, this._name, this._outputProcessor, this._settings, this._defaultArgs);
    }
}

class InputPipelineBuilder extends BasePipelineBuilder {
    constructor(parent) {
        super(parent, PipelineTypes.input);

        this._inputProcessor = null;

        this._extendWithPlugins();
    }

    _extendWithPlugins() {
        _.forEach(pipelineRegistry.inputPipelines(), (pipelineDef, key) => {
            this[key] = (...settings) => {
                this._setPath(key);

                const obj = pipelineDef.builder(this._path, ...settings);

                extendWithPipelineDef(this, key, pipelineDef, obj.settings);

                this._inputProcessor = obj.inputProcessor;

                return this;
            };
        });
    }

    build(pathKey, parentPipeline) {
        if (parentPipeline._type !== PipelineTypes.root && parentPipeline._type !== PipelineTypes.extDataMap) {
            throw new BuilderError('Input is only allowed for root pipeline', pathKey);
        }

        let key = pathKey;

        if (!this._inputProcessor) {
            throw new BuilderError('Input method must be defined.', key);
        }

        key = `${key}=>${this._name}`;

        if (this._settings && !_.isObject(this._settings)) {
            throw new BuilderError('Settings must be key-value map', key);
        }

        this._settings = _.assign(this._settings || {}, _.pick(this, '_splitter', '_batcher'));

        return new Pipeline.InputPipeline(key, parentPipeline, this._name, this._inputProcessor, this._settings, this._defaultArgs);
    }
}

class TransformPipelineBuilder extends BasePipelineBuilder {
    constructor(parent, name, transformProcessor, settings, pipelineDef) {
        super(parent, PipelineTypes.transform, settings);

        this._name = name;
        this._transformProcessor = transformProcessor;
        this._path = parent && parent._path;
        this._supportsSplit = pipelineDef && pipelineDef.supportsSplit && pipelineDef.supportsSplit() || false;
        this._supportsBatch = pipelineDef && pipelineDef.supportsBatch && pipelineDef.supportsBatch() || false;
    }

    build(pathKey, parentPipeline) {
        if (this._settings && !_.isObject(this._settings)) {
            throw new BuilderError('Settings must be key-value map', pathKey);
        }

        this._settings = _.assign(this._settings || {}, _.pick(this, '_splitter', '_batcher'));

        return new Pipeline.TransformPipeline(pathKey, parentPipeline, this._name, this._transformProcessor, this._settings);
    }
}

class PipelineBuilder extends BasePipelineBuilder {

    constructor(parent, type, settings) {
        super(parent, type, settings);

        this._path = null;
        this._pipelineBuilders = [];

        this._extendWithPlugins();
    }

    _push(type, pipelineBuilder) {
        this._pipelineBuilders.push({type, key: this._path, builder: pipelineBuilder});

        return this;
    }

    _extendWithPlugins() {
        _.forEach(pipelineRegistry.transformPipelines(), (pipelineDef, key) => {
            this[key] = (...settings) => {
                this._setPath(key);

                const obj = pipelineDef.builder(this._path, ...settings);

                return this._push(
                  PipelineTypes.transform,
                  new TransformPipelineBuilder(this, key, obj.transformProcessor, obj.settings, pipelineDef));
            };
        });
    }

    fork(builderFn) {
        this._setPath(PipelineTypes.fork);

        if (!_.isFunction(builderFn)) {
            throw new BuilderError('Must be a fork builder function', this._path);
        }

        const builder = new ForkPipelineBuilder(this);

        builderFn(builder);

        return this._push(PipelineTypes.fork, builder);
    }

    output(builderFn) {
        this._setPath(PipelineTypes.output);

        if (!_.isFunction(builderFn)) {
            throw new BuilderError('Must be an output builder function', this._path);
        }

        const builder = new OutputPipelineBuilder(this);

        builderFn(builder);

        return this._push(PipelineTypes.output, builder);
    }

    buildPipelines(root, parentPipeline, pipelineBuilders) {
        const pipelines = [];

        // null ==> any
        let nextAllowedType = root ? PipelineTypes.input : null;
        let currState = null;
        let lastKey = null;
        let lastState = null;
        let seenChildOrOutput = false;

        _.forEach(pipelineBuilders, ({type, key, builder}) => {
            if (nextAllowedType && nextAllowedType !== type) {
                // throw error
                throw new BuilderError(`Only '${nextAllowedType}' allowed`, key);
            }

            if (type === PipelineTypes.fork) {
                nextAllowedType = PipelineTypes.fork;
                currState = PipelineTypes.fork;
            } else if (type === PipelineTypes.input) {
                nextAllowedType = null;
                currState = PipelineTypes.input;
            } else if (type === PipelineTypes.output) {
                nextAllowedType = PipelineTypes.output;
                currState = PipelineTypes.output;
            } else if (type === PipelineTypes.transform) {
                nextAllowedType = null;
                currState = PipelineTypes.transform;
            } else {
                throw new BuilderError(`'${type}' call not allowed`, key);
            }

            const pipeline = builder.build(key, parentPipeline);

            if (currState === PipelineTypes.fork || currState === PipelineTypes.output) {
                // push pipeline in an array
                if (!seenChildOrOutput) {
                    pipelines.push([]);
                    seenChildOrOutput = true;
                }

                _.last(pipelines).push(pipeline);
            } else {
                pipelines.push(pipeline);
            }

            lastKey = key;
            lastState = currState;

            return true;
        });

        if (lastState === PipelineTypes.child && _.last(pipelines).length === 1) {
            throw new BuilderError('Does not make sense to fork to single child, must be multiple', lastKey);
        }

        return pipelines;
    }
}

// proxy of methods added from registry
class ForkPipelineBuilder extends PipelineBuilder {
    constructor(parent) {
        super(parent, PipelineTypes.fork);
    }

    build(pathKey, parentPipeline) {
        const pipeline = new Pipeline.ChildPipeline(pathKey, parentPipeline);

        pipeline._pipelines = this.buildPipelines(false, pipeline, this._pipelineBuilders);

        return pipeline;
    }
}

export class ExtDataMapPipelineBuilder extends BasePipelineBuilder {

    constructor(mapperFn, settings) {
        super(null, PipelineTypes.extDataMap, settings);

        this._mapperFn = mapperFn;
        this._path = null;
        this._pipelineBuilders = [];

        this._extendWithPlugins();
    }

    _push(type, pipelineBuilder) {
        this._pipelineBuilders.push({type, key: this._path, builder: pipelineBuilder});

        return this;
    }

    _extendWithPlugins() {
        _.forEach(pipelineRegistry.transformPipelines(), (pipelineDef, key) => {
            this[key] = (...settings) => {
                this._setPath(key);

                const obj = pipelineDef.builder(this._path, ...settings);

                return this._push(
                  PipelineTypes.transform,
                  new TransformPipelineBuilder(this, key, obj.transformProcessor, obj.settings, pipelineDef));
            };
        });
    }

    input(builderFn) {
        this._setPath(PipelineTypes.input);

        if (!_.isFunction(builderFn)) {
            throw new BuilderError('Must be an input builder function', this._path);
        }

        const builder = new InputPipelineBuilder(this);

        builderFn(builder);

        return this._push(PipelineTypes.input, builder);
    }

    build() {
        const extDataMapPipeline = new Pipeline.ExtDataMapPipeline(this._key, this._settings);

        const pipelines = [];

        // null ==> any
        let nextAllowedType = PipelineTypes.input;

        _.forEach(this._pipelineBuilders, ({type, key, builder}) => {
            if (nextAllowedType && nextAllowedType !== type) {
                // throw error
                throw new BuilderError(`Only '${nextAllowedType}' allowed`, key);
            }

            if (type === PipelineTypes.input) {
                nextAllowedType = null;
            } else if (type === PipelineTypes.transform) {
                nextAllowedType = null;
            } else {
                throw new BuilderError(`'${type}' call not allowed`, key);
            }

            const pipeline = builder.build(key, extDataMapPipeline);

            pipelines.push(pipeline);

            return true;
        });

        const obj = extMapperWriterBuilder(this._path, this._mapperFn);

        pipelines.push(new Pipeline.OutputPipeline(this._path, extDataMapPipeline, 'MapperFnWriter', obj.outputProcessor, obj.settings));

        extDataMapPipeline._pipelines = pipelines;

        return extDataMapPipeline;
    }

}

// proxy of basic pipeline builder methods and methods added from registry
class RootPipelineBuilder extends PipelineBuilder {
    constructor(name) {
        super(null, PipelineTypes.root, DefaultPipelineParams);
        this._name = name;
        this._key = PipelineTypes.root;
        this._path = this._name;
        this._args = {};
    }

    input(builderFn) {
        this._setPath(PipelineTypes.input);

        if (!_.isFunction(builderFn)) {
            throw new BuilderError('Must be an input builder function', this._path);
        }

        const builder = new InputPipelineBuilder(this);

        builderFn(builder);

        return this._push(PipelineTypes.input, builder);
    }

    withArg(name, builderFn) {
        this._setPath('withArg');

        if (!_.isFunction(builderFn)) {
            throw new BuilderError('Must be an arg builder function', this._path);
        }

        if (_.has(this._args, name)) {
            throw new BuilderError(`${name} arg is already defined earlier`, this._path);
        }

        const builder = new ArgBuilder(name, this._path);

        builderFn(builder);

        this._args[name] = builder.build();
        return this;
    }

    arg(name, required, defaultValue) {
        if (!name || _.isEmpty(name)) {
            throw new BuilderError('Should be a valid arg name');
        }

        if (!_.has(this._args, name)) {
            throw new BuilderError(`Found an undefined arg '${name}'`);
        }

        return new Pipeline.PipelineArg(name, required, defaultValue);
    }

    env(name, required, defaultValue) {
        if (!name || _.isEmpty(name)) {
            throw new BuilderError('Should be a valid env name');
        }

        return new Pipeline.PipelineEnvArg(name, required, defaultValue);
    }

    withSetting(key, value) {
        this._setPath('withSetting');

        if (!key) {
            throw new BuilderError('Setting key must be defined', this._path);
        }

        if (_.has(this._settings, key)) {
            console.warn(`At ${this._path} overwriting '${key}' setting value: ${_.get(this._settings, key)} with new value: ${value}`);
        }

        _.set(this._settings, key, value);
        return this;
    }

    // recursively check for terminates
    validatePipeline(pipeline) {
        if (!_.isObject(pipeline)) {
            return false;
        }

        // this would be output pipeline
        if (pipeline instanceof Pipeline.OutputPipeline) {
            return true;
        }

        if (!pipeline instanceof Pipeline.ChildPipeline) {
            throw new BuilderError('Pipeline must be either OutputPipeline or ChildPipeline', pipeline.key());
        }

        return this.validatePipelines(pipeline.pipelines());
    }

    validatePipelines(pipelines) {
        const lastPipeline = _.last(pipelines);
        if (_.isArray(lastPipeline)) {
            // for all it shall terminate
            return _.every(lastPipeline, childPipeline => this.validatePipeline(childPipeline));
        }

        // this is good
        if (lastPipeline instanceof Pipeline.OutputPipeline) {
            return true;
        }

        throw new BuilderError('Pipeline must be OutputPipeline', lastPipeline.key());
    }

    collectDefaultArgs(pipeline) {
        if (_.isArray(pipeline)) {
            _.forEach(pipeline, child => {
                this.collectDefaultArgs(child);
                return true;
            });
        } else if (pipeline instanceof Pipeline.ChildPipeline || pipeline instanceof Pipeline.RootPipeline || pipeline instanceof Pipeline.ExtDataMapPipeline) {
            this.collectDefaultArgs(pipeline.pipelines());
        } else if (pipeline.defaultArgs && _.isFunction(pipeline.defaultArgs)) {
            const defaultArgs = pipeline.defaultArgs();
            if (!defaultArgs) {
                return;
            }

            if (!_.isObject(defaultArgs)) {
                throw new BuilderError('Default args must be key-value map', pipeline.key());
            }

            _.forEach(defaultArgs, (arg, key) => {
                if (_.has(this._args, key)) {
                    if (_.get(this._args, key).required()) {
                        throw new BuilderError(`Duplicate default arg: ${key}`, pipeline.key());
                    }

                    return;
                }

                const setting = pipeline.settings(key);
                if (!setting || setting instanceof Pipeline.PipelineArg && setting.name() === key) {
                    _.set(this._args, key, arg);
                }
            });
        }
    }

    build() {
        const rootPipeline = new Pipeline.RootPipeline(PipelineTypes.root, this._settings, this._args);

        const pipelines = rootPipeline._pipelines = this.buildPipelines(true, rootPipeline, this._pipelineBuilders);

        this.validatePipelines(pipelines);

        if (_.isEmpty(rootPipeline.args())) {
            this.collectDefaultArgs(pipelines);
        }

        return rootPipeline;
    }
}

export class ArgBuilder extends HasPath {

    constructor(name, parentPath) {
        super();

        this._name = name;
        this._parentPath = parentPath;
        this._required = false;
        this._type = 'string';
        this._validValues = null;

        if (!this._name) {
            throw new BuilderError('Must specify arg name', this._parentPath);
        }
    }

    required() {
        this._setPath('required');

        if (this._required) {
            throw new BuilderError('Duplicate call to required()', this._path);
        }

        this._required = true;
        return this;
    }

    boolean() {
        this._setPath('boolean');

        if (this._type && this._type !== 'string') {
            throw new BuilderError(`Duplicate type set, existing type: ${this._type}`, this._path);
        }

        this._type = 'boolean';
        return this;
    }

    validValues(...values) {
        this._setPath('validValues');

        if (this._type && this._type !== 'string') {
            throw new BuilderError(`Duplicate type set, existing type: ${this._type}`, this._path);
        }

        if (!values) {
            throw new BuilderError('Must specify one or more valid values', this._path);
        }

        this._type = 'enum';
        this._validValues = values;
        return this;
    }

    short(value) {
        this._setPath('short');

        if (this._shortName) {
            throw new BuilderError('Duplicate call to short()', this._path);
        }

        this._shortName = value;
        return this;
    }

    description(value) {
        this._setPath('description');

        if (this._description) {
            throw new BuilderError('Duplicate call to description()', this._path);
        }

        this._description = value;
        return this;
    }

    defaultValue(value) {
        this._setPath('defaultValue');

        if (this._defaultValue) {
            throw new BuilderError('Duplicate call to defaultValue()', this._path);
        }

        this._defaultValue = value;
        return this;
    }

    build() {
        return new Pipeline.PipelineArg(this._name, this._required, this._defaultValue, this._type, this._shortName, this._description, this._validValues);
    }

}

class BatcherBuilder extends HasPath {

    constructor(parentPath) {
        super();

        this._parentPath = parentPath;
    }

    checkDuplicateCall() {
        if (this._by) {
            throw new BuilderError('Duplicate call. Already one of byDays(), byHours(), byMinutes(), bySeconds(), byRecords() has been called.', this._path);
        }
    }

    byWeeks(value = 1) {
        this._setPath('byWeeks');

        this.checkDuplicateCall();

        if (!value) {
            throw new BuilderError('Weeks value must be defined', this._path);
        }

        this._by = 'weeks';
        this._num = value;
        this._datePrefixFormat = 'YYYY';
        this._dateSuffixPart = 'weeks';
        this._dateSufixLength = 2;

        return this;
    }

    byDays(value = 1) {
        this._setPath('byDays');

        this.checkDuplicateCall();

        if (!value) {
            throw new BuilderError('Days value must be defined', this._path);
        }

        this._by = 'days';
        this._num = value;
        this._datePrefixFormat = 'YYYYMM';
        this._dateSuffixPart = 'dates';
        this._dateSufixLength = 2;

        return this;
    }

    byHours(value = 1) {
        this._setPath('byHours');

        this.checkDuplicateCall();

        if (!value) {
            throw new BuilderError('Hours value must be defined', this._path);
        }

        this._by = 'hours';
        this._num = value;
        this._datePrefixFormat = 'YYYYMMDD';
        this._dateSuffixPart = 'hours';
        this._dateSufixLength = 2;

        return this;
    }

    byMinutes(value = 1) {
        this._setPath('byMinutes');

        this.checkDuplicateCall();

        if (!value) {
            throw new BuilderError('Minutes value must be defined', this._path);
        }

        this._by = 'minutes';
        this._num = value;
        this._datePrefixFormat = 'YYYYMMDDHH';
        this._dateSuffixPart = 'minutes';
        this._dateSufixLength = 2;

        return this;
    }

    bySeconds(value = 10) {
        this._setPath('bySeconds');

        this.checkDuplicateCall();

        if (!value) {
            throw new BuilderError('Seconds value must be defined', this._path);
        }

        this._by = 'seconds';
        this._num = value;
        this._datePrefixFormat = 'YYYYMMDDHHmm';
        this._dateSuffixPart = 'seconds';
        this._dateSufixLength = 2;

        return this;
    }

    byRecords(value = 100) {
        this._setPath('byRecords');

        this.checkDuplicateCall();

        if (!value) {
            throw new BuilderError('Records value must be defined', this._path);
        }

        this._by = 'records';
        this._num = value;

        return this;
    }

    build() {
        if (!this._by) {
            throw new BuilderError('One of byDays(), byHours(), byMinutes(), bySeconds(), byRecords() must be called.', this._parentPath);
        }

        if (this._unit === 'records') {
            return new Pipeline.RecordsBasedBatcher(this._num);
        }

        return new Pipeline.TimeBasedBatcher(this._by, this._num, this._datePrefixFormat, this._dateSuffixPart, this._dateSufixLength);
    }

}

class SplitterBuilder extends HasPath {

    constructor(parentPath) {
        super();

        this._parentPath = parentPath;
    }

    checkDuplicateCall() {
        if (this._by) {
            throw new BuilderError('Duplicate call. Already one of byDays(), byHours(), byMinutes(), bySeconds(), byDateField(), byField(), byFn() has been called.', this._path);
        }
    }

    byWeeks(field, dateFieldFormat, value = 1, defaultValue) {
        this._setPath('byWeeks');

        this.checkDuplicateCall();

        if (!field) {
            throw new BuilderError('Field must be defined', this._path);
        }

        if (!value) {
            throw new BuilderError('Weeks value must be defined', this._path);
        }

        this._by = 'weeks';
        this._field = field;
        this._num = value;
        this._dateFieldFormat = dateFieldFormat;
        this._datePrefixFormat = 'YYYY';
        this._dateSuffixPart = 'weeks';
        this._dateSufixLength = 2;
        this._defaultValue = defaultValue;

        return this;
    }

    byDays(field, dateFieldFormat, value = 1, defaultValue) {
        this._setPath('byDays');

        this.checkDuplicateCall();

        if (!field) {
            throw new BuilderError('Field must be defined', this._path);
        }

        if (!value) {
            throw new BuilderError('Days value must be defined', this._path);
        }

        this._by = 'days';
        this._field = field;
        this._num = value;
        this._dateFieldFormat = dateFieldFormat;
        this._datePrefixFormat = 'YYYYMM';
        this._dateSuffixPart = 'date';
        this._dateSufixLength = 2;
        this._defaultValue = defaultValue;

        return this;
    }

    byHours(field, dateFieldFormat, value = 1, defaultValue) {
        this._setPath('byHours');

        this.checkDuplicateCall();

        if (!field) {
            throw new BuilderError('Field must be defined', this._path);
        }

        if (!value) {
            throw new BuilderError('Hours value must be defined', this._path);
        }

        this._by = 'hours';
        this._field = field;
        this._num = value;
        this._dateFieldFormat = dateFieldFormat;
        this._datePrefixFormat = 'YYYYMMDD';
        this._dateSuffixPart = 'hours';
        this._dateSufixLength = 2;
        this._defaultValue = defaultValue;

        return this;
    }

    byMinutes(field, dateFieldFormat, value = 1, defaultValue) {
        this._setPath('byMinutes');

        this.checkDuplicateCall();

        if (!field) {
            throw new BuilderError('Field must be defined', this._path);
        }

        if (!value) {
            throw new BuilderError('Minutes value must be defined', this._path);
        }

        this._by = 'minutes';
        this._field = field;
        this._num = value;
        this._dateFieldFormat = dateFieldFormat;
        this._datePrefixFormat = 'YYYYMMDDHH';
        this._dateSuffixPart = 'minutes';
        this._dateSufixLength = 2;
        this._defaultValue = defaultValue;

        return this;
    }

    bySeconds(field, dateFieldFormat, value = 10, defaultValue) {
        this._setPath('bySeconds');

        this.checkDuplicateCall();

        if (!field) {
            throw new BuilderError('Field must be defined', this._path);
        }

        if (!value) {
            throw new BuilderError('Seconds value must be defined', this._path);
        }

        this._by = 'seconds';
        this._field = field;
        this._num = value;
        this._dateFieldFormat = dateFieldFormat;
        this._datePrefixFormat = 'YYYYMMDDHHmm';
        this._dateSuffixPart = 'seconds';
        this._dateSufixLength = 2;
        this._defaultValue = defaultValue;

        return this;
    }

    byDateField(field, dateOutputFormat, dateFieldFormat, defaultValue) {
        this._setPath('byDateField');

        this.checkDuplicateCall();

        if (!field) {
            throw new BuilderError('Field must be defined', this._path);
        }

        if (!dateOutputFormat) {
            throw new BuilderError('Date format must be defined', this._path);
        }

        this._by = 'dateField';
        this._field = field;
        this._dateOutputFormat = dateOutputFormat;
        this._dateFieldFormat = dateFieldFormat;
        this._defaultValue = defaultValue;

        return this;
    }

    byField(field, defaultValue) {
        this._setPath('byField');

        this.checkDuplicateCall();

        if (!field) {
            throw new BuilderError('Field must be defined', this._path);
        }

        this._by = 'field';
        this._field = field;
        this._defaultValue = defaultValue;

        return this;
    }

    byFn(fn) {
        this._setPath('byFn');

        this.checkDuplicateCall();

        if (!fn) {
            throw new BuilderError('Fn must be defined', this._path);
        }

        if (!_.isFunction(fn)) {
            throw new BuilderError('Fn must be a function', this._path);
        }

        this._by = 'fn';
        this._fn = fn;

        return this;
    }

    build() {
        if (!this._by) {
            throw new BuilderError('One of byDays(), byHours(), byMinutes(), bySeconds(), byDateField(), byField(), byFn() must be called.', this._parentPath);
        }

        if (this._by === 'fn') {
            return new Pipeline.FnBasedSplitter(this._fn);
        }

        if (this._by === 'dateField') {
            return new Pipeline.DateFieldBasedSplitter(this._field, this._dateOutputFormat, this._dateFieldFormat, this._defaultValue);
        }

        if (this._by === 'weeks' || this._by === 'days' || this._by === 'hours' || this._by === 'minutes' || this._by === 'seconds') {
            return new Pipeline.DateFieldBoundaryBasedSplitter(this._field,
              this._dateFieldFormat,
              this._by,
              this._num,
              this._datePrefixFormat,
              this._dateSuffixPart,
              this._dateSufixLength,
              this._defaultValue);
        }

        return new Pipeline.FieldBasedSplitter(this._field, this._defaultValue);
    }

}

export default function (name) {
    return new RootPipelineBuilder(name);
}