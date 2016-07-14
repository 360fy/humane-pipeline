import _ from 'lodash';
import moment from 'moment';
import ValidationError from 'humane-node-commons/lib/ValidationError';
import PipelineTypes from './PipelineTypes';

class BasePipeline {
    
    constructor(key, parent, type, settings, defaultArgs) {
        this._key = key;
        this._type = type;
        this._parent = parent;

        this._name = null;
        this._terminates = false;
        this._settings = settings;
        
        this._defaultArgs = defaultArgs;
    }
    
    key() {
        return this._key;
    }

    parent() {
        return this._parent;
    }

    type() {
        return this._type;
    }

    name() {
        return this._name;
    }

    terminates() {
        return this._terminates;
    }

    settings(key) {
        if (key) {
            return _.get(this._settings, key);
        }
        
        return this._settings;
    }
    
    defaultArgs() {
        return this._defaultArgs;
    }
    
}

export class OutputPipeline extends BasePipeline {
    
    constructor(key, parent, name, outputProcessor, settings, defaultArgs) {
        super(key, parent, PipelineTypes.output, settings, defaultArgs);
        this._name = name;
        this._terminates = true;
        this._outputProcessor = outputProcessor;
    }

    outputProcessor() {
        return this._outputProcessor;
    }
    
}

export class InputPipeline extends BasePipeline {
    
    constructor(key, parent, name, inputProcessor, settings, defaultArgs) {
        super(key, parent, PipelineTypes.input, settings, defaultArgs);
        
        this._name = name;
        this._inputProcessor = inputProcessor;
    }

    inputProcessor() {
        return this._inputProcessor;
    }
    
}

export class TransformPipeline extends BasePipeline {
    
    constructor(key, parent, name, transformProcessor, settings) {
        super(key, parent, PipelineTypes.transform, settings);

        this._name = name;
        this._transformProcessor = transformProcessor;
    }

    transformProcessor() {
        return this._transformProcessor;
    }
    
}

export class Pipeline extends BasePipeline {

    pipelines() {
        return this._pipelines;
    }
    
}

export class ChildPipeline extends Pipeline {
    
    constructor(key, parent, settings) {
        super(key, parent, PipelineTypes.child, settings);
    }
    
}

export class RootPipeline extends Pipeline {
    
    constructor(key, settings, args) {
        super(key, null, PipelineTypes.root, settings);

        this._args = args;
    }

    inputPipeline() {
        const pipeline = _.head(this.pipelines());

        if (!pipeline || !(pipeline instanceof InputPipeline)) {
            throw new ValidationError(`First pipeline for ${this.key()} must be InputPipeline`);
        }
        
        return pipeline;
    }

    args() {
        return this._args;
    }
    
}

export class ExtDataMapPipeline extends Pipeline {

    constructor(key, settings) {
        super(key, null, PipelineTypes.extDataMap, settings);
    }

    inputPipeline() {
        const pipeline = _.head(this.pipelines());

        if (!pipeline || !(pipeline instanceof InputPipeline)) {
            throw new ValidationError(`First pipeline for ${this.key()} must be InputPipeline`);
        }

        return pipeline;
    }
    
}

export class Batcher {
}

export class RecordsBasedBatcher extends Batcher {

    constructor(num) {
        super();

        this._batchId = 0;
        this._num = num;
        this._currentCount = 0;
    }

    shouldRoll(chunk) {
        this._currentCount = (this._currentCount + 1) % this._num;

        if (this._currentCount === 0) {
            this._batchId++;
            return true;
        }

        return false;
    }

    batchId() {
        return this._batchId;
    }

}

// specific time boundaries
export class TimeBasedBatcher extends Batcher {

    constructor(by, num, datePrefixFormat, dateSuffixPart, dateSuffixLength) {
        super();

        this._by = by;
        this._num = num;
        this._datePrefixFormat = datePrefixFormat;
        this._dateSuffixPart = dateSuffixPart;
        this._dateSuffixLength = dateSuffixLength;
        this._batchId = null;
    }

    shouldRoll(chunk) {
        const currentTime = moment();

        const dateSuffixPart = currentTime[this._dateSuffixPart]();

        const batchId = `${currentTime.format(this._datePrefixFormat)}${_.padStart(_.toString(dateSuffixPart - (dateSuffixPart % this._num)), this._dateSuffixLength, '0')}`;

        if (this._batchId !== null && this._batchId !== batchId) {
            this._batchId = batchId;
            return true;
        }

        return false;
    }

    batchId() {
        return this._batchId;
    }

}

export class Splitter {

}

export class FnBasedSplitter extends Splitter {

    constructor(fn) {
        super();

        this._fn = fn;
    }

    splitId(chunk) {
        return this._fn(chunk);
    }

}

export class FieldBasedSplitter extends Splitter {

    constructor(field, defaultValue) {
        super();

        this._field = field;
        this._defaultValue = defaultValue;
    }

    splitId(chunk) {
        return _.get(chunk, this._field, this._defaultValue);
    }

}

export class DateFieldBasedSplitter extends Splitter {

    constructor(field, dateOutputFormat, dateFieldFormat, defaultValue) {
        super();

        this._field = field;
        this._dateOutputFormat = dateOutputFormat;
        this._dateFieldFormat = dateFieldFormat;
        this._defaultValue = defaultValue;
    }

    splitId(chunk) {
        if (_.has(chunk, this._field)) {
            const value = _.get(chunk, this._field);
            const timeAsMoment = moment(value, this._dateFieldFormat);

            return timeAsMoment.format(this._dateOutputFormat);
        }

        return this._defaultValue;
    }

}

export class DateFieldBoundaryBasedSplitter extends Splitter {

    constructor(field, dateFieldFormat, by, num, datePrefixFormat, dateSuffixPart, dateSuffixLength, defaultValue) {
        super();

        this._by = by;
        this._field = field;
        this._num = num;
        this._dateFieldFormat = dateFieldFormat;
        this._datePrefixFormat = datePrefixFormat;
        this._dateSuffixPart = dateSuffixPart;
        this._dateSuffixLength = dateSuffixLength;
        this._defaultValue = defaultValue;
    }

    splitId(chunk) {
        let time = null;
        if (_.has(chunk, this._field)) {
            time = moment(_.get(chunk, this._field), this._dateFieldFormat);
        } else if (this._defaultValue) {
            time = moment(this._defaultValue, this._dateFieldFormat);
        } else {
            time = moment();
        }

        const dateSuffixPart = time[this._dateSuffixPart]();

        return `${time.format(this._datePrefixFormat)}${_.padStart(_.toString(dateSuffixPart - (dateSuffixPart % this._num)), this._dateSuffixLength, '0')}`;
    }

}

export class PipelineArg {
    constructor(name, required, defaultValue, type, shortName, description, validValues) {
        this._name = name;
        this._required = required;
        this._defaultValue = defaultValue;
        this._type = type;
        this._shortName = shortName;
        this._description = description;
        this._validValues = validValues;
    }
    
    name() {
        return this._name;
    }
    
    required() {
        return this._required;
    }

    defaultValue() {
        return this._defaultValue;
    }

    type() {
        return this._type;
    }
    
    shortName() {
        return this._shortName;
    }
    
    description() {
        return this._description;
    }
    
    validValues() {
        return this._validValues;
    }
}

export class PipelineEnvArg {
    constructor(name, required, defaultValue) {
        this._name = name;
        this._required = required;
        this._defaultValue = defaultValue;
    }

    name() {
        return this._name;
    }

    required() {
        return this._required;
    }

    defaultValue() {
        return this._defaultValue;
    }
}