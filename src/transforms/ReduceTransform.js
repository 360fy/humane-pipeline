import _ from 'lodash';

export default class ReduceTransform extends require('stream').Transform {
    constructor(key, params) {
        super({readableObjectMode: true, writableObjectMode: true});

        this.iteratee = params.iteratee;
        this.originalAccumulator = params.accumulator;
        this.accumulator = this._setAccumulator();
        this._batcher = params._batcher;
    }

    _transform(chunk, encoding, done) {
        if (!_.isUndefined(chunk) && !_.isNull(chunk)) {
            this.iteratee(this.accumulator, chunk);
        }

        if (this._batcher && this._batcher.shouldRoll(chunk)) {
            this.push(this.accumulator);
            this._setAccumulator();
        }

        done();
    }
    
    _setAccumulator() {
        this.accumulator = this.originalAccumulator && _.cloneDeep(this.originalAccumulator);
        return this.accumulator;
    }

    _flush(done) {
        this.push(this.accumulator);
        done();
    }
}