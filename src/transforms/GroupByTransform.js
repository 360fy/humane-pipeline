import _ from 'lodash';

export default class GroupByTransform extends require('stream').Transform {
    constructor(key, params) {
        super({readableObjectMode: true, writableObjectMode: true});

        // validate iteratee

        this.iteratee = params.iteratee;
        this._accumulated = [];
    }

    _transform(chunk, encoding, done) {
        if (_.isObject(chunk) || _.isArray(chunk)) {
            this._accumulated.push(chunk);
        } else if (!_.isUndefined(chunk) && !_.isNull(chunk)) {
            console.warn('GroupByTransform: not applying transform - can be applied to only object or array: ', this.iteratee);
            // this.push(chunk);
        }

        if (this._batcher && this._batcher.shouldRoll(chunk) && this._accumulated.length > 0) {
            this.push(_.groupBy(this._accumulated, this.iteratee));
            this._accumulated = [];
        }

        done();
    }

    _flush(done) {
        if (this._accumulated.length > 0) {
            this.push(_.groupBy(this._accumulated, this.iteratee));
        }
        
        done();
    }
}