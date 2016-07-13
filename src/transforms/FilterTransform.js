import _ from 'lodash';

export default class FilterTransform extends require('stream').Transform {
    constructor(key, params) {
        super({readableObjectMode: true, writableObjectMode: true});

        // in lodash iteratee can be Array, Function, Object, String
        // Array is matched by _.matchesProperty shorthand - first index being property path, 2nd being property value
        // String is matched by _.property shorthand
        // Object is matched by _.matches shorthand
        this.iteratee = params.iteratee;
    }

    _transform(chunk, encoding, done) {
        if (_.isArray(chunk)) {
            this.push(_.filter(chunk, this.iteratee));
        } else if (_.isObject(chunk)) {
            if (this.iteratee(chunk)) {
                this.push(chunk);
            }
        } else if (!_.isUndefined(chunk) && !_.isNull(chunk)) {
            console.warn('FilterTransform: not applying transform - can be applied to only object or array: ', this.iteratee);
            this.push(chunk);
        }

        done();
    }
}