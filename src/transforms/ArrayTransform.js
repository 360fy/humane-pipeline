import _ from 'lodash';

export default class ArrayTransform extends require('stream').Transform {
    constructor(key) {
        super({readableObjectMode: true, writableObjectMode: true});
    }

    _transform(chunk, encoding, done) {
        if (_.isArray(chunk)) {
            _.forEach(chunk, item => this.push(item));
        } else {
            this.push(chunk);
        }

        done();
    }
}