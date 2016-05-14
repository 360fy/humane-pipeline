import _ from 'lodash';

/* eslint-disable global-require */
export default class ArrayTransform extends require('stream').Transform {
    constructor() {
        super({objectMode: true});
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