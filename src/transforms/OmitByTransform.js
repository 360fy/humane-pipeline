import _ from 'lodash';

export default class OmitByTransform extends require('stream').Transform {
    constructor(key, params) {
        super({readableObjectMode: true, writableObjectMode: true});

        this.predicate = params.predicate;
    }

    _transform(chunk, encoding, done) {
        if (_.isArray(chunk)) {
            _.forEach(chunk, chunkPart => {
                this.push(_.omitBy(chunkPart, this.predicate));
            });
        } else if (_.isObject(chunk)) {
            this.push(_.omitBy(chunk, this.predicate));
        } else if (!_.isUndefined(chunk) && !_.isNull(chunk)) {
            console.warn('OmitByTransform: not applying transform - can be applied to only object: ', this.predicate);
            this.push(chunk);
        }

        done();
    }
}