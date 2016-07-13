import _ from 'lodash';

export default class PickByTransform extends require('stream').Transform {
    constructor(key, params) {
        super({readableObjectMode: true, writableObjectMode: true});

        this.predicate = params.predicate;
    }

    _transform(chunk, encoding, done) {
        if (_.isArray(chunk)) {
            _.forEach(chunk, chunkPart => {
                this.push(_.pickBy(chunkPart, this.predicate));
            });
        } else if (_.isObject(chunk)) {
            this.push(_.pickBy(chunk, this.predicate));
        } else if (!_.isUndefined(chunk) && !_.isNull(chunk)) {
            console.warn('PickByTransform: not applying transform - can be applied to only object: ', this.predicate);
            this.push(chunk);
        }

        done();
    }
}