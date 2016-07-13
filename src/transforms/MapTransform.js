import _ from 'lodash';

export default class MapTransform extends require('stream').Transform {
    constructor(key, params) {
        super({readableObjectMode: true, writableObjectMode: true});

        this.iteratee = params.iteratee;
    }

    _transform(chunk, encoding, done) {
        if (_.isArray(chunk)) {
            _.forEach(chunk, chunkPart => {
                this.push(this.iteratee(chunkPart));
            });
        } else if (_.isObject(chunk)) {
            this.push(this.iteratee(chunk));
        } else if (!_.isUndefined(chunk) && !_.isNull(chunk)) {
            console.warn('MapTransform: not applying transform - can be applied to only object or array: ', this.iteratee);
            this.push(chunk);
        }

        done();
    }
}