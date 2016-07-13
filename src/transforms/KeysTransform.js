import _ from 'lodash';

export default class KeysTransform extends require('stream').Transform {
    constructor(key, params) {
        super({readableObjectMode: true, writableObjectMode: true});

        this.iteratee = params.iteratee;
    }

    _transform(chunk, encoding, done) {
        if (_.isArray(chunk)) {
            _(chunk).forEach(chunkPart => {
                if (_.isObject(chunkPart)) {
                    _(chunkPart).keys().forEach(value => {
                        this.push(value);
                    });
                }
            }).value();
        } else if (_.isObject(chunk)) {
            _(chunk).keys().forEach(value => this.push(value));
            this.push(_.values(chunk));
        } else if (!_.isUndefined(chunk) && !_.isNull(chunk)) {
            console.warn('KeysTransform: not applying transform - can be applied to only object or array: ', this.iteratee);
            this.push(chunk);
        }

        done();
    }
}