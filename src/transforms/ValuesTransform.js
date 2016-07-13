import _ from 'lodash';

export default class ValuesTransform extends require('stream').Transform {
    constructor(key) {
        super({readableObjectMode: true, writableObjectMode: true});
    }

    _transform(chunk, encoding, done) {
        if (_.isArray(chunk)) {
            // console.log('Values: array chunk: ', chunk);
            _(chunk).forEach(chunkPart => {
                if (_.isObject(chunkPart)) {
                    _(chunkPart).values().forEach(value => {
                        this.push(value);
                    });
                }
            }).value();
        } else if (_.isObject(chunk)) {
            // console.log('Values: object chunk: ', chunk);
            _(chunk).values().forEach(value => {
                this.push(value);
            });
        } else if (!_.isUndefined(chunk) && !_.isNull(chunk)) {
            console.warn('ValuesTransform: not applying transform - can be applied to only object or array');
            this.push(chunk);
        }

        done();
    }
}