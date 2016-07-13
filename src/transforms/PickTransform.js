import _ from 'lodash';

export default class PickTransform extends require('stream').Transform {
    constructor(key, params) {
        super({readableObjectMode: true, writableObjectMode: true});

        this.props = params.props;
    }

    _transform(chunk, encoding, done) {
        if (_.isArray(chunk)) {
            _.forEach(chunk, chunkPart => {
                this.push(_.pick(chunkPart, this.props));
            });
        } else if (_.isObject(chunk)) {
            this.push(_.pick(chunk, this.props));
        } else if (!_.isUndefined(chunk) && !_.isNull(chunk)) {
            console.warn('PickTransform: not applying transform - can be applied to only object: ', this.props);
            this.push(chunk);
        }

        done();
    }
}