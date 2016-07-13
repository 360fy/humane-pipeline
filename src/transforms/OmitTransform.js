import _ from 'lodash';

export default class OmitTransform extends require('stream').Transform {
    constructor(key, params) {
        super({readableObjectMode: true, writableObjectMode: true});

        // props can be String or Array of String
        this.props = params.props;
    }

    _transform(chunk, encoding, done) {
        if (_.isArray(chunk)) {
            _.forEach(chunk, chunkPart => {
                this.push(_.omit(chunkPart, this.props));
            });
        } else if (_.isObject(chunk)) {
            this.push(_.omit(chunk, this.props));
        } else if (!_.isUndefined(chunk) && !_.isNull(chunk)) {
            console.warn('OmitTransform: not applying transform - can be applied to only object: ', this.props);
            this.push(chunk);
        }

        done();
    }
}