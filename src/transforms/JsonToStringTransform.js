import _ from 'lodash';
import OS from 'os';

export default class JsonToStringTransform extends require('stream').Transform {
    constructor(key) {
        super({writableObjectMode: true});
    }

    _transform(chunk, encoding, done) {
        if (_.isObject(chunk)) {
            this.push(`${JSON.stringify(chunk)}${OS.EOL}`);
        } else if (!_.isUndefined(chunk) && !_.isNull(chunk)) {
            this.push(chunk);
        }

        done();
    }
}