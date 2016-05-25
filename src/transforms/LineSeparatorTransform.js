import _ from 'lodash';
import {StringDecoder} from 'string_decoder';

export default class LineSeparatorTransform extends require('stream').Transform {
    constructor(options) {
        super({readableObjectMode: true});
        this.buffer = '';
        this.separator = (options && options.separator) || /\r\n|\r|\n/g;
        this.flushTail = (options && options.flushTail || true);
        this.decoder = new StringDecoder(options && options.encoding || 'utf8');
        this.jsonParse = options && options.jsonParse || false;
    }

    enqueue(value) {
        value = _.trim(value);

        if (value) {
            if (this.jsonParse) {
                try {
                    value = JSON.parse(value);
                } catch (er) {
                    this.emit('error', er);
                    return;
                }
            }

            this.push(value);
        }
    }

    _transform(chunk, encoding, done) {
        const str = this.buffer + this.decoder.write(chunk);
        const list = str.split(this.separator);

        let remaining = null;
        if (list.length >= 1) {
            remaining = list.pop();
        } else {
            // we could not split it...
            remaining = str;
        }

        for (let i = 0; i < list.length; i++) {
            this.enqueue(list[i]);
        }

        this.buffer = remaining || '';

        done();
    }

    _flush(done) {
        if (this.flushTail) {
            this.enqueue(this.buffer);
        }

        done();
    }
}