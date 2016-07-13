import _ from 'lodash';

export default class SplitWriteTransform extends require('stream').Writable {
    constructor(key, objectMode, encoding, splitter, writeStreamBuilder) {
        super({objectMode});

        this._encoding = encoding;
        this._splitter = splitter;
        this._writeStreamBuilder = writeStreamBuilder;
        this._writeStreams = {

        };
    }

    _write(chunk, encoding, done) {
        const splitName = this._splitter.splitId(chunk);
        let writeStream = _.get(this._writeStreams, splitName);
        if (!writeStream) {
            writeStream = this._writeStreamBuilder(splitName);
            _.set(this._writeStreams, splitName, writeStream);
        }

        writeStream.write(chunk, this._encoding, done);

        return true;
    }

}