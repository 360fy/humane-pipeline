import _ from 'lodash';
import {StringDecoder} from 'string_decoder';
import OS from 'os';

export default class CsvTransform extends require('stream').Transform {
    constructor(options) {
        super({readableObjectMode: true});

        this.columns = options.columns;
        this.totalColumnCount = this.columns.length;
        this.rowDelimiter = options.rowDelimiter || OS.EOL;
        this.rowDelimiterLength = this.rowDelimiter.length;

        if (this.rowDelimiterLength > 1) {
            this.rowDelimiter = this.rowDelimiter.split('');
        }

        this.columnDelimiter = options.delimiter || ',';
        this.quote = _.isUndefined(options.quote) ? '"' : options.quote;
        this.quoteEscape = _.isUndefined(options.escape) ? '"' : options.escape;

        if (this.quote === null) {
            this.quoteEscape = null;
        }

        this.decoder = new StringDecoder(options && options.encoding || 'utf8');

        this.currentColumnNum = 1;
        this.currentRowBuffer = [];
        this.currentColumnBuffer = null;

        this.lastChar = null;
        this.inQuotedBlock = false;
    }

    isRowDelimiter() {
        if (this.rowDelimiterLength === 1) {
            return _.last(this.currentColumnBuffer) === this.rowDelimiter;
        }

        return _.isEqual(_.takeRight(this.currentColumnBuffer, this.rowDelimiterLength), this.rowDelimiter);
    }

    _transform(chunk, encoding, done) {
        const str = this.decoder.write(chunk);

        for (let i = 0, len = str.length; i < len; i++) {
            const char = str[i];

            // push character in current column
            if (this.currentColumnBuffer === null) {
                this.currentColumnBuffer = [char];
            } else {
                this.currentColumnBuffer.push(char);
            }

            if (this.lastChar && this.lastChar === this.quoteEscape && this.quote && char === this.quote) {
                if (this.quote && this.lastChar === this.quote) {
                    this.inQuotedBlock = false;
                }

                // pop 2 - one escape and the current char, and then push quote char
                this.currentColumnBuffer.pop();
                this.currentColumnBuffer.pop();
                this.currentColumnBuffer.push(char);
            } else if (this.quote && char === this.quote) {
                // do not consider quote char
                this.currentColumnBuffer.pop();
                this.inQuotedBlock = !this.inQuotedBlock;
            } else if (!this.inQuotedBlock && char === this.columnDelimiter) {
                // do not consider column delimiter
                this.currentColumnBuffer.pop();
                this.currentRowBuffer.push(this.currentColumnBuffer.join('')); // convert into array
                this.currentColumnNum++;
                this.currentColumnBuffer = null;
            } else if (!this.inQuotedBlock && this.currentColumnNum === this.totalColumnCount && this.isRowDelimiter()) {
                for (let j = 0; j < this.rowDelimiterLength; j++) {
                    this.currentColumnBuffer.pop();
                }

                // do not consider row delimiters
                this.currentRowBuffer.push(this.currentColumnBuffer.join(''));
                this.currentColumnBuffer = null;
                this.push(_.zipObject(this.columns, this.currentRowBuffer));
                this.currentRowBuffer = [];
                this.currentColumnNum = 1;
                this.currentColumnBuffer = null;
            }

            this.lastChar = char;
        }

        done();
    }
}