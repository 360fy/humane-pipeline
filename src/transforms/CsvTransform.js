import _ from 'lodash';
import {StringDecoder} from 'string_decoder';
import OS from 'os';

/* eslint-disable global-require */
export default class CsvTransform extends require('stream').Transform {
    constructor(options) {
        super({readableObjectMode: true});

        this.header = options.header;
        this.columns = options.columns;
        this.totalColumnCount = this.columns && this.columns.length;
        this.rowDelimiter = options.rowDelimiter || OS.EOL;
        this.rowDelimiterLength = this.rowDelimiter.length;

        if (this.rowDelimiterLength > 1) {
            this.rowDelimiter = Buffer.from(this.rowDelimiter, 'utf8');
        } else {
            this.rowDelimiter = this.rowDelimiter.charCodeAt(0);
        }

        this.columnDelimiter = options.delimiter || ',';

        this.columnDelimiterLength = this.columnDelimiter.length;

        if (this.columnDelimiterLength > 1) {
            this.columnDelimiter = Buffer.from(this.columnDelimiter, 'utf8');
        } else {
            this.columnDelimiter = this.columnDelimiter.charCodeAt(0);
        }

        // todo: for now quote and quoteEscape chars shall be same
        // this.quoteEscape = _lodash2.default.isUndefined(options.escape) ? '"' : options.escape;
        // if (this.quote === null) {
        //     this.quoteEscape = null;
        // }

        this.quote = _.isUndefined(options.quote) ? '"' : options.quote;

        if (this.quote) {
            this.quote = this.quote.charCodeAt(0);
            this.quoteEscape = this.quote; //this.quoteEscape.charCodeAt(0);
        }

        // if (this.quoteEscape) {
        //     this.quoteEscape = this.quoteEscape.charCodeAt(0);
        // }

        this.decoder = new StringDecoder(options && options.encoding || 'utf8');

        this.currentColumnNum = 1;
        this.currentRowBuffer = [];
        this.currentBufferAllocatedLength = 131072;
        this.currentBuffer = Buffer.alloc(this.currentBufferAllocatedLength);
        this.currentBufferLength = 0;

        this.lastChar = null;
        
        this.quoteBufferLength = 0;
        this.inQuotedBlock = false;

        this.headerRowSeen = false;

        if (!this.header && (!this.columns || _.isEmpty(this.columns))) {
            throw new Error('Either columns array should be provided or header should be true!');
        }
    }

    isSameDelimiter(delimiterLength, delimiter) {
        if (delimiterLength === 1) {
            if (this.currentBufferLength <= 0) {
                return false;
            }

            return this.currentBuffer[this.currentBufferLength - 1] === delimiter;
        }

        if (this.currentBufferLength < delimiterLength) {
            return false;
        }

        let same = true;
        for (let j = this.currentBufferLength - delimiterLength, k = 0; j < this.currentBufferLength; j++, k++) {
            if (this.currentBuffer[j] !== delimiter[k]) {
                same = false;
                break;
            }
        }

        return same;
    }

    isColumnDelimiter() {
        return this.isSameDelimiter(this.columnDelimiterLength, this.columnDelimiter);
    }

    isRowDelimiter() {
        return this.isSameDelimiter(this.rowDelimiterLength, this.rowDelimiter);
    }

    _transform(chunk, encoding, done) {
        const chunkLength = chunk.length;

        if (chunkLength + this.currentBufferLength > this.currentBufferAllocatedLength) {
            const oldBuffer = this.currentBuffer;

            while (chunkLength + this.currentBufferLength > this.currentBufferAllocatedLength) {
                this.currentBufferAllocatedLength *= 2;
            }

            const newBuffer = Buffer.alloc(this.currentBufferAllocatedLength);
            oldBuffer.copy(newBuffer, 0, this.currentBufferLength);

            this.currentBuffer = newBuffer;
        }

        for (let i = 0; i < chunkLength; i++) {
            const char = chunk[i];

            // this scenario handles when quote and quoteEscape are same
            if (char === this.quote) {
                // keep collecting
                this.quoteBufferLength++;
                continue;
            } else if (this.quoteBufferLength > 0) {
                // we have some sequence of quotes
                if (this.quoteBufferLength % 2 === 1) {
                    // we switch the quoted block
                    this.inQuotedBlock = !this.inQuotedBlock;
                    this.quoteBufferLength--;
                }

                while (this.quoteBufferLength > 0) {
                    this.currentBuffer[this.currentBufferLength++] = this.quote;
                    this.quoteBufferLength--;
                }
            }

            this.currentBuffer[this.currentBufferLength++] = char;

            if (!this.inQuotedBlock && this.isColumnDelimiter()) {
                this.currentBufferLength -= this.columnDelimiterLength;

                this.currentRowBuffer.push(this.decoder.write(this.currentBuffer.slice(0, this.currentBufferLength))); // convert into array
                this.currentColumnNum++;
                this.currentBufferLength = 0;
            } else if (!this.inQuotedBlock && ((this.header && !this.headerRowSeen) || this.currentColumnNum === this.totalColumnCount) && this.isRowDelimiter()) {
                this.currentBufferLength -= this.rowDelimiterLength;

                // do not consider row delimiters
                this.currentRowBuffer.push(this.decoder.write(this.currentBuffer.slice(0, this.currentBufferLength))); // convert into array
                if (this.header && !this.headerRowSeen) {
                    // set these as columns
                    this.columns = this.currentRowBuffer;
                    this.totalColumnCount = this.columns.length;
                    this.headerRowSeen = true;
                } else {
                    this.push(_.zipObject(this.columns, this.currentRowBuffer));
                }

                this.currentRowBuffer = [];
                this.currentColumnNum = 1;
                this.currentBufferLength = 0;
            }

            this.lastChar = char;
        }

        done();
    }
}