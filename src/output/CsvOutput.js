import _ from 'lodash';
import OS from 'os';
import FS from 'fs';
import buildCsvStringifier from 'csv-stringify';

export default class CsvOutput {
    constructor(params) {
        this.csvStringifier = buildCsvStringifier({
            delimiter: params.delimiter,
            rowDelimiter: params.rowDelimiter,
            header: _.isUndefined(params.header) ? true : params.header
        });

        if (params.outputFile) {
            this.stream = FS.createWriteStream(params.outputFile, {flags: 'a', autoClose: true});
            this.stdout = false;
        } else {
            this.stream = process.stdout;
            this.stdout = true;
        }

        this.csvStringifier.on('error', (err) => {
            console.error(err.message);
        });

        this.csvStringifier.on('finish', () => {
            console.log('<<< Done >>>');
        });

        this.csvStringifier.pipe(this.stream);
    }  

    handle(doc) {
        this.csvStringifier.write(doc);
        return doc;
    }

    shutdown() {
        this.csvStringifier.end();
        if (!this.stdout) {
            this.stream.end();
        }
        
        return true;
    }
}