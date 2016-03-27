import _ from 'lodash';
import buildCsvStringifier from 'csv-stringify';

export default class CsvOutput {
    constructor(params) {
        this.csvStringifier = buildCsvStringifier({
            delimiter: params.delimiter,
            rowDelimiter: params.rowDelimiter,
            header: _.isUndefined(params.header) ? true : params.header
        });

        this.csvStringifier.on('readable', (data) => {
            console.log(data);
        });

        this.csvStringifier.on('error', (err) => {
            console.error(err.message);
        });

        this.csvStringifier.on('finish', () => {
            console.log('<<< Done >>>');
        });
    }  

    handle(doc) {
        return this.csvStringifier.write(doc);
    }

    shutdown() {
    }
}