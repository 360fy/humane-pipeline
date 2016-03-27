import _ from 'lodash';
import parseCsv from 'csv-parse';


export default function (stream, params) {
    return stream.pipe(parseCsv({
        delimiter: params.delimiter,
        rowDelimiter: params.rowDelimiter,
        quote: params.quote,
        escape: params.escape,
        skip_empty_lines: true,
        columns: _.isUndefined(params.columns) ? true : params.columns,
        trim: true
    }));
}