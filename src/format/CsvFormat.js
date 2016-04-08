import _ from 'lodash';
import CsvTransform from '../transforms/CsvTransform';

export default function (stream, params) {
    if (_.isUndefined(params.columns) || !params.columns || _.isEmpty(params.columns)) {
        throw new Error('Columns array must be defined');
    }
    
    return stream.pipe(new CsvTransform({
        delimiter: params.delimiter,
        rowDelimiter: params.rowDelimiter,
        quote: params.quote,
        escape: params.escape,
        columns: params.columns,
        trim: true
    }));
}