import _ from 'lodash';
import FS from 'fs';
import buildCsvStringifier from 'csv-stringify';

export default function (stream, params) {
    const eventEmitter = params.eventEmitter;
    
    const csvStringifier = buildCsvStringifier({
        delimiter: params.delimiter,
        rowDelimiter: params.rowDelimiter,
        header: _.isUndefined(params.header) ? true : params.header
    });
    
    let outputStream = null;
    
    if (params.outputFile) {
        outputStream = FS.createWriteStream(params.outputFile, {flags: 'a', autoClose: true});
    } else {
        outputStream = process.stdout;
    }
    
    const finalStream = stream.pipe(csvStringifier).pipe(outputStream);
    
    finalStream.on('finish', () => {
        eventEmitter.emit('OUTPUT_FINISH');
    });

    return finalStream;
}