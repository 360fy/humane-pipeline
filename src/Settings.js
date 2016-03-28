// inputs
import fileInput from './input/FileInput';

// formats
import jsonArrayFormat from './format/JsonArrayFormat';
import jsonFormat from './format/JsonFormat';
import logFormat from './format/LogFormat';
import csvFormat from './format/CsvFormat';

// mappers
import functionMapper from './mapper/FunctionMapper';

// outputs
import indexerOutput from './output/IndexerOutput';
import csvOutput from './output/CsvOutput';

export default {
    source: {
        file: fileInput
    },
    format: {
        log: logFormat,
        json: jsonFormat,
        'json-array': jsonArrayFormat,
        csv: csvFormat,
        tsv: csvFormat
    },
    mapper: {
        fn: functionMapper
    },
    output: {
        indexer: indexerOutput,
        csv: csvOutput
    }
};