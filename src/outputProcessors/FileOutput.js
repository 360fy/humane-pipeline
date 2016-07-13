import _ from 'lodash';
import OS from 'os';
import FS from 'fs';
import moment from 'moment';
import {ArgBuilder} from '../pipeline/PipelineBuilder';
import SplitWriteTransform from './../transforms/SplitWriteTransform';

export const name = 'file';

export const supportsSplit = () => true;

export const defaultArgs = () => ({
    output: new ArgBuilder('output')
      .short('o')
      .required()
      .description('File path for output')
      .build()
});

const SplitIdRegex = /\${[\s]*splitId[\s]*}/;


export function attachSplitId(fileName, splitId) {
    if (!SplitIdRegex.test(fileName)) {
        // attach splitId to fileName
        const parts = _.split(fileName, '.');
        if (_.isArray(parts) && parts.length > 0) {
            const lastPart = _.last(parts);
            if (parts.length > 1 && lastPart.length <= 4) {
                // we attach before
                fileName = `${_.join(_.initial(parts), '.')}.${splitId}.${lastPart}`;
            } else {
                // we attach after
                fileName = `${fileName}.${splitId}`;
            }
        }
    }

    return fileName;
}

export const fileNameTemplateContext = (splitId) => {
    const time = moment();

    return _.defaults({
        host: OS.hostname(),
        time: time.format('YYYYMMDDHHmmss'),
        epoch: time.valueOf(),
        splitId
    }, process.env);
};

// TODO: support gzip
export function outputProcessor(key, stream, params) {
    if (!params || !params.output) {
        throw new Error(`FileOutput: At ${key} output path not defined`);
    }

    let finalStream = null;

    if (params._splitter) {
        const splitTransform = new SplitWriteTransform(key,
          false,
          params.encoding || 'utf8',
          params._splitter,
          (splitId) => {
              const fileName = _.template(attachSplitId(params.output, splitId))(fileNameTemplateContext(splitId));
              return FS.createWriteStream(fileName, {flags: 'a', autoClose: true});
          });
        finalStream = stream.pipe(splitTransform);
    } else {
        const fileName = _.template(params.output)(fileNameTemplateContext());
        finalStream = stream.pipe(FS.createWriteStream(fileName, {flags: 'a', autoClose: true}));
    }

    finalStream.on('finish', () => {
        params.resolve(true);
    });

    return finalStream;
}

export function builder(buildKey, fileNameOrSettings) {
    let settings = null;
    if (fileNameOrSettings) {
        if (_.isString(fileNameOrSettings)) {
            settings = {output: fileNameOrSettings};
        } else if (_.isObject(fileNameOrSettings)) {
            settings = fileNameOrSettings;
        }
    }

    return {
        settings,
        outputProcessor
    };
}