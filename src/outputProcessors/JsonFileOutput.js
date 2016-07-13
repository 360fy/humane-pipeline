import _ from 'lodash';
import FS from 'fs';
import SplitWriteTransform from './../transforms/SplitWriteTransform';
import JsonToStringTransform from '../transforms/JsonToStringTransform';
import * as FileOutput from './FileOutput';

export const name = 'jsonFile';

export const defaultArgs = FileOutput.defaultArgs;

export const supportsSplit = () => true;

// TODO: support gzip
export function builder(buildKey, fileNameOrSettings) {
    let settings = null;
    if (_.isString(fileNameOrSettings)) {
        settings = {output: fileNameOrSettings};
    } else if (_.isObject(fileNameOrSettings)) {
        settings = fileNameOrSettings;
    }

    return {
        settings,
        outputProcessor: (key, stream, params) => {
            if (params._splitter) {
                const splitTransform = new SplitWriteTransform(key,
                  true,
                  params.encoding || 'utf8',
                  params._splitter,
                  (splitId) => {
                      const fileName = _.template(FileOutput.attachSplitId(params.output, splitId))(FileOutput.fileNameTemplateContext(splitId));
                      
                      const inputStream = new JsonToStringTransform();
                      
                      inputStream.pipe(FS.createWriteStream(fileName, {flags: 'a', autoClose: true}));
                      
                      return inputStream;
                  });

                const finalStream = stream.pipe(splitTransform);

                finalStream.on('finish', () => {
                    params.resolve(true);
                });

                return finalStream;
            }

            return FileOutput.outputProcessor(key, stream.pipe(new JsonToStringTransform()), params);
        }
    };
}