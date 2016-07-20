import _ from 'lodash';
import Promise from 'bluebird';
import ValidationError from 'humane-node-commons/lib/ValidationError';
import {ArgBuilder} from '../pipeline/PipelineBuilder';
import glob from 'glob';

import {FileInputProcessor} from './File';

export const defaultArgs = () => ({
    pattern: new ArgBuilder('pattern')
      .required()
      .description('File(s) pattern')
      .build(),
    watch: new ArgBuilder('watch')
      .boolean()
      .description('Enables watch mode for the file(s) pattern')
      .build(),
    mode: new ArgBuilder('mode')
      .validValues('gzip', 'zip')
      .description('Defines file(s) mode: gzip or zip')
      .build()
});

export const name = 'filePattern';

class FilePatternInputProcessor extends FileInputProcessor {

    run() {
        const params = this.resolveSettings(this.settings(), this.args());

        if (!params || !params.pattern) {
            throw new ValidationError('Must pass file(s) pattern!');
        }

        const that = this;

        return new Promise((resolve, reject) => {
            glob(params.pattern, (error, files) => {
                if (error) {
                    // console.error(`ERROR: Input file ${params.input} not found!`);
                    reject(error);
                    return;
                }

                // do sequential processing of files
                Promise
                  .mapSeries(files, (item, index, length) => that._processFile({file: item}))
                  .then(resolve)
                  .catch(reject);
            });
        });
    }
    
}

export function builder(buildKey, fileNameOrSettings) {
    let settings = null;

    if (fileNameOrSettings) {
        if (_.isString(fileNameOrSettings)) {
            settings = {input: fileNameOrSettings};
        } else if (_.isObject(fileNameOrSettings)) {
            settings = fileNameOrSettings;
        }
    }

    return {
        settings,
        inputProcessor: (rootPipeline, params, args) => new FilePatternInputProcessor(rootPipeline, params, args)
    };
}