import _ from 'lodash';
import FS from 'fs';
import Promise from 'bluebird';
import {FileInputProcessor} from './File';

import ValidationError from 'humane-node-commons/lib/ValidationError';
import {ArgBuilder} from '../pipeline/PipelineBuilder';

const FsPromise = Promise.promisifyAll(FS);

export const defaultArgs = () => ({
    directory: new ArgBuilder('directory')
      .required()
      .description('Directory path')
      .build(),
    watch: new ArgBuilder('watch')
      .boolean()
      .description('Enables watch mode for the directory')
      .build(),
    mode: new ArgBuilder('mode')
      .validValues('gzip', 'zip')
      .description('Defines file mode: gzip or zip')
      .build()
});

export const name = 'directory';

class DirectoryInputProcessor extends FileInputProcessor {

    run() {
        if (!this.params() || !this.params().directory) {
            throw new ValidationError('Must pass file(s) directory!');
        }

        const that = this;

        return new Promise((resolve, reject) => {
            FsPromise.accessAsync(that.params().directory, FS.R_OK)
              .then(() => FsPromise.stat(that.params().directory))
              .then(stats => {
                  if (!stats.isDirectory()) {
                      throw new Error(`${that.params().directory} is not a directory`);
                  }

                  return FsPromise.readDir(that.params().directory);
              })
              .mapSeries((item, index, length) => that._processFile({file: item}))
              .then(resolve)
              .catch(reject);
        });
    }
}

export function builder(buildKey, fileNameOrSettings) {
    let settings = null;

    if (fileNameOrSettings) {
        if (_.isString(fileNameOrSettings)) {
            settings = {directory: fileNameOrSettings};
        } else if (_.isObject(fileNameOrSettings)) {
            settings = fileNameOrSettings;
        }
    }

    return {
        settings,
        directoryProcessor: (rootPipeline, params, args) => new DirectoryInputProcessor(rootPipeline, params, args)
    };
}