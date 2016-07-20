import _ from 'lodash';
import Promise from 'bluebird';
import FS from 'fs';
import Zlib from 'zlib';
import FileStorage from 'lowdb/lib/file-sync';
import Chokidar from 'chokidar';
import OS from 'os';
import Path from 'path';
import performanceNow from 'performance-now';
import lowDB from 'lowdb';
import md5 from 'md5';
import ValidationError from 'humane-node-commons/lib/ValidationError';
import PipelineProcessor from '../pipeline/PipelineProcessor';
import {ArgBuilder} from '../pipeline/PipelineBuilder';

const PROCESS_NEXT_EVENT = 'PROCESS_NEXT';

export const defaultArgs = () => ({
    input: new ArgBuilder('input')
      .required()
      .short('i')
      .description('Watch Mode: file glob pattern or directory | Normal Mode: file path')
      .build(),
    watch: new ArgBuilder('watch')
      .boolean()
      .short('w')
      .description('Enables watch mode')
      .build(),
    mode: new ArgBuilder('mode')
      .validValues('gzip', 'zip')
      .short('m')
      .description('Defines file mode: gzip or zip')
      .build()
});

export const name = 'file';

// support on file complete
class FileInputProcessor extends PipelineProcessor {
    // constructor(pipeline, params) {
    //     super(pipeline, params);
    // }

    _inputStream(params) {
        const stream = FS.createReadStream(params.input, {flags: 'r'});

        if (params.mode) {
            if (params.mode === 'gzip') {
                return stream.pipe(Zlib.createGunzip({flush: 1, end: false, chunkSize: 1024 * 1024}));
            } else if (params.mode === 'zip') {
                return stream.pipe(Zlib.createUnzip({flush: 1, end: false, chunkSize: 1024 * 1024}));
            }
        }

        return stream;
    }

    _processFile(params) {
        const startTime = performanceNow();

        console.log('Started processing: ', params.input);

        return Promise.resolve(this.runPipeline(this._inputStream(params)))
          .then(() => {
              console.log(`Completed processing '${params.input}' in: ${(performanceNow() - startTime).toFixed(3)}ms`);

              if (params._watch) {
                  this.running = false;
                  this.eventEmitter.emit(PROCESS_NEXT_EVENT);
              }

              return true;
          });
    }

    _watchFiles(params) {
        const db = lowDB(params.db || Path.join(OS.homedir(), '.watcher.db'), {storage: FileStorage});

        const watcher = Chokidar.watch(params.inputFilePattern || '.', {
            persistent: true, // watch files in daemon mode
            cwd: params.cwd || '.',
            depth: params.depth || 1, // depth
            awaitWriteFinish: {
                stabilityThreshold: 5000,
                pollInterval: 1000
            }
        });

        watcher.on('add', (path, stats) => {
            // check file does not exist in db
            // if exists, skip it
            const id = md5(path);
            if (!db.get('files').find({id}).value()) {
                console.log(`>>> Watcher: picked file: ${path} <<<`);

                // else, add the file and call the callback
                db('files').push({id, path, time: Date.now()}).value();

                // TODO: if for same file entry exist then do not add...

                this.watchQueue.push({file: path, watch: true});
                this.eventEmitter.emit(PROCESS_NEXT_EVENT);
            }
        });

        // TODO: in tail mode, also add watcher for changes

        console.log(`>>> Watching files for: ${params.inputFilePattern || '.'} <<<`);
    }

    // TODO: need to see how this watch queue will work in case of constantly changing files
    _watch(params) {
        this.watchQueue = [];

        this.running = false;

        this.eventEmitter.addListener(PROCESS_NEXT_EVENT, () => this._processWatchQueue());

        this._watchFiles({inputFilePattern: params.input});

        return true;
    }

    _processWatchQueue() {
        if (this.running) return;

        this.running = true;

        const params = this.watchQueue.shift();
        if (params) {
            this._processFile(params);
        } else {
            this.running = false;
        }
    }

    // TODO: support tail mode
    // TODO: support variadic arguments
    run() {
        const params = this.resolveSettings(this.settings(), this.args());

        if (!params || !params.input) {
            throw new ValidationError('Must pass input path!');
        }

        if (params.watch) {
            return this._watch(params);
        }

        const that = this;

        return new Promise((resolve, reject) =>
          FS.access(params.input, FS.R_OK, (error) => {
              if (error) {
                  console.error(`ERROR: Input file ${params.input} not found!`);
                  return;
              }

              Promise.resolve(that._processFile(params))
                .then(resolve)
                .catch(reject);
          })
        );
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
        inputProcessor: (rootPipeline, params, args) => new FileInputProcessor(rootPipeline, params, args)
    };
}