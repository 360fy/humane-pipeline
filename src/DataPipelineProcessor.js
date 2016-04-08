import _ from 'lodash';
import Promise from 'bluebird';
import {EventEmitter} from 'events';
import performanceNow from 'performance-now';

import watchFile from './FileWatcher';
import * as GuardedPromise from './GuardedPromise';

import Settings from './Settings';
import buildLookup from './LookupBuilder';

const PROCESS_NEXT_EVENT = 'PROCESS_NEXT';
const OUTPUT_FINISH = 'OUTPUT_FINISH';

export default class DataPipelineProcessor {
    constructor(config) {
        // TODO: validate config schema with Joi
        this.config = config;
        this.watchQueue = [];
        this.running = false;
        this.eventEmitter = new EventEmitter();
        this.lookups = {};
    }

    watch(params) {
        this.eventEmitter.addListener(PROCESS_NEXT_EVENT, () => this.processWatchQueue());

        watchFile({
            inputFilePattern: params.inputFilePattern,
            process: path => {
                this.watchQueue.push({indexer: params.indexer, inputFile: path, watch: true});
                this.eventEmitter.emit(PROCESS_NEXT_EVENT);
            }
        });
    }

    processWatchQueue() {
        if (this.running) return;

        this.running = true;

        const params = this.watchQueue.shift();
        if (params) {
            this.process(params);
        } else {
            this.running = false;
        }
    }

    process(params) {
        const lookupConfigs = this.config.lookups || [];

        const lookupPromises = _.mapValues(lookupConfigs, (lookupConfig, lookupKey) => this.lookups[lookupKey] || buildLookup(lookupKey, lookupConfig));

        return Promise.props(lookupPromises)
          .then(lookups => {
              this.lookups = lookups;

              const sourceHandler = Settings.source[this.config.input.source.type];
              const formatHandler = Settings.format[this.config.input.format.type];

              const startTime = performanceNow();

              console.log('Started processing: ', params.inputFile);

              let stream = sourceHandler(_.defaultsDeep({}, params, this.config.input.source));

              stream = formatHandler(stream, _.defaultsDeep({}, params, this.config.input.format));

              if (this.config.mapper) {
                  const mapperHandler = Settings.mapper[this.config.mapper.type];

                  stream = mapperHandler(stream, _.defaultsDeep({lookups}, params, this.config.mapper));
              }

              const outputHandler = Settings.output[this.config.output.type];

              stream = outputHandler(stream, _.defaultsDeep({lookups, eventEmitter: this.eventEmitter}, params, this.config.output));
              
              const _this = this;

              return new Promise(resolve => {
                  this.eventEmitter.on(OUTPUT_FINISH, () => {
                      console.log(`Completed processing '${params.inputFile}' in: ${(performanceNow() - startTime).toFixed(3)}ms`);
                      
                      if (params.watch) {
                          _this.running = false;
                          _this.eventEmitter.emit(PROCESS_NEXT_EVENT);
                      }

                      resolve(true);
                  });
              });
          });
    }
}