import _ from 'lodash';
import Promise from 'bluebird';
import {EventEmitter} from 'events';
import performanceNow from 'performance-now';

import watchFile from './FileWatcher';
import * as GuardedPromise from './GuardedPromise';

import Settings from './Settings';
import buildLookup from './LookupBuilder';

const PROCESS_NEXT_EVENT = 'processNext';

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
            filePattern: params.filePattern,
            process: path => {
                this.watchQueue.push({indexer: params.indexer, file: path, watch: true});
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

              console.log('Started processing: ', params.file);

              let stream = sourceHandler(_.extend({}, this.config.input.source, params));

              stream = formatHandler(stream, _.extend({}, this.config.input.format, params));

              if (this.config.mapper) {
                  const mapperHandler = Settings.mapper[this.config.mapper.type];

                  stream = mapperHandler(stream, _.extend({}, this.config.mapper, params, {lookups}));
              }

              let queuedCount = 0;
              let processedCount = 0;

              const outputHandler = new (Settings.output[this.config.output.type])(_.extend({}, this.config.output, params));

              stream.on('data', GuardedPromise.guard(this.config.output.concurrency || 1, (data) => {
                  const numIndex = queuedCount++;

                  const startTime = performanceNow();

                  return Promise.resolve(outputHandler.handle(data, this.config.filter))
                    .then((result) => {
                        console.log(`Processed #${numIndex}: ${JSON.stringify(result)} in ${(performanceNow() - startTime).toFixed(3)} ms`);

                        return result;
                    })
                    .catch(error => console.error('>>>> Error: ', error, error.stack))
                    .finally(() => processedCount++);
              }));

              const _this = this;

              return new Promise(resolve => {
                  stream.on('end', () => {
                      function shutdownIndexerIfProcessed() {
                          if (queuedCount === processedCount) {
                              if (!params.watch) {
                                  resolve(outputHandler.shutdown());
                              } else {
                                  console.log('Completed processing: ', params.file);
                                  _this.running = false;
                                  _this.eventEmitter.emit(PROCESS_NEXT_EVENT);
                                  resolve(true);
                              }
                          } else {
                              // schedule next one
                              _.delay(shutdownIndexerIfProcessed, 5000);
                          }
                      }

                      shutdownIndexerIfProcessed();
                  });
              });
          });
    }
}