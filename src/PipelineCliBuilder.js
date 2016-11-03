import _ from 'lodash';
import FileStorage from 'lowdb/lib/file-sync';
import OS from 'os';
import Path from 'path';
import lowDB from 'lowdb';
import md5 from 'md5';
import Promise from 'bluebird';
import performanceNow from 'performance-now';
import {Command} from 'command-line-boilerplate/lib/CliBuilder';
import pipelineBuilder from './pipeline/PipelineBuilder';
import {RootPipeline} from './pipeline/Pipeline';
import ValidationError from 'humane-node-commons/lib/ValidationError';

// function stringify(obj, indent) {
//     return JSON.stringify(obj, (key, value) => {
//         if (value instanceof Function || typeof value === 'function') {
//             return value.toString();
//         }
//         if (value instanceof RegExp) {
//             return `_PxEgEr_${value}`;
//         }
//
//         return value;
//     }, indent);
// }

function cleanArgs(args) {
    return _(args).omit([
        'commands',
        'options',
        '_execs',
        '_allowUnknownOption',
        '_args',
        '_name',
        '_noHelp',
        'parent',
        '_description',
        '_events',
        '_eventsCount'])
      .omitBy((value) => _.isFunction(value) || _.isUndefined(value) || _.isNull(value))
      .value();
}

// TODO: how to handle running multiple pipelines together ?
export default function (pipelineConfigsOrBuilder) {
    if (_.isFunction(pipelineConfigsOrBuilder)) {
        pipelineConfigsOrBuilder = pipelineConfigsOrBuilder();
    }

    if (!_.isObject(pipelineConfigsOrBuilder)) {
        throw new ValidationError('Pipeline configs must either a function that returns or an object having pipeline key => pipeline builder');
    }

    _(pipelineConfigsOrBuilder)
      .forEach((builderFn, key) => {
          if (!_.isFunction(builderFn)) {
              throw new ValidationError(`For key ${key} value must be a pipeline builder fn`);
          }

          // const name = _.upperFirst(_.camelCase(key));

          const rootPipelineBuilder = pipelineBuilder(key);

          builderFn(rootPipelineBuilder);

          const rootPipeline = rootPipelineBuilder.build();

          if (!rootPipeline || !(rootPipeline instanceof RootPipeline)) {
              throw new ValidationError(`Pipeline for ${key} must be a valid RootPipeline`);
          }

          let command = new Command(`run-${key}`)
            .description(`Runs ${key} pipeline`)
            .action(
              args => {
                  const startTime = performanceNow();

                  const inputPipeline = rootPipeline.inputPipeline();

                  const inputProcessorBuilder = inputPipeline.inputProcessor();
                  if (!_.isFunction(inputProcessorBuilder)) {
                      throw new ValidationError(`InputProcessor for pipeline ${key} must be a builder function`);
                  }

                  const cleanedArgs = cleanArgs(args);

                  const runId = md5(`${key}:${JSON.stringify(cleanedArgs)}`);
                  const db = lowDB(Path.join(OS.homedir(), '.humane.pipeline.run.db'), {storage: FileStorage});

                  let lastRunTime = 0;
                  if (db.has(`RUNS.${runId}.TIME`).value()) {
                      lastRunTime = db.get(`RUNS.${runId}.TIME`).value();
                  }

                  const inputProcessor = inputProcessorBuilder(rootPipeline, inputPipeline.settings(), _.defaults({LAST_RUN_TIME: lastRunTime}, cleanedArgs));

                  const currentRunTime = new Date().getTime();

                  return Promise.resolve(inputProcessor.run())
                    .then(() => {
                        db.set(`RUNS.${runId}.TIME`, currentRunTime).value();
                        console.log(`Completed processing in: ${(performanceNow() - startTime).toFixed(3)}ms`);
                        return true;
                    })
                    .catch((error) => {
                        if (error instanceof ValidationError) {
                            // TODO: color code it
                            console.error(`<<<< ERROR >>>> -- ${error.message}`);
                            return false;
                        }

                        // TODO: color code it
                        console.error('<<<< ERROR >>>>', error);
                        return false;
                    });
              },
              {watch: !!rootPipeline.settings('memorySize'), memorySize: rootPipeline.settings('memorySize'), gcInterval: rootPipeline.settings('gcInterval')}
            );

          _.forEach(rootPipeline.args(), (arg) => {
              let option = '';
              if (arg.shortName()) {
                  option = `-${arg.shortName()}, `;
              }

              option = `${option}--${arg.name()}`;

              if (arg.type() === 'string' || arg.type() === 'enum') {
                  option = `${option} <${arg.name()}>`;
              }

              let description = arg.description();

              if (arg.type() === 'enum') {
                  description = `${description}. One of: ${arg.validValues()}`;
              }

              command = command.option(option, description);
          });
      });
}