import _ from 'lodash';
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

                  const inputProcessor = inputProcessorBuilder(rootPipeline, inputPipeline.settings(), args);

                  return Promise.resolve(inputProcessor.run())
                    .then(() => {
                        console.log(`Completed processing in: ${(performanceNow() - startTime).toFixed(3)}ms`);
                        return true;
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