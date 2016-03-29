import _ from 'lodash';

import {Command} from 'command-line-boilerplate/lib/CliBuilder';

import DataPipelineProcessor from './DataPipelineProcessor';

export default function (indexerBuilder, dataPipelineConfig) {
    _(dataPipelineConfig)
      .keys()
      .forEach(importKey => {
          const name = _.upperFirst(_.camelCase(importKey));

          const importConfig = dataPipelineConfig[importKey];

          const sourceType = importConfig.input.source.type;
          const sourceFormat = importConfig.input.format.type;

          if (sourceType === 'file') {
              new Command(`import${name}`)
                .option(`-i, --file <${importKey}-file-path>`, `Watch Mode: file glob pattern or directory | Normal Mode: file path for ${importKey} in ${sourceFormat} format`)
                .option('-w, --watch', 'Enable watch mode')
                .option('-gz, --gzip', 'File is gzip')
                .option('-z, --zip', 'File is zip')
                .description(`Imports ${importKey} that are in ${sourceFormat} format`)
                .action(
                  args => {
                      const dataPipelineProcessor = new DataPipelineProcessor(importConfig);
                      if (args.watch) {
                          return dataPipelineProcessor.watch({
                              filePattern: args.file,
                              indexer: indexerBuilder,
                              gzip: args.gzip,
                              zip: args.zip
                          });
                      }

                      return dataPipelineProcessor.process({
                          file: args.file,
                          indexer: indexerBuilder,
                          gzip: args.gzip,
                          zip: args.zip
                      });
                  },
                  {watch: !!importConfig.output.memorySize, memorySize: importConfig.output.memorySize, gcInterval: importConfig.output.gcInterval}
                );
          }
      });
}