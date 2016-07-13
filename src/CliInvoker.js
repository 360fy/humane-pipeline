import 'babel-polyfill';
import Promise from 'bluebird';
import globalOption from 'command-line-boilerplate/lib/GlobalOption';
import globalArg from 'command-line-boilerplate/lib/GlobalArg';
import runCli from 'command-line-boilerplate/lib/CliRunner';
import outputHelp from 'command-line-boilerplate/lib/OutputHelp';
import cli from './Cli';
import loadPlugin from 'plugin-boilerplate/lib/PluginLoader';
import ValidationError from 'humane-node-commons/lib/ValidationError';
// import _ from 'lodash';
// import Path from 'path';
// import Config from 'config-boilerplate/lib/Config';

// globalOption('-c, --config [CONFIG]', 'Path to JSON / YAML based environment configs, such as esConfig, redisConfig etc');

globalOption('-d, --definition [PIPELINE DEFINITION]',
  `Path to pipeline definition.
  Path to pipeline defined in JS, through local NPM module, or through global NPM module.`
);

// runs the cli
runCli(true);

function validPipelineDefinition(path, throwError) {
    if (!path) {
        return null;
    }

    return loadPlugin(process.env.MODULE_ROOT, path, throwError);
}

Promise.resolve(globalArg('definition'))
  .then(plugin => validPipelineDefinition(plugin, true))
  .then(plugin => {
      if (!plugin) {
          console.error('No pipeline definition was specified or found');

          outputHelp();

          return false;
      }

      // const defaultConfig = globalArg('config')
      //   ? new Config('default', globalArg('config'), Path.join(__dirname, '..', 'config'))
      //   : new Config('default', Path.join(__dirname, '..', 'config'));

      // return cli(_.defaultsDeep({plugin}, defaultConfig));

      return cli({plugin});
  })
  .catch((error) => error.name === 'ValidationError' || error instanceof ValidationError, (error) => {
      console.error('VALIDATION ERROR: ', error.message);
  });
