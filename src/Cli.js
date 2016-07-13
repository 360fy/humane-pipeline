import currentCommand from 'command-line-boilerplate/lib/CurrentCommand';
import outputHelp from 'command-line-boilerplate/lib/OutputHelp';
import runCli from 'command-line-boilerplate/lib/CliRunner';

import buildPipelineCli from './PipelineCliBuilder';

//
// cli specific includes
//
export default function (config) {
    buildPipelineCli(config.plugin);

    // runs the cli
    runCli(true);

    if (!currentCommand()) {
        // output help
        outputHelp();
    }
}