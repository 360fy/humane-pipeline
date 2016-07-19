// import _ from 'lodash';
// import Promise from 'bluebird';
// import * as Request from 'humane-node-commons/lib/Request';
// import {ArgBuilder} from '../pipeline/PipelineBuilder';

import HttpRequestWritable from '../writables/HttpRequestWritable';

export const name = 'humaneIndexSignal';

// export const supportsBatch = () => true;

// export const defaultArgs = () => ({
//     instance: new ArgBuilder('instance')
//       .required()
//       .description('Instance Url')
//       .build(),
//     // indexMethod: new ArgBuilder('indexMethod')
//     //   .required()
//     //   .description('Index handler method')
//     //   .build(),
//     indexType: new ArgBuilder('indexType')
//       .required()
//       .description('Index Type')
//       .build()
// });

export function builder(buildKey, settings) {
    // TODO: validate settings - must have instance, type

    return {
        settings,
        outputProcessor: (key, stream, params) => {
            const instance = params.instance;

            const finalStream = stream.pipe(new HttpRequestWritable(key, {
                reqUri: `${instance}/indexer/api/signal`, 
                reqMethod: 'PUT', 
                reqBodyBuilder: (chunk) => ({
                    type: chunk.type,
                    id: chunk.id,
                    signal: chunk.signal
                })}));

            finalStream.on('data', data => {
                console.log('Processed: ', JSON.stringify(data));
            });

            finalStream.on('finish', () => {
                params.resolve(true);
            });

            return finalStream;
        }
    };
}