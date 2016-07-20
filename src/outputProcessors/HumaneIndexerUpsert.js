// import _ from 'lodash';
// import Promise from 'bluebird';
import {ArgBuilder} from '../pipeline/PipelineBuilder';

import HttpRequestWritable from '../writables/HttpRequestWritable';

export const name = 'humaneIndexUpsert';

// export const supportsBatch = () => true;

export const defaultArgs = () => ({
    instance: new ArgBuilder('instance')
      .required()
      .description('Instance Url')
      .build(),
    indexType: new ArgBuilder('indexType')
      .required()
      .description('Index Type')
      .build()
});

export function builder(buildKey, settings) {
    // TODO: validate settings - must have instance, type

    return {
        settings,
        outputProcessor: (key, stream, params) => {
            const instance = params.instance;
            const indexType = params.indexType;

            const finalStream = stream.pipe(new HttpRequestWritable(key, {
                reqUri: `${instance}/indexer/api/upsert`, 
                reqMethod: 'POST', 
                reqBodyBuilder: (chunk) => ({
                    type: indexType,
                    doc: chunk
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