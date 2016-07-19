// import _ from 'lodash';
// import Promise from 'bluebird';
// import * as Request from 'humane-node-commons/lib/Request';
// import {ArgBuilder} from '../pipeline/PipelineBuilder';

import HttpRequestWritable from '../writables/HttpRequestWritable';

export const name = 'http';

// export const defaultArgs = () => ({
//     reqUri: new ArgBuilder('reqUri')
//       .required()
//       .description('Request URI')
//       .build(),
//     reqMethod: new ArgBuilder('reqMethod')
//       .required()
//       .description('Request Http Method')
//       .build()
// });

export function builder(buildKey, settings) {
    // let settings = null;
    // if (_.isString(indexTypeOrSettings)) {
    //     settings = {indexType: indexTypeOrSettings, handler: 'upsert'};
    // } else if (_.isObject(indexTypeOrSettings)) {
    //     settings = _.defaults(indexTypeOrSettings, {handler: 'upsert'});
    // }

    // TODO: validate settings - must have instance, type

    return {
        settings,
        outputProcessor: (key, stream, params) => {
            const reqUri = params.reqUri;
            const reqMethod = params.reqMethod;

            const finalStream = stream.pipe(new HttpRequestWritable(key, {reqUri, reqMethod}));

            // finalStream.on('data', data => {
            //     console.log('Processed: ', JSON.stringify(data));
            // });

            finalStream.on('finish', () => {
                params.resolve(true);
            });

            return finalStream;
        }
    };
}