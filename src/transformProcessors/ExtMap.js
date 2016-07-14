// import _ from 'lodash';
// import * as BuilderUtils from './../pipeline/BuilderUtils';
// import BuilderError from './../pipeline/BuilderError';
import ExtMapperTransform from './../transforms/ExtMapperTransform';
import {ExtDataMapPipelineBuilder} from './../pipeline/PipelineBuilder';

export const name = 'extMap';

// export const supportsBatch = () => true;

export function builder(buildKey, mapKeyFn, extDataMapPipelineBuilderFn, mapperFn) {
    // if (!iteratee) {
    //     throw new BuilderError('Iteratee must be specified', buildKey);
    // }
    //
    // BuilderUtils.validateSettingsWithSchema(buildKey, iteratee, BuilderUtils.IterateeSchema, 'iteratee');
    //
    // if (accumulator && (!_.isObject(accumulator) || !_.isArray(accumulator))) {
    //     throw new BuilderError('Accumulator must be either object or array', buildKey);
    // }

    const extDataMapPipelineBuilder = new ExtDataMapPipelineBuilder(mapperFn);

    extDataMapPipelineBuilderFn(extDataMapPipelineBuilder);

    const extDataMapPipeline = extDataMapPipelineBuilder.build();

    return {
        settings: {mapKeyFn, extDataMapPipeline},
        transformProcessor: (key, stream, params) => stream.pipe(new ExtMapperTransform(key, params))
    };
}