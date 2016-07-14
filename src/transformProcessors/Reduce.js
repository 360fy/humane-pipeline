import _ from 'lodash';
import * as BuilderUtils from './../pipeline/BuilderUtils';
import BuilderError from './../pipeline/BuilderError';
import ReduceTransform from './../transforms/ReduceTransform';

export const name = 'reduce';

export const supportsBatch = () => true;

export function builder(buildKey, iteratee, accumulator) {
    if (!iteratee) {
        throw new BuilderError('Iteratee must be specified', buildKey);
    }

    BuilderUtils.validateSettingsWithSchema(buildKey, iteratee, BuilderUtils.IterateeSchema, 'iteratee');

    if (accumulator && (!_.isObject(accumulator) && !_.isArray(accumulator))) {
        throw new BuilderError('Accumulator must be either object or array', buildKey);
    }

    return {
        settings: {iteratee, accumulator},
        transformProcessor: (key, stream, params) => stream.pipe(new ReduceTransform(key, params))
    };
}