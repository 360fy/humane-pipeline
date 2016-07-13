import BuilderError from './../pipeline/BuilderError';
import * as BuilderUtils from './../pipeline/BuilderUtils';
import FilterTransform from './../transforms/FilterTransform';

export const name = 'filter';

export function builder(buildKey, iteratee) {
    // iteratee must be specified
    if (!iteratee) {
        throw new BuilderError('Iteratee must be specified', buildKey);
    }

    BuilderUtils.validateSettingsWithSchema(buildKey, iteratee, BuilderUtils.IterateeSchema, 'iteratee');

    return {
        settings: {iteratee},
        transformProcessor: (key, stream, params) => stream.pipe(new FilterTransform(key, params))
    };
}