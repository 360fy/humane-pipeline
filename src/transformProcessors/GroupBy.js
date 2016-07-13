import * as BuilderUtils from './../pipeline/BuilderUtils';
import BuilderError from './../pipeline/BuilderError';
import GroupByTransform from './../transforms/GroupByTransform';

export const name = 'groupBy';

export const supportsBatch = () => true;

export function builder(buildKey, iteratee) {
    if (!iteratee) {
        throw new BuilderError('Iteratee must be specified', buildKey);
    }

    BuilderUtils.validateSettingsWithSchema(buildKey, iteratee, BuilderUtils.IterateeSchema, 'iteratee');

    return {
        settings: {iteratee},
        transformProcessor: (key, stream, params) => stream.pipe(new GroupByTransform(key, params))
    };
}