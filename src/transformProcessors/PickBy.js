import * as BuilderUtils from './../pipeline/BuilderUtils';
import BuilderError from './../pipeline/BuilderError';
import PickByTransform from './../transforms/PickByTransform';

export const name = 'pickBy';

export function builder(buildKey, predicate) {
    if (!predicate) {
        throw new BuilderError('Predicate must be specified', buildKey);
    }

    BuilderUtils.validateSettingsWithSchema(buildKey, predicate, BuilderUtils.PredicateSchema, 'predicate');

    return {
        settings: {predicate},
        transformProcessor: (key, stream, params) => stream.pipe(new PickByTransform(key, params))
    };
}