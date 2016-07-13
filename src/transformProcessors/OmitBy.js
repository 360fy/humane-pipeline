import * as BuilderUtils from './../pipeline/BuilderUtils';
import BuilderError from './../pipeline/BuilderError';
import OmitByTransform from './../transforms/OmitByTransform';

export const name = 'omitBy';

export function builder(buildKey, predicate) {
    if (!predicate) {
        throw new BuilderError('Predicate must be specified', buildKey);
    }

    BuilderUtils.validateSettingsWithSchema(buildKey, predicate, BuilderUtils.PredicateSchema, 'predicate');

    return {
        settings: {predicate},
        transformProcessor: (key, stream, params) => stream.pipe(new OmitByTransform(key, params))
    };
}