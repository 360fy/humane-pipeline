import _ from 'lodash';
import Joi from 'joi';
import * as BuilderUtils from './../pipeline/BuilderUtils';
import BuilderError from './../pipeline/BuilderError';
import JsonTransform from './../transforms/JsonTransform';
import ArrayTransform from './../transforms/ArrayTransform';

export const name = 'jsonArray';

export function builder(buildKey, pathOrSettings) {
    if (!pathOrSettings) {
        throw new BuilderError(`${name}: json path or object with path should be provided.`);
    }

    let settings = null;
    if (_.isString(pathOrSettings)) {
        settings = {path: pathOrSettings};
    } else if (_.isObject(pathOrSettings)) {
        settings = pathOrSettings;
    }

    BuilderUtils.validateSettingsWithSchema(buildKey, settings, Joi.object({path: Joi.string()}));

    return {
        settings,
        transformProcessor: (key, stream, params) => stream.pipe(new JsonTransform(key, params)).pipe(new ArrayTransform(key))
    };
}