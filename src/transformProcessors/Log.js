import Joi from 'joi';
import * as BuilderUtils from './../pipeline/BuilderUtils';
import LineSeparatorTransform from './../transforms/LineSeparatorTransform';
import LogTransform from './../transforms/LogTransform';

export const name = 'log';

export function builder(buildKey, settings) {
    BuilderUtils.validateSettingsWithSchema(buildKey,
      settings,
      Joi.object({
          regex: Joi.object().type(RegExp).required(),
          fields: Joi.array().items(Joi.string()),
          transform: Joi.func()
      }));


    return {
        settings,
        transformProcessor: (key, stream, params) => stream.pipe(new LineSeparatorTransform(key)).pipe(new LogTransform(key, params))
    };
}