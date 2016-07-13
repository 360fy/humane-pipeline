import _ from 'lodash';
import Joi from 'joi';
import CsvToJsonTransform from './../transforms/CsvToJsonTransform';
import * as BuilderUtils from './../pipeline/BuilderUtils';

export const name = 'csvToJSON';

export function builder(buildKey, settings) {
    // either none is specified or settings must be an object with delimiter, rowDelimiter, quote, escape, header, columns, trim properties
    if (settings) {
        BuilderUtils.validateSettingsWithSchema(buildKey,
          settings,
          Joi.object({
              delimiter: Joi.string(),
              rowDelimiter: Joi.string(),
              quote: Joi.string(),
              escape: Joi.string(),
              header: Joi.boolean(),
              fields: Joi.array().items(Joi.string()),
              trim: Joi.boolean()
          }));
    } else {
        settings = {};
    }

    return {
        settings: _.defaults(settings, {trim: true}),
        transformProcessor: (key, stream, params) => stream.pipe(new CsvToJsonTransform(key, params))
    };
}