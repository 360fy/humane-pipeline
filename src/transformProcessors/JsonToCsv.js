import _ from 'lodash';
import Joi from 'joi';
import * as BuilderUtils from './../pipeline/BuilderUtils';
import buildCsvStringifier from 'csv-stringify';

export const name = 'jsonToCSV';

export function builder(buildKey, settings) {
    if (settings) {
        BuilderUtils.validateSettingsWithSchema(buildKey,
          settings,
          Joi.object({
              delimiter: Joi.string(),
              rowDelimiter: Joi.string(),
              header: Joi.boolean()
          }));
    } else {
        settings = {};
    }
    
    return {
        settings: _.defaults(settings, {header: true}),
        transformProcessor: (key, stream, params) => stream.pipe(buildCsvStringifier(params))
    };
}