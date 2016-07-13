import Joi from 'joi';
import BuilderError from './BuilderError';

export const PropsSchema = Joi.alternatives().try(Joi.string(), Joi.array().items(Joi.string()));

// in lodash iteratee can be Array, Function, Object, String
// Array is matched by _.matchesProperty shorthand - first index being property path, 2nd being property value
// String is matched by _.property shorthand
// Object is matched by _.matches shorthand
export const IterateeSchema = Joi.alternatives().try(Joi.string(), Joi.func(), Joi.array().items(Joi.string()).length(2), Joi.object());

// in lodash predicate can be Array, Function, Object, String
// Array is matched by _.matchesProperty shorthand - first index being property path, 2nd being property value
// String is matched by _.property shorthand
// Object is matched by _.matches shorthand
export const PredicateSchema = Joi.alternatives().try(Joi.string(), Joi.func(), Joi.array().items(Joi.string()).length(2), Joi.object());

export function validateSettingsWithSchema(key, settings, schema, name) {
    const validationResult = Joi.validate(settings, schema);
    if (validationResult.error) {
        let message = null;
        if (name) {
            message = `Invalid ${name} settings. Reason: ${validationResult.error}`;
        } else {
            message = `Invalid settings. Reason: ${validationResult.error}`;
        }
        
        throw new BuilderError(message, key);
    }
}