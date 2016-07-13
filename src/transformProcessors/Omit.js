import _ from 'lodash';
import * as BuilderUtils from './../pipeline/BuilderUtils';
import BuilderError from './../pipeline/BuilderError';
import OmitTransform from './../transforms/OmitTransform';

export const name = 'omit';

export function builder(buildKey, ...props) {
    if (!props || _.isEmpty(props)) {
        throw new BuilderError('Props must be specified', buildKey);
    }

    BuilderUtils.validateSettingsWithSchema(buildKey, props, BuilderUtils.PropsSchema, 'props');

    return {
        settings: {props},
        transformProcessor: (key, stream, params) => stream.pipe(new OmitTransform(key, params))
    };
}