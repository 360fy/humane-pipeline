import KeysTransform from './../transforms/KeysTransform';

export const name = 'keys';

export function builder() {
    return {
        transformProcessor: (key, stream, params) => stream.pipe(new KeysTransform(key, params))
    };
}