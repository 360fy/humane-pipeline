import ValuesTransform from './../transforms/ValuesTransform';

export const name = 'values';

export function builder() {
    return {
        transformProcessor: (key, stream, params) => stream.pipe(new ValuesTransform(key, params))
    };
}