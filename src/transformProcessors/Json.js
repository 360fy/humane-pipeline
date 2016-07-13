import LineSeparatorTransform from './../transforms/LineSeparatorTransform';

export const name = 'json';

export function builder() {
    return {
        settings: {jsonParse: true},
        transformProcessor: (key, stream, params) => stream.pipe(new LineSeparatorTransform(key, params))
    };
}