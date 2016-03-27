import JsonTransform from '../transforms/JsonTransform';
import ArrayTransform from '../transforms/ArrayTransform';

export default function (stream, params) {
    return stream.pipe(new JsonTransform({path: params.jsonPath}))
      .pipe(new ArrayTransform());
}