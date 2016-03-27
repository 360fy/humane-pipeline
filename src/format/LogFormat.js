import LineSeparatorTransform from '../transforms/LineSeparatorTransform';
import LogTransform from '../transforms/LogTransform';

export default function (stream, params) {
    return stream.pipe(new LineSeparatorTransform())
      .pipe(new LogTransform({regex: params.regex, fields: params.fields, transform: params.transform}));
}