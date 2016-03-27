import LineSeparatorTransform from '../transforms/LineSeparatorTransform';

export default function (stream /*, params*/) {
    return stream.pipe(new LineSeparatorTransform({jsonParse: true}));
}