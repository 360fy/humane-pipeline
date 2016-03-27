class FunctionMapper extends require('stream').Transform {
    constructor(config) {
        super({objectMode: true});
        this.mapper = config.fn;
        this.lookups = config.lookups;
    }

    _transform(chunk, encoding, done) {
        this.push(this.mapper(chunk, this.lookups));

        done();
    }
}

export default function (stream, params) {
    return stream.pipe(new FunctionMapper({fn: params.fn, lookups: params.lookups}));
}