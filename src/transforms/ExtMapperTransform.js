import _ from 'lodash';
import Promise from 'bluebird';

class MapperFnWriter extends require('stream').Writable {

    constructor(params) {
        super({objectMode: true});

        this._data = params.__ext_mapper_chunk__;
        this._mapperFn = params.mapperFn;
    }

    _write(chunk, encoding, done) {
        Promise.resolve(this._mapperFn(this._data, chunk))
          .then(() => done());

        return true;
    }
}

export function extMapperWriterBuilder(buildKey, mapperFn) {
    return {
        settings: {mapperFn},
        outputProcessor: (key, stream, params) => {
            const finalStream = stream.pipe(new MapperFnWriter(params));

            finalStream.on('finish', () => {
                params.resolve(true);
            });
        }
    };
}

export default class ExtMapperTransform extends require('stream').Transform {
    constructor(key, params) {
        super({readableObjectMode: true, writableObjectMode: true});

        this._mapKeyFn = params.mapKeyFn;
        this._extDataMapPipeline = params.extDataMapPipeline;
        this._params = _.omit(params, ['mapKeyFn', 'extDataMapPipeline']);
    }

    _transform(chunk, encoding, done) {
        const keyParams = this._mapKeyFn(chunk);

        const inputPipeline = this._extDataMapPipeline.inputPipeline();

        const inputProcessorBuilder = inputPipeline.inputProcessor();
        // if (!_.isFunction(inputProcessorBuilder)) {
        //     throw new ValidationError(`InputProcessor for pipeline ${key} must be a builder function`);
        // }

        const inputProcessor = inputProcessorBuilder(this._extDataMapPipeline, _.defaults({}, keyParams, inputPipeline.settings(), this._params), {__ext_mapper_chunk__: chunk});

        Promise.resolve(inputProcessor.run())
          .then(() => {
              this.push(chunk);
              done();
          });
    }
}