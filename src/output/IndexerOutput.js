import _ from 'lodash';
import Promise from 'bluebird';
/* eslint-disable global-require */

class IndexerDataTransform extends require('stream').Transform {
    constructor(params) {
        super({objectMode: true});

        if (!params || !params.indexer) {
            throw new Error('Params must have indexer instance');
        }

        this.indexer = params.indexer;
        this.handler = params.handler;
        this.indexType = params.indexType;
        this.filter = params.filter;
    }

    _transform(chunk, encoding, done) {
        Promise.resolve(this.handler.call(this.indexer, null, {type: this.indexType, doc: chunk, filter: this.filter}))
          .then(result => {
              this.push(result);
              done();
          })
          .catch(error => {
              console.error('Error in executing Indexer: ', chunk, this.indexType, this.handler, error, error.stack);
              this.push({error});
              done();
          });
    }
}

export default function (stream, params) {
    const eventEmitter = params.eventEmitter;
    const indexer = _.isFunction(params.indexer) ? params.indexer() : params.indexer;
    const handler = indexer[params.handler];
    const indexType = params.indexType;
    const filter = params.filter;

    const finalStream = stream.pipe(new IndexerDataTransform({indexer, handler, indexType, filter}));
    
    finalStream.on('data', data => {
        console.log('Processed: ', JSON.stringify(data));        
    });
    
    finalStream.on('end', () => {
        Promise.resolve(indexer.shutdown())
          .then(() => {
              eventEmitter.emit('OUTPUT_FINISH');
          });
    });

    return finalStream;
}