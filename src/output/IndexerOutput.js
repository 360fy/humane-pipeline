import _ from 'lodash';

export default class IndexerOutput {
    constructor(params) {
        if (!params || !params.indexer) {
            throw new Error('Params must have indexer instance');
        }

        this.indexer = _.isFunction(params.indexer) ? params.indexer() : params.indexer;
        this.handler = this.indexer[params.handler];
        this.indexType = params.indexType;
    }

    handle(doc, filter) {
        return this.handler.call(this.indexer, null, {type: this.indexType, doc, filter});
    }

    shutdown() {
        return this.indexer.shutdown();
    }
}