import _ from 'lodash';
import * as Request from 'humane-node-commons/lib/Request';

export default class HttpRequestWritable extends require('stream').Writable {
    constructor(key, params) {
        super({objectMode: true});
        
        // TODO: validate params

        // this._baseUrl = _.get(params, 'baseUrl', '');
        
        this._reqMethod = _.get(params, 'reqMethod');
        this._reqUri = _.get(params, 'reqUri');
        this._logLevel = _.get(params, 'logLevel', 'info');
        
        this._https = _.get(params, 'https', /^https:/.test(this._reqUri));
        
        this._requestBodyBuilder = _.get(params, 'reqBodyBuilder', (chunk) => chunk);
        
        // this._requestUriBuilder = _.get(params, 'requestUriBuilder', () => this._reqUri || '');
        // this._requestMethodBuilder = _.get(params, 'requestMethodBuilder', () => this._reqMethod || 'POST');

        this._requestParams = {uri: this._reqUri, https: this._https, logLevel: this._logLevel, method: this._reqMethod};

        this._request = Request.builder(this._requestParams);
    }

    _write(chunk, encoding, done) {
        Promise.resolve(this._request({
            // method: this._requestMethodBuilder(chunk),
            // uri: this._requestUriBuilder(chunk),
            body: this._requestBodyBuilder(chunk)
        }))
          .then(response => Request.handleResponse(response))
          .then(() => done())
          .catch(error => {
              // TODO: better print error
              console.error('<< ERROR >>', chunk, this._requestParams, error, error.stack);
              done();
          });

        return true;
    }

}