import JsonParser from 'jsonparse';
import _ from 'lodash';
import {StringDecoder} from 'string_decoder';

function check(pathKey, dataKey) {
    if (_.isString(pathKey)) {
        return dataKey === pathKey;
    } else if (pathKey && _.isFunction(pathKey.exec)) {
        return pathKey.exec(dataKey);
    } else if (_.isBoolean(pathKey)) {
        return pathKey;
    } else if (_.isFunction(pathKey)) {
        return pathKey(dataKey);
    }

    return false;
}

export default class JsonTransform extends require('stream').Transform {
    constructor(options) {
        super({readableObjectMode: true});
        this.decoder = new StringDecoder(options && options.encoding || 'utf8');

        this.root = null;

        let path = options && options.path;

        if (_.isString(path)) {
            path = path.split('.').map(e => {
                if (e === '*') {
                    return true;
                } else if (e === '') { // '..'.split('.') returns an empty string
                    return {recurse: true};
                }

                return e;
            });
        }

        if (!path || !path.length) {
            path = null;
        }

        this.path = path;

        this.count = 0;

        this.parser = new JsonParser();

        const _onToken = this.parser.onToken.bind(this.parser);

        this.parser.onValue = JsonTransform.onJsonValue(this).bind(this.parser);
        this.parser.onToken = JsonTransform.onJsonToken(_onToken, this).bind(this.parser);
        this.parser.onError = JsonTransform.onJsonError(this).bind(this.parser);
    }

    static onJsonToken(superOnToken, stream) {
        return function (token, value) {
            superOnToken(token, value);
            if (this.stack.length === 0) {
                if (stream.root) {
                    if (!stream.path) {
                        stream.push(stream.root);
                    }

                    stream.count = 0;
                    stream.root = null;
                }
            }
        };
    }

    static onJsonError(stream) {
        return function (err) {
            if (err.message.indexOf('at position') > -1) {
                err.message = `Invalid JSON (${err.message})`;
            }

            stream.emit('error', err);
        };
    }

    static onJsonValue(stream) {
        return function (value) {
            if (!stream.root) {
                stream.root = value;
            }

            if (!stream.path) {
                return;
            }

            let i = 0; // iterates on path
            let j = 0; // iterates on stack
            while (i < stream.path.length) {
                const pathKey = stream.path[i];

                let dataKey;

                j++;

                if (pathKey && !pathKey.recurse) {
                    dataKey = (j === this.stack.length) ? this : this.stack[j];

                    if (!dataKey) {
                        return;
                    }

                    if (!check(pathKey, dataKey.key)) {
                        return;
                    }

                    i++;
                } else {
                    i++;

                    const nextKey = stream.path[i];
                    if (!nextKey) return;

                    while (true) {
                        dataKey = (j === this.stack.length) ? this : this.stack[j];

                        if (!dataKey) {
                            return;
                        }

                        if (check(nextKey, dataKey.key)) {
                            i++;
                            this.stack[j].value = null;
                            break;
                        }

                        j++;
                    }
                }
            }

            if (j !== this.stack.length) {
                return;
            }

            stream.count++;

            const actualPath = this.stack.slice(1).map((element) => element.key).concat([this.key]);

            let data = this.value[this.key];

            if (!_.isNull(data)) {
                data = stream.map ? stream.map(data, actualPath) : data;

                if (!_.isNull(data)) {
                    stream.push(data);
                }
            }

            delete this.value[this.key];

            _.forEach(this.stack, (stackValue, index) => {
                this.stack[index].value = null;
                return true;
            });
        };
    }

    _transform(chunk, encoding, done) {
        if (_.isString(chunk)) {
            chunk = new Buffer(chunk);
        }

        this.parser.write(chunk);

        done();
    }
}