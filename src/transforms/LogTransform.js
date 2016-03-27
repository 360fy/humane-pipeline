import _ from 'lodash';
import {StringDecoder} from 'string_decoder';

import moment from 'moment';

export default class LogTransform extends require('stream').Transform {
    constructor(options) {
        super({objectMode: true});
        this.decoder = new StringDecoder(options.encoding || 'utf8');
        this.regex = options.regex;
        this.fields = options.fields;
        this.transformFn = options.transform;
    }

    _transform(chunk, encoding, done) {
        const line = this.decoder.write(chunk);

        // parse str as per log config
        const match = line.match(this.regex);

        if (match) {
            let result = null;

            if (this.fields && (match.length > this.fields.length)) {
                result = {};

                _.forEach(this.fields, (field, index) => {
                    let value = _.trim(match[index + 1]);
                    if (!isNaN(value)) {
                        value = Number(value);
                    }

                    let fieldKey = null;
                    let fieldInfo = null;
                    if (_.isString(field)) {
                        fieldKey = field;
                    } else if (_.isObject(field)) {
                        fieldKey = _(field).keys().first();
                        fieldInfo = field[fieldKey];
                    }

                    if (fieldInfo) {
                        if (fieldInfo.type === 'date') {
                            const date = moment(value, fieldInfo.format, true);
                            if (date) {
                                value = date.toDate();
                            }
                        }

                        if (_.isFunction(fieldInfo.transform)) {
                            value = fieldInfo.transform(value);
                        }
                    }

                    result[fieldKey] = value;
                });

                if (this.transformFn) {
                    try {
                        result = this.transformFn(result);
                    } catch (ex) {
                        console.error('Error in .transformFn():', ex);
                    }
                }

                if (result) {
                    if (_.isArray(result)) {
                        _.forEach(result, (item) => this.push(item));
                    } else {
                        this.push(result);
                    }
                }
            }
        }

        done();
    }
}