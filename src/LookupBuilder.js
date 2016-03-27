import _ from 'lodash';
import Promise from 'bluebird';

import performanceNow from 'performance-now';

import Settings from './Settings';

export default function (lookupName, config) {
    const sourceHandler = Settings.source[config.input.source.type];
    const formatHandler = Settings.format[config.input.format.type];

    console.log('Building Lookup for: ', lookupName, config);

    let stream = sourceHandler(config.input.source);

    stream = formatHandler(stream, config.input.format);

    if (config.mapper) {
        const mapperHandler = Settings.mapper[config.mapper.type];

        stream = mapperHandler(stream, config.mapper);
    }

    let queuedCount = 0;
    let processedCount = 0;

    const lookup = {};

    stream.on('data', data => {
        const numIndex = queuedCount++;

        const startTime = performanceNow();

        const key = data[config.key];
        lookup[key] = data;

        console.log(`Processed #${numIndex}: ${key} in ${(performanceNow() - startTime).toFixed(3)} ms`);

        processedCount++;
    });

    return new Promise(resolve => {
        stream.on('end', () => {
            function shutdownIndexerIfProcessed() {
                if (queuedCount === processedCount) {
                    resolve({get: (key) => lookup[key]});
                } else {
                    // schedule next one
                    _.delay(shutdownIndexerIfProcessed, 5000);
                }
            }

            shutdownIndexerIfProcessed();
        });
    });
}