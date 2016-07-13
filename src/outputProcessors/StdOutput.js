export const name = 'stdout';

export function builder() {
    return {
        outputProcessor: (key, stream, params) => {
            const finalStream = stream.pipe(process.stdout);

            finalStream.on('finish', () => {
                params.resolve(true);
            });

            return finalStream;
        }
    };
}