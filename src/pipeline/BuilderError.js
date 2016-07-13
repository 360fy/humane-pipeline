import ValidationError from 'humane-node-commons/lib/ValidationError';

export default class BuilderError extends ValidationError {
    constructor(message, path) {
        super(!!path ? `At path: ${path}, found error: ${message}` : message);
    }
}