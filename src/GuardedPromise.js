import Promise from 'bluebird';

export function limit(allowed) {
    let count = 0;
    const waiting = [];

    const exit = function () {
        if (count > 0) {
            count--;
        }
        if (waiting.length) {
            waiting.shift()(exit);
        }
    };

    return function enter() {
        return new Promise((resolve) => {
            if (count < allowed) {
                resolve(exit);
            } else {
                waiting.push(resolve);
            }
            count += 1;
        });
    };
}

export function guard(condition, fn) {
    if (typeof condition === 'number') {
        condition = limit(condition);
    }

    return function (...params) {
        const self = this;
        const args = new Array(params.length);
        for (let i = 0; i < args.length; i++) {
            args[i] = params[i];
        }

        return Promise.resolve(condition()).then((exit) => Promise.resolve(fn.apply(self, args)).finally(exit));
    };
}