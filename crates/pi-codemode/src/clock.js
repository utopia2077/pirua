// The controller remains private to Rust; scripts only see the frozen globals.
(function (startedAt, seed, controlled) {
    const RealDate = Date;
    const realNow = RealDate.now;
    let now = startedAt;
    function ScriptDate(...args) {
        if (!new.target) return new RealDate(controlled ? now : realNow()).toString();
        return Reflect.construct(RealDate, args.length ? args : [controlled ? now : realNow()], new.target);
    }
    ScriptDate.prototype = RealDate.prototype;
    Object.defineProperty(ScriptDate.prototype, "constructor", { value: ScriptDate, writable: true, configurable: true });
    ScriptDate.now = () => controlled ? now : realNow();
    ScriptDate.parse = RealDate.parse;
    ScriptDate.UTC = RealDate.UTC;
    globalThis.Date = ScriptDate;
    globalThis.performance = { now: () => (controlled ? now : realNow()) - startedAt, timeOrigin: startedAt };
    if (controlled) {
        let state = seed >>> 0;
        Math.random = () => {
            state = (state + 0x6d2b79f5) >>> 0;
            let value = Math.imul(state ^ (state >>> 15), 1 | state);
            value = (value + Math.imul(value ^ (value >>> 7), 61 | value)) ^ value;
            return ((value ^ (value >>> 14)) >>> 0) / 4294967296;
        };
    }
    return (next) => { now = Math.max(now, next); };
})
