import assert from 'node:assert/strict';

// Product JSON stays unchanged. Only an observed JavaScript string containing an
// unpaired surrogate uses this reserved trace marker, preserving every UTF-16 unit.
export function isScalarString(value) {
  for (let index = 0; index < value.length; index++) {
    const unit = value.charCodeAt(index);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const next = value.charCodeAt(++index);
      if (!(next >= 0xdc00 && next <= 0xdfff)) return false;
    } else if (unit >= 0xdc00 && unit <= 0xdfff) return false;
  }
  return true;
}
export function encodeTraceValue(value) {
  if (typeof value === 'string') {
    return isScalarString(value) ? value : { $utf16: Array.from({ length: value.length }, (_, index) => value.charCodeAt(index)) };
  }
  if (Array.isArray(value)) return value.map(encodeTraceValue);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).map(([key, child]) => {
      assert.ok(isScalarString(key), 'Non-scalar object keys are outside these trace inputs');
      return [key, encodeTraceValue(child)];
    }));
  }
  return value;
}
export function assertScalarStrings(value, path = '$') {
  if (typeof value === 'string') assert.ok(isScalarString(value), `Non-scalar JSON string at ${path}`);
  else if (Array.isArray(value)) value.forEach((child, index) => assertScalarStrings(child, `${path}[${index}]`));
  else if (value && typeof value === 'object') {
    for (const [key, child] of Object.entries(value)) {
      assert.ok(isScalarString(key), `Non-scalar JSON object key at ${path}`);
      assertScalarStrings(child, `${path}.${key}`);
    }
  }
}
