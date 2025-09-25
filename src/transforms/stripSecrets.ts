export function transform(doc: any) {
  // Remove commonly sensitive fields
  if (doc && typeof doc === 'object') {
    const copy = { ...doc };
    delete copy.password;
    delete copy.secret;
    return copy;
  }
  return doc;
}
