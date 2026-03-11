# Refactoring ObjectIdMapping and MemQuadStorage

The goal of this task is to decouple `MemQuadStorage` from `MemObjectIdMapping` and improve the performance of the `ObjectIdMapping` trait by using optimized scalar operations and a vectorized insertion path.

## Open Issues

- [ ] **Refactor `ObjectIdMapping` trait and scalar types**
    - [ ] Update `ObjectIdScalar` to use `Box<[u8]>` internally instead of `ScalarValue`.
    - [ ] Update the `ObjectIdMapping` trait to return `ObjectIdScalar` (or a similar optimized type) where appropriate.
    - [ ] Ensure `ObjectIdScalar` is used instead of the previously planned `BoxedObjectId`.

- [ ] **Vectorized Insertion Path**
    - [ ] Refactor `MemQuadStorage::extend` to avoid the row-by-row `encode_quad` path.
    - [ ] Introduce or use a `PlainTermQuads` array structure (containing four `PlainTermArray` components: graph, subject, predicate, object).
    - [ ] Implement vectorized encoding of these components using `ObjectIdMapping::encode_array`.
    - [ ] Update `MemQuadIndex::insert` to handle these encoded arrays efficiently.

- [ ] **Decouple `MemQuadStorage` from `MemObjectIdMapping`**
    - [ ] Ensure `MemQuadStorage` (and its components like `scan`, `snapshot`, etc.) only interacts with the `ObjectIdMapping` trait via `self.encoding.mapping()`.
    - [ ] Remove any remaining direct dependencies on `MemObjectIdMapping`MemQuadIndex implementation details.

- [ ] **Prefer `PlainTermScalar` over `TermRef`**
    - [ ] Update `ObjectIdMapping::try_get_object_id` and `encode_scalar` to take `&PlainTermScalar` instead of `TermRef<'_>`.
    - [ ] Ensure consistency across the codebase where `PlainTermScalar` is the preferred representation.

- [ ] **Address Performance Regressions**
    - [ ] **Direct Scalar Operations**: Implement `encode_scalar` and `try_get_object_id` directly in `MemObjectIdMapping` to avoid the extremely slow path of creating one-element Arrow arrays.
    - [ ] **Optimized Decoding**: Add a way to decode a single object ID directly to a `PlainTermScalar` without going through `ScalarValue::to_array()` and `decode_array`.
    - [ ] **DataFusion Kernels**: Acknowledge that query regressions are likely rooted in DataFusion's `FixedSizeBinary` kernels.
    - [ ] **Avoid unnecessary BTreeSet rebuilds**: In `MemQuadIndex::insert`, optimize how quads are collected and deduplicated across permutations.

- [ ] **Validation and Testing**
    - [ ] Verify that all existing tests pass.

## Additional Context

- The performance dropped by ~80% in `insert` and ~50% in `extend` benchmarks.
- The `encode_quad` row-by-row approach is a major bottleneck.
- `ObjectIdScalar` will be the primary vehicle for handling individual object IDs, avoiding `ScalarValue` overhead where possible.
- No ID length specific optimizations are requested at this time.
