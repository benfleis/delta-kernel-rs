# duckdb-delta issue #303 — partition predicate reproducer

Demonstrates that delta-kernel-rs correctly evaluates typed equality predicates on
`BIGINT` partition columns via the C FFI. This confirms that duckdb/duckdb-delta#303
is a bug in the extension's `PredicateVisitor`, not in the kernel.

Upstream issue: https://github.com/duckdb/duckdb-delta/issues/303

## What it tests

Against a Delta table with a `BIGINT` partition column `part`, two predicate forms are
sent via `EnginePredicate` / `visit_predicate_eq`:

| Predicate | Expected | Actual (both builds) |
|---|---|---|
| baseline (no predicate) | N files | ✅ N |
| `column("part") = literal_long(42)` | N files (all match) | ✅ N |
| `column("part") = literal_long(31)` | 0 files (none match) | ✅ 0 |

The `literal_long` (Int64) form matches what duckdb-delta's `PredicateVisitor` sends for
`BIGINT` partition columns. The kernel evaluates both cases correctly in default and
`--all-features` builds — the partition predicate bug is not in the kernel.

## Build the kernel library

From the repo root (`delta-kernel-rs/`):

```bash
cargo build -p delta_kernel_ffi
# or: cargo build -p delta_kernel_ffi --all-features
```

## Build and run the test

```bash
cd ffi/examples/duckdb-delta-issue-303
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build
./build/partition_pred_test <path-to-delta-table>
```

The table must have a `BIGINT` partition column named `part` with at least one file
where `part = 42`. The duckdb-delta test table at
`duckdb-delta/data/inlined/issue_303_partitioned/delta_lake` works.

## Expected output (both builds)

```
=== baseline (no predicate) ===
files returned: 12

=== part = 42 (should match all) ===
files returned: 12

=== part = 31 (should match none) ===
files returned: 0

--- summary ---
baseline:            12 files
part=42 [expect 12]: 12
part=31 [expect 0]:  0
PASS
```

The partition value in the Delta log is stored as the string `"42"` but the kernel
converts it to `Int64` via `MapToStruct` before comparison — so `literal_long(42)`
matches correctly. The bug in duckdb-delta is that the extension's `PredicateVisitor`
does not always send the right type.
