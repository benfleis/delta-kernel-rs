// partition_pred_test.cpp
//
// Confirms that the kernel correctly evaluates typed (Int64) equality predicates
// on a BIGINT partition column via the C FFI.  Regression test for the bug
// reported in duckdb/duckdb-delta#303, where the bug was in the extension's
// PredicateVisitor, not the kernel.
//
// Build:
//   cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug && cmake --build build
// Run:
//   ./build/partition_pred_test <path-to-delta-table>

#define DEFINE_DEFAULT_ENGINE_BASE 1
#include "delta_kernel_ffi.hpp"

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <string>

using namespace ffi;

struct KernelErr : public EngineError { std::string msg; };

struct ScanCtx { Handle<SharedExternEngine> engine; int files = 0; };

static EngineError *alloc_err(KernelError etype, KernelStringSlice msg) {
    auto *e = new KernelErr();
    e->etype = etype;
    e->msg = {msg.ptr, msg.len};
    return e;
}

static void on_file(NullableCvoid ctx, KernelStringSlice path,
                    int64_t, int64_t, const Stats *, const CDvInfo *,
                    const Expression *, const CStringMap *) {
    static_cast<ScanCtx *>(ctx)->files++;
    printf("  file: %.*s\n", (int)path.len, path.ptr);
}

static void on_batch(NullableCvoid ctx, Handle<SharedScanMetadata> meta) {
    auto *c = static_cast<ScanCtx *>(ctx);
    auto r = visit_scan_metadata(meta, c->engine, ctx, on_file);
    if (r.tag != ExternResult<bool>::Tag::Ok) { fprintf(stderr, "visit_scan_metadata failed\n"); exit(1); }
}

// val_ptr == nullptr  → trivially-true (empty AND); otherwise part = *val_ptr
static uintptr_t pred_fn(void *val_ptr, KernelExpressionVisitorState *state) {
    if (!val_ptr) {
        EngineIterator it{nullptr, [](void *) -> const void * { return nullptr; }};
        return visit_predicate_and(state, &it);
    }

    auto col = visit_expression_column(state, {"part", 4}, alloc_err);
    if (col.tag != ExternResult<uintptr_t>::Tag::Ok) { fprintf(stderr, "visit_expression_column failed\n"); exit(1); }

    uintptr_t eq = visit_predicate_eq(state, col.ok._0,
                                       visit_expression_literal_long(state, *static_cast<long *>(val_ptr)));

    struct Once { uintptr_t val; bool done = false; };
    Once once{eq};
    EngineIterator it{&once, [](void *p) -> const void * {
        auto *o = static_cast<Once *>(p);
        if (o->done) return nullptr;
        o->done = true;
        return reinterpret_cast<const void *>(o->val);
    }};
    return visit_predicate_and(state, &it);
}

static int run_scan(Handle<SharedExternEngine> engine, const char *path,
                    long *pred_val, const char *label) {
    printf("\n=== %s ===\n", label);

    EnginePredicate pred{pred_val, pred_fn};

    auto sb_r = get_snapshot_builder({path, strlen(path)}, engine);
    if (sb_r.tag != ExternResult<Handle<MutableFfiSnapshotBuilder>>::Tag::Ok) { fprintf(stderr, "get_snapshot_builder failed\n"); exit(1); }
    auto snap_r = snapshot_builder_build(sb_r.ok._0);
    if (snap_r.tag != ExternResult<Handle<SharedSnapshot>>::Tag::Ok) { fprintf(stderr, "snapshot_builder_build failed\n"); exit(1); }

    auto scan_r = scan(snap_r.ok._0, engine, &pred, nullptr);
    if (scan_r.tag != ExternResult<Handle<SharedScan>>::Tag::Ok) { fprintf(stderr, "scan failed\n"); exit(1); }

    auto iter_r = scan_metadata_iter_init(engine, scan_r.ok._0);
    if (iter_r.tag != ExternResult<Handle<SharedScanMetadataIterator>>::Tag::Ok) { fprintf(stderr, "scan_metadata_iter_init failed\n"); exit(1); }

    ScanCtx ctx{engine, 0};
    bool has_more = true;
    while (has_more) {
        auto r = scan_metadata_next(iter_r.ok._0, &ctx, on_batch);
        if (r.tag != ExternResult<bool>::Tag::Ok) { fprintf(stderr, "scan_metadata_next failed\n"); exit(1); }
        has_more = r.ok._0;
    }

    printf("files returned: %d\n", ctx.files);
    return ctx.files;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s <path-to-delta-table>\n", argv[0]);
        return 1;
    }
    const char *path = argv[1];
    printf("Opening table: %s\n", path);

    auto br = get_engine_builder({path, strlen(path)}, alloc_err);
    if (br.tag != ExternResult<EngineBuilder *>::Tag::Ok) { fprintf(stderr, "get_engine_builder failed\n"); return 1; }
    set_builder_with_multithreaded_executor(br.ok._0, 0, 0);
    auto er = builder_build(br.ok._0);
    if (er.tag != ExternResult<Handle<SharedExternEngine>>::Tag::Ok) { fprintf(stderr, "builder_build failed\n"); return 1; }
    Handle<SharedExternEngine> engine = er.ok._0;

    long val_42 = 42, val_31 = 31;
    int baseline = run_scan(engine, path, nullptr, "baseline (no predicate)");
    int match    = run_scan(engine, path, &val_42,  "part = 42 (should match all)");
    int nomatch  = run_scan(engine, path, &val_31,  "part = 31 (should match none)");

    printf("\n--- summary ---\n");
    printf("baseline:            %d files\n", baseline);
    printf("part=42 [expect %d]: %d\n", baseline, match);
    printf("part=31 [expect 0]:  %d\n", nomatch);

    if (match != baseline || nomatch != 0) {
        printf("FAIL\n");
        return 1;
    }
    printf("PASS\n");
    return 0;
}
