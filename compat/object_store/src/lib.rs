//! Compatibility harness for the Apache Arrow `object_store` crate against
//! rusts3. All logic lives in `tests/compat.rs`; this crate has no runtime code
//! — the empty lib just gives Cargo a target to hang the integration tests on.
//!
//! Run the suite with `./run.sh`, which starts a throwaway rusts3, creates the
//! test bucket, and points the tests at it via environment variables.
