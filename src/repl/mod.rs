//! Leader → replica replication (asynchronous, like Redis' master/replica).
//!
//! - `backlog`: the ordered stream of encoded write records, addressed by byte offset.
//!
//! The full flow is described in docs/REPLICATION.md.

pub mod backlog;
