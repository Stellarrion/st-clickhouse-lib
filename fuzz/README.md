# Fuzz Targets

Run with `cargo fuzz` from the repository root:

```bash
cargo fuzz run varint
cargo fuzz run block
cargo fuzz run exception
cargo fuzz run ssh
cargo fuzz run chunked
cargo fuzz run coordinator
```

Targets cover wire varints/strings, block-ish packet shapes, recursive
exception chains, SSH challenge payload construction, chunked-frame
normalization, and distributed/coordinator packet entry points.
