# typed-wire-format-wal

**BREAKING. Planning only, not scheduled for implementation.** Carries every
OTLP `AnyValue` on the acceptor → writer Flight wire and in both WALs as its
own OTLP protobuf encoding instead of JSON text. Duplicate keys, key order,
non-finite doubles, bytes and arbitrarily nested structure then survive to
the storage boundary, and a duplicate-aware residue keeps them retrievable.
This is layer 12.2 of the `otel-native-schema` stack.
