# publishable-ql-crates

Make SignalDB's compatibility query languages standalone publishable parser
crates — lexing, parsing, and syntactic validation only, with lowering left
behind in the querier, and no SignalDB or FDAP dependency of any kind
(`thiserror` alone)
