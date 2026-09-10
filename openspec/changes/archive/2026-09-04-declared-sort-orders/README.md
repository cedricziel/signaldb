# declared-sort-orders

Declare a time-leading Iceberg sort order on every signal table, have every
file producer sort by it and attest it per file, and let the querier trust
the attestation to elide redundant sorts — with the benchmark gate that
measured where that pays off (ascending reads) and where DataFusion 54 does
not yet let it (recent-first `DESC` TopK)
