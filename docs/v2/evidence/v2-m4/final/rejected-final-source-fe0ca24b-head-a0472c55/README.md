# Rejected M4 Final publication

Status: `NON_PROMOTABLE`.

The adjacent `m4-final.json` preserves, byte for byte, the Final candidate generated from exact tested source
`fe0ca24bbb01f2db5320bd8e001b3e0820fd95dc` and published at evidence head
`6a9225efc7cfcba279a6d2efe78d2f9971b53f2f`. Its SHA-256 is
`83b3f64edf7d3e4402ed11e6b2d4bb6aaf014e2aec9002448bf1318e14317603`.

The candidate's own fail-closed validator passed, but aggregate `v2M4Check` failed at publication commit
`a0472c55c6e4d67d4241b5c65b2e5d09d17fbada`: the historical design-contract fixture depended on the live scenario
state, and `v2M4FinalSourceCheck` was not compatible with Gradle configuration-cache serialization. Because the
aggregate gate did not pass, this preserved candidate is not canonical, must not be rebound to a scenario, and cannot
authorize M4 promotion. A later tested source must execute fresh formal evidence and publish a new canonical Final.
