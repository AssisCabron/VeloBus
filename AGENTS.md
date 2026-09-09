# Nodara project guidance

- Product focus: general backend APIs and microservices, with awaited request/reply, bounded concurrency and overload protection. Avoid vehicle-specific product examples.
- The user explicitly authorized committing and pushing each validated logical update to `origin/main` (`https://github.com/AssisCabron/VeloBus.git`). Keep that workflow for subsequent updates. Do not force-push or rewrite published history.
- Run checks appropriate to each change. Verify SDK + broker integration for wire-protocol changes. Describe limitations and actual measurements honestly.
- Commit source, lockfiles, tests and documentation. Keep credentials, local state, node_modules, target binaries and generated packages out of Git.
- The user explicitly authorized the initial npm publication of `nodara@0.3.0`, selected the name Nodara and the MIT license. Package: `nodara`, publishing account: `assiscabron`. Git pushes remain authorized.
- Cluster mode distributes RPC across independent brokers; do not claim replicated state, zero-loss failover, or exactly-once effects. Historical v0.2 benchmark records retain the original VeloBus name and binary metadata.
