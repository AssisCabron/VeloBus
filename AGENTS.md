# VeloBus project guidance

- Product focus: general backend APIs and microservices, with awaited request/reply, bounded concurrency and overload protection. Avoid vehicle-specific product examples.
- The user explicitly authorized committing and pushing each validated logical update to `origin/main` (`https://github.com/AssisCabron/VeloBus.git`). Keep that workflow for subsequent updates. Do not force-push or rewrite published history.
- Run checks appropriate to each change. Verify SDK + broker integration for wire-protocol changes. Describe limitations and actual measurements honestly.
- Commit source, lockfiles, tests and documentation. Keep credentials, local state, node_modules, target binaries and generated packages out of Git.
- The npm package remains local/private until publication is explicitly requested. Git pushes are already authorized.
