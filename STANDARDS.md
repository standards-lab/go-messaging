# go-messaging standards

The judgement calls the standards-reviewer applies to go-messaging, beyond what `mise run check` enforces.

- A change that alters documented behavior updates the README and the affected `doc.go` in the same change.
- `architecture/standards/go-elemental/principles/dependencies.md`: the bottom-up line and no provider in a base: the base module on the standard library and go-core alone, the engine and provider dependencies only in their sub-modules.
- `architecture/standards/go-elemental/principles/topology-and-naming.md`: the base module at the root, and the `postgres` and `nats` sub-modules and their tags.
- `architecture/standards/go-elemental/principles/tests-and-docs.md`: the doc.go inventory of each package, and `messagingtest` as the conformance suite every provider runs.
- `architecture/standards/go-elemental/principles/release-and-ci.md`: the check, currency, and a changelog per module.
- `architecture/standards/go-elemental/principles/lifecycle-and-context.md`: the runtime and providers as go-core lifecycle values, and their drain.
- `architecture/principles/service-tiers.md`: the standard-tier broker contract and the providers beneath it.
- `architecture/principles/context-architecture.md`: the README and `doc.go` are the homes.
