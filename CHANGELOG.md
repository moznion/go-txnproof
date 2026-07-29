# Changelog

## [v0.2.0](https://github.com/moznion/go-txnproof/compare/v0.1.1...v0.2.0) - 2026-07-29

- Let allows pin the exact write-unit count they cover: supports `txnproof.AllowNonAtomic` and `allowlist.Add` by @moznion in https://github.com/moznion/go-txnproof/pull/12
- Let boundaries be marked non-atomic from the write that causes it: supports `AllowNonAtomicHere` by @moznion in https://github.com/moznion/go-txnproof/pull/14

## [v0.1.1](https://github.com/moznion/go-txnproof/compare/v0.1.0...v0.1.1) - 2026-07-26

- Add allocation benchmarks, regression guards, and a Performance section by @moznion in https://github.com/moznion/go-txnproof/pull/6
- Snapshot recorded statements only when reporting a violation by @moznion in https://github.com/moznion/go-txnproof/pull/8
- Reduce hot-path overheads: bounded throttle-key derivation and conditional outer-boundary lookup by @moznion in https://github.com/moznion/go-txnproof/pull/9
- Classify prepared statements once at Prepare instead of per execution by @moznion in https://github.com/moznion/go-txnproof/pull/10
- Add fuzzing for the parsers, the classifier and the detector by @moznion in https://github.com/moznion/go-txnproof/pull/11

## [v0.1.0](https://github.com/moznion/go-txnproof/compare/v0.0.1...v0.1.0) - 2026-07-23

### Breaking Change

- Reduce allocations to zero per statement and one per boundary by @moznion in https://github.com/moznion/go-txnproof/pull/4

## [v0.0.1](https://github.com/moznion/go-txnproof/commits/v0.0.1) - 2026-07-21

Initial Release.

- Tweak the way for package import by @moznion in https://github.com/moznion/go-txnproof/pull/1
- Activate Songmu/tagpr by @moznion in https://github.com/moznion/go-txnproof/pull/2
