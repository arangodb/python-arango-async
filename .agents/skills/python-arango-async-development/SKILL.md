---
name: python-arango-async-development
description: >-
  Maintain and extend python-arango-async, the asyncio ArangoDB Python driver.
  Use for driver features, bug fixes, refactoring, and related tests or
  documentation, rather than application code that uses the driver.
---

# Async Python driver development

Follow the repository's [AGENTS.md](../../../AGENTS.md) for setup, validation
commands, and contribution rules.

Before changing an operation, trace a neighboring method with a similar
request and response shape. Public API methods usually construct a `Request`,
define a synchronous response handler, and call
`await self._executor.execute(request, response_handler)`.
See `arangoasync/request.py`, `arangoasync/executor.py`, and the API group
being changed. Keep request construction and response conversion at the same
layer as neighboring operations.

Preserve executor dispatch. It supports default execution, transactions, and
ArangoDB background jobs. Calling the connection or HTTP client directly can
bypass transaction headers and job handling. Preserve `Result[T]` annotations
where applicable: after awaiting an operation, its result may be `T`,
`AsyncJob[T]`, or `None`, depending on the execution context.

Distinguish asyncio I/O from ArangoDB asynchronous API execution.
`begin_async_execution()` selects server-side background jobs; it does not
make an otherwise synchronous driver asynchronous. Follow existing boundaries
between synchronous wrapper factories and awaited operations rather than
adding `async` or `await` indiscriminately.

For endpoint or option changes, verify the server HTTP API for the targeted
ArangoDB versions. Map Python option names to the specified wire names and
locations: endpoint path, `params`, `headers`, or `data`. Preserve existing
defaults and distinguish omitted options from explicit `False`, `0`, or empty
values. Where `None` means omission, use `is not None`. `Request.normalized_params()`
stringifies supplied values, including `None`; omit such query entries before
constructing the request. Keep explicit JSON nulls where the body requires them.

Preserve typed option and response wrappers in `arangoasync/typings.py`.
Follow neighboring uses of `JsonWrapper`, `.to_dict()`, validation, and typed
properties rather than replacing wrappers with plain dictionaries. Keep public
signatures, annotations, Sphinx docstrings, and response shapes aligned.

Respect configurable serialization. Inspect whether an operation uses the
executor's serializer/deserializer for API payloads and metadata, or the
collection's document serializer/deserializer for user documents. Preserve
`Collection[T, U, V]` contracts, including distinct single-document and
multiple-document deserialization results. Do not replace these hooks with
direct JSON calls or assume custom documents are dictionaries.
See `arangoasync/serialization.py` and `docs/serialization.rst`.

Follow neighboring handlers' success checks and special-status handling.
Preserve revision-conflict exceptions, missing-resource options, and silent
results. Raise operation-specific exceptions from `arangoasync/exceptions.py`
with `(resp, request)` to retain HTTP and ArangoDB error details.

For client, transport, cursor, or job changes, preserve awaited cleanup and
existing async context-manager behavior. Avoid blocking I/O or sleeps in
coroutines. Keep transport changes compatible with the `HTTPClient` abstraction
in `arangoasync/http.py`. Inspect connection and resolver behavior before changing
authentication, host failover, or retries.

Cursors use async iteration and retain mutable batch state. Preserve batch
boundaries, exhaustion behavior, and server-resource cleanup. Cursors obtained
from background jobs fetch subsequent batches through a non-job executor;
transaction cursors must retain their transaction context.
See `arangoasync/aql.py`, `arangoasync/cursor.py`, and `arangoasync/job.py`.

Add focused regression coverage in the relevant `tests/test_*.py` module,
reusing fixtures and helpers from `tests/conftest.py` and `tests/helpers.py`.
Follow existing `pytest.mark.asyncio` and `pytest_asyncio.fixture` patterns.
Cover affected request mapping, result conversion, and error behavior;
exercise related execution contexts when the change affects them.

Pytest normally contacts ArangoDB during configuration, even for database-free
tests or collection. Use `--skip-arango-setup` only for tests whose fixture
dependencies need no server, for example:

    python -m pytest tests/test_imports.py tests/test_typings.py --skip-arango-setup

This flag bypasses setup; it does not make integration tests run offline.
For server behavior, use a disposable instance and the commands in AGENTS.md.
Pass `--cluster` only for an actual cluster; the flag does not start one.
Check version and capability skips: skipped cases do not validate the change.
Start with affected tests, expand for changed contracts, and report unverified
behavior.
