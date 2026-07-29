# cio documentation

| document | what it is | status |
|---|---|---|
| [scheduler-v2.md](scheduler-v2.md) | Runtime-v2 design: ownership model, hot-path rules, queue contracts, reactor sharding, parking and the invariants that hold them together | **implemented** |
| [scheduler-results.md](scheduler-results.md) | The measurement record behind that design: retained results, every rejected variant, parameter sweeps and artifact hashes | evidence archive |
| [roadmap.md](roadmap.md) | Open evidence gaps, deferred runtime work, the staged I/O plan | current |
| [io-infrastructure.md](io-infrastructure.md) | Additive design for resolver, dialer, files, stream concepts, TLS and signals | **mostly implemented**; divergences flagged inline |

## Where to look

**Changing the runtime?** Read [scheduler-v2.md](scheduler-v2.md) for the
invariants and [AGENTS.md](../AGENTS.md) for the verification rules. Before
proposing a scheduler mechanism, check the rejected-designs table in
[scheduler-results.md](scheduler-results.md#rejected-designs) — more than
twenty implemented variants were already measured and removed.

**Making a performance claim?** The benchmark rules are in
[AGENTS.md](../AGENTS.md#benchmark-rules) and the harnesses are documented in
[bench/http-comparison](../bench/http-comparison/README.md). A run that is
valid is not automatically publishable; see the joint gate in
[scheduler-v2.md](scheduler-v2.md#gates-for-future-scheduler-work).

**Adding an I/O API?** [io-infrastructure.md](io-infrastructure.md) fixes the
public shape. Its stages are sequenced in [roadmap.md](roadmap.md).

**Using the library?** Start at the [project README](../README.md).

## Conventions

- A design document describes what is true of the implementation. Deferred work
  belongs in the roadmap, and measurements belong in the results archive — not
  inline in a design.
- Every retained performance number states its baseline, its direction and
  whether it is release evidence or an engineering screen.
- Raw benchmark run directories under `bench/*/results/` are local and
  untracked. Referencing one by name identifies a run on the benchmark host, not
  a file in this repository.
