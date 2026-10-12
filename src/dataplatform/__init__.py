"""Lakehouse platform — the three correctness guarantees, as code.

Each module owns a named set of guarantees and states them in its own
docstring, because the guarantee is the thing worth reviewing:

* `cdc_apply`    — applying database CDC to Iceberg safely under replay (G1–G4)
* `publish_gate` — deciding a dataset is complete, and refusing to publish
                   when it is not (G5–G7)
* `restatement`  — absorbing a correction without rewriting history (G8–G10)
* `sqlutil`      — the only sanctioned way to put a runtime value into SQL

Surrounding plumbing (Spark session construction, Kafka security, the schema
registry, alerting, the override approval workflow) is deliberately stubbed and
named as such at each call site.
"""

__all__ = ["cdc_apply", "publish_gate", "restatement", "sqlutil"]
