"""Phase 7 — Metrics Engine.

Ported from Portfolio Performance's Java reference implementation.
All calculations use scaled integers (Architecture Decision 3):
  - Monetary amounts: BIGINT cents (10^-2)
  - Shares/prices/FX: BIGINT 10^-8
"""
