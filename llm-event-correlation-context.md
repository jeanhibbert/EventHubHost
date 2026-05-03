# LLM Event Correlation Architecture — Copilot Context File

> Use this file as context in GitHub Copilot Chat (VS Code) by opening it and referencing it with `#file:llm-event-correlation-context.md` in your prompt, or by adding it to your Copilot workspace context.

---

## Project Overview

We are designing a **centrally hosted LLM-based event correlation system** that monitors events from multiple source systems and uses an LLM reasoning layer to detect correlation patterns between them.

This is an **event-driven architecture** with the LLM acting as an intelligent pattern detection engine, not a data store.

---

## Core Architecture: Event-Driven LLM Correlation Hub

### Design Principles
- Decouple event ingestion from LLM analysis
- LLM is a **reasoning layer** over windowed, normalised event batches
- Persist raw and enriched events for replay, audit, and future fine-tuning
- Use RAG (Retrieval-Augmented Generation) over a vector store of historical events to improve correlation quality
- Use traditional rule-based pre-filters to gate LLM invocation (cost/latency control)

---

## Data Flow

```
System A ──────────┐
                   ▼
              [Message Broker / Kafka / Event Hubs]
                   │
System B ──────────┘
                   │
                   ▼
         [Stream Processor / Flink / Stream Analytics]
         (normalise, deduplicate, window, pre-filter)
                   │
          ┌────────┴────────┐
          ▼                 ▼
    [Time-Series DB]   [Vector DB]
    (raw storage)      (embeddings + RAG)
          │                 │
          └────────┬────────┘
                   ▼
          [LLM Correlation Engine]
          (windowed prompt + RAG context injection)
                   │
                   ▼
    ┌──────────────┼──────────────┐
    ▼              ▼              ▼
[Alerts]     [Dashboard]      [API / DB]
```
