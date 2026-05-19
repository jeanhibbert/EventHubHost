# EventHubHost — improvements over the baseline

The baseline implementation (see [README.md](../README.md) and [docs/architecture.md](architecture.md)) ships a working SQL-grounded RAG pipeline: events are persisted to SQL Server, embedded with `nomic-embed-text`, indexed in Qdrant, and an Ollama `llama3.1:8b` model is asked a question with a grounded-answer guard. That works, but it has the well-known flaws of any local-model RAG demo:

- The user waits 20–60 s with no feedback before any answer appears.
- Pure semantic retrieval drags in irrelevant events when the question explicitly mentions a system or event type.
- The system never learns from earlier Q&A pairs.
- There is no automated way to surface that "something just changed" without the user asking.
- Tracing of the LLM pipeline relies on log scraping.
- A new failure mode (e.g. the model starts hallucinating) cannot be caught in CI.
- On a GPU host, Ollama still runs on CPU because the Aspire container is started without the NVIDIA runtime flags.

This document describes the seven improvements implemented on top of the baseline and how to enable each.

---

## 1. Streaming LLM answers via SignalR

**Problem.** A single synchronous `POST /correlations/query` blocks until the full LLM response is generated, which on `llama3.1:8b` running on CPU can take long enough that the user sees nothing during that time.

**Fix.** A new endpoint `POST /correlations/query/stream` is added that:

1. Computes the deterministic, SQL-derived answer immediately (the same evidence sentence the grounded-answer guard already enforces) and returns it in the HTTP response together with a `StreamId`.
2. Fires a background `Task.Run` that calls Ollama's `/api/generate` with `stream=true`. The NDJSON response is parsed line-by-line; each `response` chunk is broadcast over the `EventIngestionHub` as `InsightTokenAppended` (`StreamId`, `TokenDelta`).
3. When the stream ends, the verified final answer is broadcast as `InsightAnswerCompleted` (`StreamId`, `FinalAnswer`, `UsedLlm`).

**UI.** `Home.razor` now shows two cards: the deterministic answer (instant) and the LLM elaboration (streaming…). The original `POST /correlations/query` synchronous endpoint is preserved for the test harness and for clients that prefer a single blocking call.

**Files.** `EventHubHost.ApiService/Program.cs`, `EventHubHost.ApiService/CorrelationServices.cs` (`BeginStreamingAskAsync`, `StreamLlmElaborationAsync`, `GenerateStreamAsync`), `EventHubHost.ApiService/Hubs/EventIngestionHub.cs`, `EventHubHost.Web/CorrelationApiClient.cs` (`AskStreamAsync`), `EventHubHost.Web/Components/Pages/Home.razor`.

---

## 2. ML.NET anomaly-triggered insights

**Problem.** The baseline only answers when the user asks. Operationally, the more interesting case is "the system noticed something — here is what we think happened".

**Fix.** A new `AnomalyDetectionWorker` runs on a 30 s cadence (configurable). Every cycle it:

1. Queries the last 5 minutes of events from SQL.
2. Bins them into fixed-width buckets (default 5 s) over the entire window.
3. Runs `Microsoft.ML.AnomalyDetection.DetectEntireAnomalyBySrCnn` with `SrCnnDetectMode.AnomalyAndMargin` to obtain `expectedValue` per bucket.
4. If an anomaly fires in the most recent bucket, finds the dominant `SourceSystem` / `EventType` for that bucket and asks the LLM to explain the spike (re-using the existing grounded-answer guard).
5. Broadcasts the result over SignalR as `AnomalyDetected` so the UI can show it as a notification.

**Disable.** Set `Anomaly:Enabled = false` in `EventHubHost.ApiService/appsettings.json`.

**Tuning.** `Anomaly:WindowSeconds`, `Anomaly:BinSeconds`, `Anomaly:MinPointsToScan`, `Anomaly:Sensitivity` (90.0 by default; higher = more aggressive).

**Files.** `EventHubHost.ApiService/AnomalyDetectionWorker.cs`, `EventHubHost.ApiService/EventHubHost.ApiService.csproj` (added `Microsoft.ML` and `Microsoft.ML.TimeSeries` 4.0.2 packages), `EventHubHost.Web/Components/Pages/Home.razor` (anomaly panel).

---

## 3. Hybrid retrieval (entity-filtered vector search)

**Problem.** "Show me System A type 2 events that triggered System B" is a question with explicit constraints, but a pure vector search will happily return the top-K events by cosine similarity regardless of `SourceSystem` or `EventType`. The model then has to reason its way out of irrelevant context, which it often does poorly.

**Fix.** A small regex-based `EntityExtractor` extracts mentions of `System A`/`System B` and event-type numbers (`type 2`, `type 9002`, `9002`, …) from the question and produces a `QuestionEntities` record. `QdrantEventVectorStore.SearchAsync` is overloaded to accept this and post a Qdrant `filter.must` payload alongside the vector query, so the cosine search runs only against payload-matching points. When no entities are extracted, behaviour is identical to the baseline pure vector search.

**Files.** `EventHubHost.ApiService/EntityExtractor.cs`, `EventHubHost.ApiService/CorrelationServices.cs` (`QdrantEventVectorStore.SearchAsync` overload).

---

## 4. Insight memory (second Qdrant collection)

**Problem.** Every Ask call starts from scratch. If the user asks the same question (or a closely related one) twice, the model re-derives the answer with no awareness of the prior reasoning.

**Fix.** A second Qdrant collection (`insights`, configurable via `Qdrant:InsightCollection`) is created. Every time `InsightRepository.AddAsync` persists a new insight to SQL it also embeds `Question + " | " + Answer` and upserts the vector. During `OllamaCorrelationClient.AskAsync` (and the streaming variant), the question embedding is searched against the insight collection for top-3 prior matches; those are injected into the prompt under a "Past related questions" section. The injection is opportunistic — failure of either embed or search is non-fatal.

**Files.** `EventHubHost.ApiService/QdrantInsightVectorStore.cs`, `EventHubHost.ApiService/CorrelationServices.cs` (`InsightRepository.AddAsync`, `RetrieveContextAsync`, `CreatePrompt`).

---

## 5. Golden-question evaluation harness

**Problem.** It is easy for a prompt change to silently regress: the answer still "looks" reasonable but no longer contains the evidence sentence the grounded-answer guard requires, or starts using forbidden words like "statistically significant". Without an automated check this is only ever caught by manual eyeballing.

**Fix.** A new parameterized xUnit theory in `EventHubHost.Tests/GoldenQuestionTests.cs` reuses the existing Aspire fixture pattern. For each (question, expectedSubstring, forbiddenWords) tuple it:

1. Triggers the Type-2 scenario to guarantee a grounded matched-event row exists.
2. Posts the question to `/correlations/query`.
3. Asserts the deterministic evidence sentence is present and the forbidden words are absent.

The test is `[Trait("Category", "Live")]` so CI can opt out by category; locally it runs against the same Aspire orchestration as the existing scenario test.

**Files.** `EventHubHost.Tests/GoldenQuestionTests.cs`.

---

## 6. Telemetry via `ActivitySource`

**Problem.** When an Ask call takes 50 s, you cannot tell whether the bottleneck is the embed call, the vector search, the LLM generate, or the verify step without reading logs.

**Fix.** A static `CorrelationTelemetry.Source = new ActivitySource("EventHubHost.Correlation")` is added and used to wrap key operations:

- `ingest.event` (in `EventRepository.AddAsync`)
- `llm.ask`, `llm.ask.streaming`, `llm.embed.question`, `llm.generate`, `llm.generate.stream`
- `vector.search.events`, `vector.search.insights`
- `anomaly.scan`

Span tags include the model name, prompt length, vector match count, filter contents, and stream id. The source is registered with the OpenTelemetry tracer in `Program.cs` via `ConfigureOpenTelemetryTracerProvider(tracing => tracing.AddSource(CorrelationTelemetry.SourceName))`, which means spans are exported through the existing Aspire OTLP pipeline and visible in the Aspire dashboard's **Traces** tab.

**Files.** `EventHubHost.ApiService/Telemetry.cs`, `EventHubHost.ApiService/Program.cs`, `EventHubHost.ApiService/CorrelationServices.cs`.

---

## 7. GPU on Ollama (feature toggle)

**Problem.** Even when running on a GPU host, the Ollama model is fully CPU-bound because the Aspire `ollama/ollama` container is started without the NVIDIA runtime flags. On a 4-core CPU this can dominate Ask latency.

**Fix.** A new feature toggle `Llm:UseGpu` is read in `EventHubHost.AppHost/AppHost.cs`. When `true`, the `llm` container is augmented with `.WithContainerRuntimeArgs("--gpus=all")`, which Aspire passes through to `docker run`. When `false` (the default), behaviour is unchanged — safe for laptops and CI machines without the NVIDIA Container Toolkit.

**Enable.** Set in `EventHubHost.AppHost/appsettings.json`:

```jsonc
{
  "Llm": {
    "UseGpu": true
  }
}
```

Or via environment variable when launching AppHost: `Llm__UseGpu=true`.

**Prerequisites.** Docker Desktop with WSL2 + NVIDIA Container Toolkit installed on Linux; on Windows, Docker Desktop with GPU support enabled. If the toggle is on but the host has no GPU support, `docker run` will fail and the `llm` container will not start — Aspire will surface the error in the dashboard.

**Expected impact.** First-token latency typically drops from 5–10 s to under 1 s; full-answer latency typically drops from 20–60 s to 2–6 s on consumer GPUs (RTX 3060+).

**Files.** `EventHubHost.AppHost/AppHost.cs`, `EventHubHost.AppHost/appsettings.json`.

---

## Recommended ordering when re-applying these to a fresh checkout

1. GPU toggle (#7) — biggest single latency win on GPU hosts.
2. Streaming (#1) — biggest perceived-latency win on CPU hosts.
3. Telemetry (#6) — needed to measure the impact of any other change.
4. Hybrid retrieval (#3) and insight memory (#4) — additive precision wins.
5. Anomaly worker (#2) — largest new code surface; turn on last.
6. Golden-question harness (#5) — pin the prompt behaviour so future changes do not regress.
