# EventHubHost as a Proposal for Cross-System Trading Correlation

> Companion document to [README.md](README.md), [docs/architecture.md](docs/architecture.md) and [docs/improvements.md](docs/improvements.md). This document is written for a non-developer audience — a steering committee, a head of trading technology, or an architecture review board — who need to evaluate whether the EventHubHost proof of concept is a credible starting point for correlating events across **independent, non-integrated trading systems**.

---

## 1. Executive summary

Most large trading organisations run a portfolio of systems — order management, execution, risk, settlement, market data, surveillance, post-trade reporting — that grew up independently. They share **no correlation key**, no transaction id, no trace id. When something goes wrong (a stuck order, a missed hedge, a regulatory breach, a client complaint) the investigation is manual: someone opens 4–7 UIs, eyeballs timestamps, and writes a narrative in a ticket.

EventHubHost demonstrates a working pattern for solving this *without* a multi-year integration programme:

1. Each system continues to emit events into its own store, unchanged.
2. A thin ingestion layer pushes those events into a **single SQL store of record** plus a **vector index** for semantic recall.
3. A small local LLM (Ollama `llama3.2:3b`) answers free-text questions over the combined dataset, with a **deterministic SQL-derived answer** always returned first and the LLM elaboration streamed token-by-token afterwards.
4. An anomaly detector (ML.NET SR-CNN) watches the per-minute event rate and proactively asks the LLM to explain spikes, broadcasting the result to the UI as a notification.
5. The whole stack runs locally under .NET Aspire — five containers, one `dotnet run`, no cloud dependency, no per-token cost.

The proof of concept is intentionally scoped to two synthetic systems with one hypothesis ("System A type 2 is followed by System B type 9002"), but **every architectural choice scales linearly** to twenty systems and N hypotheses. The grounded-answer guard, hybrid retrieval, insight memory, telemetry and golden-question harness are the pieces that turn a demo into an operational tool.

---

## 2. Honest assessment of the current solution

### 2.1 What this solution demonstrates well

- **Grounded RAG over heterogeneous systems is real and tractable.** The pipeline fuses two independent event streams through SQL + vector + LLM and produces an answer that is verifiably tied to the underlying data — exactly the problem trading firms have (siloed systems, no shared correlation key).
- **The grounded-answer guard is the most important idea here.** Forcing the LLM to include a deterministic evidence sentence (and rejecting forbidden words like "statistically significant", "correlation key", "proves") is the pattern that makes LLMs safe to put in front of corporate data. It is the difference between a demo and something an ops team would let near production.
- **Hybrid retrieval + insight memory** directly address the two failure modes that kill most corporate RAG pilots: irrelevant context and amnesia between sessions.
- **Streaming + GPU toggle + anomaly-triggered insights** turn the system from "ask and wait" into "the system tells you when something is interesting" — that is the demo that actually moves a stakeholder.
- **Telemetry + golden tests** are the unglamorous bits that decide whether an LLM feature survives its first quarter in production.

### 2.2 Where it is intentionally narrow

- Single hard-coded hypothesis (System A type 2 → System B type 9002). A real corporate deployment needs hypotheses to be data-driven or user-defined, not baked into a `CreateMatchedPairLines` helper.
- `llama3.2:3b` is the smallest useful model. It is great for showing the architecture works on a laptop; for real corporate Q&A you would swap in a 7B–14B model (or a hosted endpoint) with the exact same code path.
- Two source systems, simulated. Adding a third system is a config change, but the current prompt and evidence sentence assume the binary case.
- No authentication, no PII redaction, no row-level security on the SQL or vector layer. Those are mandatory before this touches real trading data.

### 2.3 Does it highlight LLM integration into multiple corporate systems?

Yes — and arguably better than most demos in this space, because it makes the integration story honest:

1. The LLM is **never the source of truth**. SQL is. The deterministic answer is computed and returned first; the LLM only elaborates.
2. Retrieval is **hybrid** (structured filter + semantic), which is what corporate data actually needs — pure vector search is a toy.
3. The system **closes the loop**: anomalies trigger insights, insights are remembered, and prompts are pinned by golden tests.
4. Aspire orchestrates **five heterogeneous components** (SQL, Qdrant, Ollama, API, Web) the way a real platform team would — containerised, observable, resilient — which is the pattern that scales from "two systems" to "twenty".

The framing to take to a steering committee is: *"the LLM is a presentation layer over a deterministic, observable correlation engine"*, **not** *"the LLM figured it out"*. That is the message corporate buyers respond to, and this solution supports it end-to-end.

---

## 3. The corporate problem in one picture

A typical mid-to-large trading firm has, at minimum:

| System | Owns | Typical event |
|---|---|---|
| Order Management (OMS) | Client orders, allocations | `OrderAccepted`, `OrderAmended`, `OrderCancelled` |
| Execution Management (EMS) | Routing, child orders | `ChildSent`, `ChildFilled`, `ChildRejected` |
| Market Data | Quotes, ticks | `QuoteUpdate`, `TradePrint` |
| Risk | Limits, exposure | `LimitBreached`, `LimitReleased` |
| Post-trade / Settlement | Confirms, fails | `Confirmed`, `SettlementFailed` |
| Surveillance | Alerts | `WashTradeAlert`, `LayeringAlert` |
| Client portal / CRM | Complaints, queries | `ClientComplaint`, `Callback` |

These systems were built in different decades by different vendors. They share **no key**. When a client calls at 14:32 saying "my order didn't fill", an analyst spends 20–60 minutes correlating timestamps across 4–7 UIs to build the timeline.

The same architecture EventHubHost demonstrates — push events into a unified SQL + vector store, ask grounded questions over the combined dataset — collapses that 20–60 minutes into a single question with a verifiable answer.

---

## 4. How each EventHubHost capability maps to a real trading workflow

| EventHubHost capability | Trading-floor workflow it unlocks |
|---|---|
| **SQL store of record** with idempotent ingestion | Permanent, query-able audit trail across systems — satisfies MiFID II / SEC 17a-4 record-keeping requirements without modifying upstream systems. |
| **Grounded-answer guard** (evidence sentence + forbidden words) | Compliance-safe natural-language Q&A. The LLM cannot say "statistically significant" or invent a correlation key; every claim cites a SQL-computed fact. |
| **Hybrid retrieval** (entity filter + vector) | "Show me all orders for client X on instrument Y between 14:00 and 15:00" — filters on structured fields, ranks by semantic similarity. The exact pattern needed for trade-reconstruction queries. |
| **Insight memory** (second Qdrant collection) | Institutional memory: "have we seen this kind of incident before?" Past Q&A becomes a searchable knowledge base, not a stack of Jira tickets. |
| **Streaming LLM answers** | Analysts get the deterministic timeline instantly and the narrative explanation as it is generated — no 30 s blank screen. Crucial for trading-floor UX. |
| **ML.NET SR-CNN anomaly detector** | Proactive surveillance: "the rate of cancellations on Venue A just spiked, here is the LLM's explanation using the surrounding events". A starting point for layering / spoofing detection. |
| **OpenTelemetry spans** (`ActivitySource`) | Every LLM call, every embedding, every vector search is a span in the Aspire dashboard — feeds straight into Grafana / Datadog / Dynatrace. Mandatory for production LLM operations. |
| **Golden-question harness** | Every prompt change is gated by a CI test asserting "the answer still cites the SQL evidence sentence and does not use forbidden words". Stops silent prompt regressions — the #1 cause of LLM features being switched off in production. |
| **GPU feature toggle** | Same code on a laptop (CPU, slow) and on a GPU host (production, fast). No code branches. |

---

## 5. Additional insights and opportunities to highlight in the proposal

### 5.1 Strategic — *why this approach beats the alternatives*

1. **No upstream changes.** Every existing system continues to emit what it already emits. The integration cost is **one adapter per system**, not a re-platforming programme. This is the single biggest reason this pattern survives a CIO review.
2. **Buy-vs-build inversion.** Vendor "cross-system observability" platforms (Splunk, Datadog, ITRS, custom CEP) cost £200k–£2M/year and still need the LLM bolted on top. EventHubHost's stack — SQL Server, Qdrant, Ollama, .NET Aspire — is **zero licence cost** and runs on existing infrastructure.
3. **Local LLM = data sovereignty.** Trading data cannot leave the bank's network. Ollama runs the model locally; **no token leaves the perimeter**. This is the slide that closes the deal with InfoSec and the DPO.
4. **Deterministic floor.** Because the SQL answer is always computed first, the system has a **non-LLM fallback** for every question. If the LLM is down, slow, or hallucinating, the user still gets a correct answer. This satisfies the "explainability" and "human-in-the-loop" requirements of model risk management (SR 11-7, PRA SS1/23).
5. **Composable, not monolithic.** Each piece (Qdrant, Ollama, SQL, anomaly worker) can be replaced independently. The day a regulator mandates an on-prem GPT-4-class model, you swap the Ollama URL and nothing else changes.

### 5.2 Tactical — *concrete trading use cases this PoC unlocks within one quarter*

1. **Trade reconstruction on demand.** "Reconstruct everything that happened to order 12345 between OMS, EMS and settlement." Today: 30 minutes of manual work. With this pattern: 5 seconds, with a cited evidence trail.
2. **Client-complaint triage.** "Client says their order didn't fill at 14:32 — what actually happened?" The LLM produces a plain-English timeline citing OMS + EMS + market-data events.
3. **Break investigation.** "Why did settlement fail for trade T?" Hybrid retrieval pulls the matching OMS + confirm + settlement events; the LLM explains the discrepancy.
4. **Anomaly-triggered surveillance assistance.** SR-CNN flags an unusual cancellation burst; the LLM drafts a paragraph the surveillance officer can paste into a STOR (suspicious transaction) report — they still decide whether to file.
5. **"What changed?" RCA.** "We had a P&L spike at 11:03 — what events around that time look unusual?" Anomaly detector + LLM elaboration.
6. **Operational runbook automation.** New joiners ask the system "what does a SettlementFailed on bond instrument with venue X usually mean?" — insight memory surfaces the last 5 times it happened and how it was resolved.
7. **Regulatory query response.** When the regulator asks "show us every event related to this trade", the answer is a single grounded query, not a week of forensics.

### 5.3 Operational — *what would have to be added before go-live*

These are **scoping** items, not blockers — each is small relative to the value:

| Area | What to add | Why |
|---|---|---|
| Authentication | OIDC / Entra ID on the API and the Blazor UI; per-desk row-level security on `Events` and `Insights` | A FX trader must not see Equities flow. |
| PII / MNPI redaction | A pre-embed scrubber on the ingestion path (regex + named-entity removal) | Client names, account numbers and order intent must not be embedded into the vector store unredacted. |
| Audit trail | Append-only log of every Ask + every Answer + the user who asked | Required by model risk management and by surveillance review. |
| Model risk governance | Document the model, the prompt, the guard, the golden tests; sign off under SR 11-7 / PRA SS1/23 | Mandatory for any LLM that informs a trading or compliance decision. |
| HA / DR | SQL Always On; Qdrant snapshots; Ollama replicas behind a load balancer | Trading hours = zero downtime. |
| Capacity | Move from `llama3.2:3b` to a 7B–14B model (e.g. `llama3.1:8b`, `qwen2.5:14b`) on a single-GPU host; benchmark against golden questions | The architecture does not change; only the model name in `appsettings.json`. |
| Connector framework | Replace `EventSimulationWorker` with one adapter per real system (Kafka, MQ, REST, FIX-drop-copy) | The shape of `CorrelationEvent` already supports any source. |
| Hypothesis catalogue | Move the hard-coded "A type 2 → B type 9002" rule into a configurable rule set | Real desks have dozens of hypotheses; each becomes a row in a `Hypotheses` table with its own SR-CNN binning + LLM prompt template. |
| FinOps | Track tokens-per-question and seconds-per-question via the existing OTel spans; alert on regression | LLM cost only stays low if it is measured. |

### 5.4 Risks and how this proof of concept already mitigates them

| Risk a CIO will raise | How the architecture answers it |
|---|---|
| "The LLM will hallucinate and our traders will act on it." | Deterministic SQL answer is returned first and is always present; the grounded-answer guard rejects answers that omit it. |
| "Our data cannot leave the bank." | Ollama runs locally inside the Aspire perimeter. No external API call. |
| "We cannot afford a multi-year integration programme." | Upstream systems are unchanged. Each new system = one adapter. |
| "How will we know when the model regresses?" | Golden-question harness in CI; OTel spans on every LLM call surfaced in the Aspire dashboard. |
| "Who is accountable for what the LLM says?" | The LLM is positioned as a *presentation layer* over a deterministic engine. Every claim has a SQL-cited evidence sentence; the analyst remains accountable. |
| "What happens when the LLM is down?" | The deterministic SQL answer is still produced and returned. The user is told the LLM is unavailable; nothing else breaks. |
| "What about audit?" | Every Q&A is persisted in the `Insights` SQL table with timestamp, user, vector match count, temporal window evidence. |

### 5.5 The 90-day roadmap to take this from PoC to pilot

- **Days 0–30 — Connect one read-only feed.** Pick the lowest-risk system (e.g. drop-copy from EMS). Build one adapter that publishes its events into `EventRepository.AddAsync`. Keep the simulator running alongside so the UI continues to demo well. Run the existing golden tests against the live data.
- **Days 30–60 — Add the second system + hybrid hypotheses.** Connect OMS. Replace the hard-coded type-2/9002 hypothesis with a `Hypotheses` table seeded with 3–5 real desk-defined rules. Re-train the SR-CNN anomaly thresholds against real per-minute event rates. Stand up the larger model on a single GPU host.
- **Days 60–90 — Compliance hardening.** OIDC auth; row-level security; PII scrubber on the embed path; SR 11-7 / PRA SS1/23 model documentation pack. Pilot with a single desk for two weeks. Measure: questions asked per analyst per day, average time-to-answer vs. baseline, count of insights re-used from memory.
- **Decision gate.** Did the pilot desk save measurable time? Did the model risk committee sign off? If yes, expand to a second desk. If no, the only sunk cost is the connectors — the architecture itself remains useful as a generic event-correlation store.

---

## 6. The one-slide pitch

> *Most of our trading systems were never designed to talk to each other. EventHubHost shows that we do not have to make them.*
> *We can leave them untouched, copy their events into a single grounded store, and put a small local language model in front of it that turns 30-minute investigations into 5-second questions — without sending a single byte of trading data outside the bank, and without the LLM ever being trusted as the source of truth.*
> *The proof of concept runs today on a laptop. The same code runs on a GPU host with a larger model. Adding a system means writing one adapter; adding a hypothesis means writing one row.*
