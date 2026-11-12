# Visual Architecture Blueprints

Both diagrams are stored as standalone Mermaid.js sources in `diagrams/` and are validated by `tests/diagram_validation/parse.mjs` with Mermaid 12.3.0.

## System Architecture Diagram

```mermaid
flowchart LR
  classDef client fill:#e0f2fe,stroke:#0369a1,color:#0c4a6e;
  classDef edge fill:#fef3c7,stroke:#b45309,color:#7c2d12;
  classDef isolation fill:#ede9fe,stroke:#6d28d9,color:#4c1d95;
  classDef service fill:#dcfce7,stroke:#15803d,color:#14532d;
  classDef store fill:#fee2e2,stroke:#b91c1c,color:#7f1d1d;
  classDef bus fill:#ffedd5,stroke:#c2410c,color:#7c2d12;

  subgraph C["Client-facing API segmentation"]
    direction TB
    OMS["B2C OMS<br/>storefront + OMS"]
    ERP["B2B ERP<br/>EDI + ASN"]
    PORTAL["3PL client portal<br/>inventory + invoice views"]
    DEVICES["RF / AMR / forklift<br/>task and telemetry APIs"]
  end

  subgraph E["Edge and tenant resolution"]
    direction TB
    GW["API Gateway<br/>OIDC, mTLS, scopes, CIDR,<br/>rate limit, idempotency key"]
    CTX["TenantContext<br/>X-Tenant-ID + X-Client-ID<br/>app.current_tenant_id"]
  end

  subgraph I["3PL isolation layers"]
    direction TB
    RLS["PostgreSQL RLS<br/>forced tenant_id policies"]
    WF["Workflow registry<br/>tenant-specific picking rules"]
    CONTRACTS["Tenant contracts<br/>rate cards + VAS catalog"]
    SILO["Siloed inventory<br/>tenant, lot, serial, expiry, status"]
    CLIENTS["tenant_api_clients<br/>scopes, modes, CIDRs"]
  end

  subgraph S["Orchestrator microservices"]
    direction TB
    ROUTE["Order intake + hybrid routing"]
    WAVE["Wave + affinity planner"]
    ALLOC["Allocation + inventory engine"]
    TASKS["Task dispatcher + labour queue"]
    TWIN["Digital twin exception engine"]
    PUTAWAY["Directed putaway + cross-dock"]
    BILLING["Billing + VAS engine"]
    RELAY["Transactional outbox relay"]
  end

  subgraph D["State and messaging"]
    direction TB
    PG[("PostgreSQL 16+<br/>wms schema, partitions,<br/>RLS, ledger, outbox")]
    REDIS[("Redis<br/>worker task pushes")]
    KAFKA[("Kafka event bus<br/>wms.* topics")]
  end

  OMS --> GW
  ERP --> GW
  PORTAL --> GW
  DEVICES --> GW
  GW --> CTX
  CTX --> ROUTE
  CTX --> PUTAWAY
  CTX --> TWIN
  CTX --> BILLING
  CLIENTS --> GW
  RLS --> PG
  WF --> ROUTE
  WF --> WAVE
  CONTRACTS --> BILLING
  SILO --> ALLOC
  ROUTE --> WAVE
  WAVE --> ALLOC
  ALLOC --> TASKS
  ROUTE --> PUTAWAY
  TWIN --> ALLOC
  TWIN --> TASKS
  BILLING --> PG
  ROUTE --> PG
  WAVE --> PG
  ALLOC --> PG
  TASKS --> PG
  TWIN --> PG
  PUTAWAY --> PG
  RELAY --> PG
  TASKS --> REDIS
  REDIS --> DEVICES
  RELAY --> KAFKA
  KAFKA --> TWIN
  KAFKA --> BILLING

  class OMS,ERP,PORTAL,DEVICES client;
  class GW,CTX edge;
  class RLS,WF,CONTRACTS,SILO,CLIENTS isolation;
  class ROUTE,WAVE,ALLOC,TASKS,TWIN,PUTAWAY,BILLING,RELAY service;
  class PG,REDIS store;
  class KAFKA bus;
```

## Hybrid Order Fulfillment Workflow

```mermaid
flowchart TD
  classDef ingress fill:#e0f2fe,stroke:#0369a1,color:#0c4a6e;
  classDef decision fill:#fef3c7,stroke:#b45309,color:#7c2d12;
  classDef b2c fill:#dcfce7,stroke:#15803d,color:#14532d;
  classDef b2b fill:#ede9fe,stroke:#6d28d9,color:#4c1d95;
  classDef exception fill:#fee2e2,stroke:#b91c1c,color:#7f1d1d;
  classDef billing fill:#ffedd5,stroke:#c2410c,color:#7c2d12;
  classDef terminal fill:#f1f5f9,stroke:#334155,color:#0f172a;

  START(["Order / API request"])
  GW["Gateway: idempotency,<br/>tenant scope, CIDR, rate limit"]
  PERSIST["Persist order + lines<br/>validate mode and pallet multiples"]
  WORKFLOW["Load tenant workflow<br/>CLUSTER / WAVE / ZONE / MULTI_BATCH"]
  MODE{"Order mode?"}
  START --> GW --> PERSIST --> WORKFLOW --> MODE

  subgraph B2CPATH["B2C piece-pick path"]
    direction TB
    CAND["Read inventory_available<br/>by tenant, SKU, lot, expiry"]
    PLAN["plan_line_stops<br/>FEFO + same-lot + 3D travel"]
    ROUTE["build_route<br/>nearest-neighbour from dock"]
    AFFINITY{"Affinity score >= threshold<br/>and capacity / SLA fit?"}
    OPENWAVE["Attach to open wave"]
    NEWWAVE["Create wave<br/>cutoff bucket + priority"]
    ALLOC["allocate_line in SAVEPOINT<br/>FOR UPDATE SKIP LOCKED<br/>+ version checks"]
    REBUILD["Rebuild wave_route_stops"]
    PICKTSK["Create PICK_CLUSTER /<br/>PICK_ZONE / PICK_MULTI_BATCH"]
    ASSIGN["Assign by zone, equipment,<br/>heartbeat, load, priority"]
    RF["RF scanner / cart / AMR"]
    CAND --> PLAN --> ROUTE --> AFFINITY
    AFFINITY -->|yes| OPENWAVE
    AFFINITY -->|no| NEWWAVE
    OPENWAVE --> ALLOC
    NEWWAVE --> ALLOC
    ALLOC --> REBUILD --> PICKTSK --> ASSIGN --> RF
  end

  subgraph B2BPATH["B2B bulk pallet path"]
    direction TB
    PALLETQTY{"Exact full-pallet multiple?"}
    REJECT["Reject line<br/>ValidationError"]
    SKIP["Skip piece-pick queue<br/>wave_id = NULL"]
    FULLPALLET["LOCK_FULL_PALLET<br/>EXACT / SINGLE_LOT / IGNORE"]
    SHORT{"Enough full pallets?"}
    BACKORDER["Backorder line<br/>REPLENISHMENT task"]
    RESERVE["Reserve full handling units<br/>available to allocated"]
    PALLETTASK["Directed Pallet Pick<br/>PALLET_PICK, forklift equipment"]
    FORKLIFT["Forklift / reach truck / AGV"]
    PALLETQTY -->|no| REJECT
    PALLETQTY -->|yes| SKIP --> FULLPALLET --> SHORT
    SHORT -->|no| BACKORDER
    SHORT -->|yes| RESERVE --> PALLETTASK --> FORKLIFT
  end

  MODE -->|B2C| CAND
  MODE -->|B2B| PALLETQTY
  MODE -->|HYBRID| CAND
  MODE -->|HYBRID| PALLETQTY

  INBOUND(["Inbound pallet"])
  PUT["Directed putaway / cross-dock<br/>PALLET_PUTAWAY / CROSS_DOCK_MOVE"]
  INBOUND --> PUT --> FORKLIFT

  DISC(["AMR / RF discrepancy<br/>expected 10, found 0"])
  QUARANTINE["Version-checked location quarantine"]
  RELEASE["Release reserved allocations<br/>cancel affected tasks"]
  QSTOCK["Move physical balance to quarantine"]
  REROUTE["Reroute remaining wave lines<br/>identical SKU + Lot,<br/>exclude quarantined location"]
  ALTSTOCK{"Alternative identical<br/>SKU / Lot stock?"}
  CYCLE["CYCLE_COUNT priority 1000<br/>system-directed"]
  DISC --> QUARANTINE --> RELEASE --> QSTOCK --> REROUTE --> ALTSTOCK
  ALTSTOCK -->|no| BACKORDER
  ALTSTOCK -->|yes| ALLOC
  QUARANTINE --> CYCLE --> ASSIGN

  PICKCONF(["Pick confirmations /<br/>storage snapshots / VAS transactions"])
  BILLINGENG["BillingEngine<br/>REPEATABLE READ + billing_event_keys"]
  CHARGE["Storage / B2C pick / B2B pallet /<br/>VAS charge calculators"]
  LEDGER["billing_events + invoice lines<br/>rate_snapshot + components"]
  OUTBOX["Transactional outbox"]
  KAFKA[("Kafka wms.* topics")]
  PICKCONF --> BILLINGENG --> CHARGE --> LEDGER --> OUTBOX

  ALLOC --> OUTBOX
  PICKTSK --> OUTBOX
  PALLETTASK --> OUTBOX
  PUT --> OUTBOX
  QUARANTINE --> OUTBOX
  REROUTE --> OUTBOX
  CYCLE --> OUTBOX
  OUTBOX --> KAFKA
  KAFKA --> DISC

  COMPLETE(["Order status:<br/>allocated / exception"])
  RF --> COMPLETE
  FORKLIFT --> COMPLETE
  BACKORDER --> COMPLETE
  REJECT --> COMPLETE

  class START,GW,PERSIST,WORKFLOW,INBOUND,PICKCONF ingress;
  class MODE,AFFINITY,PALLETQTY,SHORT,ALTSTOCK decision;
  class CAND,PLAN,ROUTE,OPENWAVE,NEWWAVE,ALLOC,REBUILD,PICKTSK,ASSIGN,RF b2c;
  class SKIP,FULLPALLET,RESERVE,PALLETTASK,FORKLIFT,PUT b2b;
  class DISC,QUARANTINE,RELEASE,QSTOCK,REROUTE,CYCLE,BACKORDER,REJECT exception;
  class BILLINGENG,CHARGE,LEDGER,OUTBOX,KAFKA billing;
  class COMPLETE terminal;
```
