# System Architecture

```mermaid
graph TD
    subgraph "Docker Environment (Managed by Docker Compose)"
        A[API Poller] -- Live Game JSON --> B(Kafka Topic);
        B -- Raw JSON --> C(Real-time Transformer);
        C -- Structured Data --> D(PostgreSQL Database);
        E[Airflow] -- Schedules --> F(Airflow DAG);
        F -- Reads Raw Data --> G[Raw JSON Storage];
        F -- Writes Aggregated/Recalculated Data --> D;
        H[Backend API] -- Reads Data --> D;
    end

    subgraph External Systems
        MLB[statsapi.mlb.com] --> A;
        User[User / Frontend / Models] --> H;
    end

    A -- Writes Raw JSON --> G;
    C -- Optionally Writes Raw JSON Backup --> G;

    style A fill:#fff,stroke:#333,color:#333
    style B fill:#f80,stroke:#a50,color:#333
    style C fill:#fff,stroke:#333,color:#333
    style D fill:#d3e0f0,stroke:#333,color:#333
    style E fill:#9cf,stroke:#369,color:#333
    style F fill:#fff,stroke:#333,color:#333
    style G fill:#eee,stroke:#333,color:#333,stroke-dasharray: 5 5
    style H fill:#fff,stroke:#333,color:#333
    style MLB fill:#f9f,stroke:#333,color:#333,stroke-width:2px
    style User fill:#ccf,stroke:#333,color:#333,stroke-width:2px
