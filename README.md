```mermaid
graph LR
    subgraph "Fabric OneLake"
        A[Bronze Lakehouse: Raw CSVs] -->|Notebook 1| B(Silver Lakehouse: Cleansed Delta)
        B -->|Notebook 2| C(Gold Lakehouse: Star Schema)
    end

    WHO[WHO GLASS Portal] -->|Manual Ingestion| A
    C -->|DirectLake| PBI[Power BI Decision Engine]

    style WHO fill:#ffffff,stroke:#333,stroke-width:1px
    style A fill:#E1F5FE,stroke:#0277BD,stroke-width:2px
    style B fill:#E1F5FE,stroke:#0277BD,stroke-width:2px
    style C fill:#E1F5FE,stroke:#0277BD,stroke-width:2px
    style PBI fill:#ffffff,stroke:#333,stroke-width:1px
```


# GLASS-AMR-Data-Engine-End-to-End-WHO-Surveillance-Pipeline-in-Microsoft-Fabric
This project engineers a fully automated, end-to-end data analytics platform in Microsoft Fabric to procness, model, and visualize global Antimicrobial Resistance (AMR) data. By bridgig Data Engineering with Microbiological domain logic, this pipeline transforms raw, disjointed surveillance datasets into a bias-adjusted Decision Intelligence engine
