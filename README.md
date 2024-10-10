# SIC-Integration
Integration of the Smart Insect Counting (SIC), work package 3 (WP3) from the Sustainable Production of Healthy Foods (SPoHF) into the Digital Twin (WP6)

Dependencies:
- Apache Spark (for sql querries and streaming)
- Dela lake (for lakehouse architecture)
- PrimeNG/Angular (for visualisations)

## Guidelines
1. Incoming data is converted to parquet for storage in 'Bronze' section
2. The next step is 'LesserTransmutation' where the parquet files from the 'Bronze' section are converted to organized delta lake tables ans stored in the 'Silver' section
3. In the step 'GreaterTransmutation' the delta lake tables enriched, prepared for visualisations and stored in the 'Gold' section
