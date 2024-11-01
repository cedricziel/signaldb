# signaldb

A signal database based on the FDAP stack.

## Project goals

I am building this project in my free time. The goal is to build a database for observability signals (metrics/logs/traces/..).

The key traits I want this project to develop over time:
* cost-effective storage
* open standards native ingestion (OTLP, Prom, ..)
* effective querying
* robust interfacing with popular analysis tools (Grafana, Perses, ..)
* easy operation

## What is the FDAP stack?

The FDAP stack is a set of technologies that can be used to build a data acquisition and processing system. 
It is composed of the following components:

* **F**light - Apache Arrow Flight
* **D**ataFusion - Apache DataFusion
* **A**rrow - Apache Arrow
* **P**arquet - Apache Parquet

https://www.influxdata.com/glossary/fdap-stack/

## License

AGPL-3.0
