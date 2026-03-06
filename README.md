# An Agentic Multi-Database RAG System for Multilingual Question Answering over Global News

This repository contains the implementation developed for my M.Sc. thesis in Computer Engineering at Roma Tre University.

The project presents an agentic multi-database Retrieval-Augmented Generation (RAG) system for multilingual question answering over global news. The system is built on top of GDELT data and combines structured, graph-based, lexical, and semantic retrieval to answer heterogeneous natural-language questions grounded in verifiable evidence.

## Overview

The system is designed to support different classes of questions, including:

- analytical and quantitative questions
- entity-centric and relational questions
- descriptive questions over news articles
- hybrid questions requiring both textual and structured evidence

To support these scenarios, the architecture combines multiple datastores and retrieval strategies under a single orchestrated workflow.

## Architecture

The system includes:

- **PostgreSQL** for structured and quantitative queries
- **Neo4j** for graph-based and relational retrieval
- **Elasticsearch** for full-text lexical retrieval
- **Qdrant** for dense vector retrieval
- **LangGraph** for orchestration and control flow
- **FastAPI** for the query interface
- **RAGAS** for evaluation

The workflow is agentic and includes:

1. query routing
2. evidence retrieval from one or more stores
3. evidence grading
4. query rewriting when evidence is insufficient
5. grounded answer generation

## Main Features

- Multi-store retrieval over heterogeneous knowledge sources
- Dynamic routing based on query type
- Hybrid lexical + dense retrieval
- Graph-to-text bridge for turning graph results into textual evidence
- Controlled self-correction loop with limited retries
- Multilingual question answering over global news
- Evaluation pipeline with golden dataset generation and RAG metrics

## Data Pipeline

The project starts from GDELT structured data and extends it with full-text article acquisition.

The pipeline includes:

- structured data preprocessing
- article reconstruction from GDELT Web News NGrams
- article scraping with fallback strategies
- text cleaning and normalization
- recursive chunking
- dense embedding generation
- indexing into multiple datastores
