# ukb-omop-rx-trajectories
# A reproducible PySpark pipeline for deriving prescription trajectories and discontinuation phenotypes from UK Biobank OMOP data

This repository contains a reproducible PySpark pipeline for deriving person-level prescription trajectories and discontinuation phenotypes from UK Biobank data represented in the OMOP Common Data Model (CDM).

## Overview

The pipeline is designed for large-scale, drug-agnostic analysis of longitudinal prescribing patterns. It uses:

- `omop_drug_era` as the primary exposure representation
- `omop_drug_exposure` for sensitivity analysis
- `omop_observation_period` for eligibility and censoring
- `omop_death` optionally for censoring
- OMOP vocabulary tables (`CONCEPT`, `concept_ancestor`) for concept validation and ingredient mapping

The outputs include:

- eligible participant tables
- analysis-era tables
- month-level treatment burden and turnover summaries
- ingredient-level discontinuation events
- person-level discontinuation phenotypes
- unsupervised trajectory clusters
- cluster summaries and top ingredients for interpretation

## Scientific rationale

The primary analysis uses OMOP `drug_era`, which represents continuous ingredient-level exposure episodes and is therefore appropriate for trajectory analysis. The sensitivity analysis reconstructs eras from `drug_exposure`, allowing explicit assessment of robustness to exposure-processing assumptions.

The resulting phenotypes represent patterns of **prescribed exposure** in primary care records. They should not be interpreted as direct measures of dispensing or medication ingestion.

## Data requirements

Expected UK Biobank RAP inputs:

- `OMOP/raw_data/omop_drug_era.csv.gz`
- `OMOP/raw_data/omop_drug_exposure.csv.gz`
- `OMOP/raw_data/omop_observation_period.csv.gz`
- `OMOP/raw_data/omop_death.csv.gz` (optional)
- `ICB/OMOP/maps/athena_omop/CONCEPT.csv`
- `ICB/OMOP/maps/athena_omop/concept_ancestor.csv`

## What is not used as a primary source

Helper files in `ICB/OMOP/` such as counts tables or joined summaries are not used to derive phenotypes. Raw OMOP tables are treated as the analytic source of truth.

## Installation

In UK Biobank RAP, PySpark is typically preinstalled. Install the remaining dependency:

```bash
pip install -r requirements.txt

