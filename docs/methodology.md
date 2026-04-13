# Methodology

## Study design
This project implements a retrospective longitudinal prescription analysis pipeline in UK Biobank using data represented in the OMOP Common Data Model.

## Primary exposure representation
The primary analysis uses `omop_drug_era`, which represents inferred continuous ingredient-level exposure episodes. This is the preferred OMOP representation for whole-person prescription trajectory analysis because it reduces fragmentation of longitudinal exposure.

## Sensitivity analysis
A secondary analysis reconstructs eras from `omop_drug_exposure` by mapping `drug_concept_id` to ingredient ancestors and merging overlapping or near-contiguous intervals using a 30-day gap rule.

## Cohort definition
Participants must have:
- at least one eligible drug record
- at least 365 days of observation before the first eligible era

Follow-up begins at the first eligible era and ends at the earliest of:
- end of observation
- death, if available
- 24 months after index

## Monthly prescription trajectories
Each participant’s follow-up is divided into person-specific monthly intervals. For each month, the pipeline derives:
- number of active ingredients
- number of starts
- number of stops
- month-to-month treatment turnover

## Monthly states
Each month is classified as one of:
- NoRx
- Initiation
- StableMono
- StableLowPoly
- StablePolypharmacy
- Intensifying
- Deintensifying
- HighTurnover

Polypharmacy is defined as 5 or more concurrent active ingredients.

## Discontinuation logic
Discontinuation is only evaluated for maintenance-eligible participant–ingredient pairs:
- at least 2 eras, or
- at least 28 total era-days

For these pairs, the pipeline calculates:
- restart within 180 days
- switch within 60 days
- early discontinuation of first era (<90 days, no restart within 180 days)

## Person-level phenotypes
The following descriptive discontinuation phenotypes are assigned:
- Persistent stable use
- Stable polypharmacy
- Intermittent stop-start
- High-turnover switching
- Early drop-off / de-intensification
- Mixed transition pattern

## Clustering
Trajectories are summarized using burden, turnover, state proportions, and discontinuation features, then clustered using K-means. The final number of clusters is chosen by silhouette width.

## Interpretation
The resulting phenotypes represent patterns of prescribed exposure captured in primary care records. They should not be interpreted as direct measures of dispensing or adherence.
