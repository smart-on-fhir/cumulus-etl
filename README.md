# Cumulus: Clinical Investigation at Population Scale
Cumulus will enable the flow of aggregate and de-identified data on a broad set of health condition and patient population trends over time.
Cumulus is based out of Boston Children’s Hospital with collaborations in US hospitals and departments of public health.

* [SMART push button population health](https://www.nature.com/articles/s41746-020-00358-4)
* [AEGIS infectious disease monitoring](https://pubmed.ncbi.nlm.nih.gov/17600100)

## 21st Century Cures Act: effective Jan 1, 2023
Cumulus will capitalize on “21st Century Cures Act” availability of EHR data in bulk FHIR to enable population health investigations locally, regionally, and nationally.
21st century federal regulations take effect beginning Jan 2023, and Cumulus will capitalize on this new clinical datasource for clinical investigations.

# Cumulus Features
* Extracts bulk FHIR data
* Performs natural language processing (NLP) on physician notes via [cTAKES](https://ctakes.apache.org/) to extract symptoms and other information
* De-identifies protected health information (PHI) before any data leaves your health institution
* All data is encrypted at rest and in transit
* Focuses on non-human-subject research and minimal disclosures -- researchers only see patient counts
* A dashboard provides graphs of patient count data, for multiple studies
* Regional clustering and aggregation

## SQL Queries Over Patient Populations
Use the provided [Cumulus Library](https://github.com/comorbidity/library)
* Packages "public health data feeds" into well-defined patient cohorts
* Simplifies FHIR data as SQL views for easier accessibility to biomedical staff
* Simplifies common views like Patient demographics, Hospital Encounters, Condition coding, etc

## Regional Cluster
* [Federalist principles for healthcare data networks](https://www.nature.com/articles/nbt.3180)

# Further Reading
* An [overview of how Cumulus works](docs/explanations/overview.md) (non-technical)
* An [explanation of how Cumulus de-identifies patient data](docs/explanations/deid.md) (lightly-technical)
* A [first-time setup guide](docs/howtos/first-time-setup.md) (highly technical)
* An [integration guide](https://github.com/smart-on-fhir/cumulus-aggregator/docs/site-integration.md) for the Cumulus ecosystem (highly technical)
