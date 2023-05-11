# n3c_pregnancy_cohort
Code used to define cohort eras (episodes) during which persons are pregnant in the National COVID Cohort Collaborative (N3C).

## Overview
Here we infer pregnancy episodes and their gestational timing for pregnancies of women recorded in OMOP Electronic Health Records across multiple data partner sites, using pooled data from the National Cohort Collaborative (N3C).

## Steps for Reproducibility
#### Cases where data is in the OMOP Common Data Model (https://www.ohdsi.org/data-standardization/) and a Spark environment is available
Please run the scripts in the order indicated by the filenames. Python language and a Spark environment is required to directly run the code. 

#### Cases where data is in the OMOP Common Data Model (https://www.ohdsi.org/data-standardization/) and a Spark environment is not available
In the case that a Spark environment is not available for your patient data, we recommend options depending on the size of your data.
Step 1
Large-scale data (billions of total patient data rows):
Option 1 - Intall Spark (https://spark.apache.org/docs/latest/api/python/getting_started/install.html) to run the code in a distributed manner locally, removing any platform-specific transform decorators.
Option 2 - Consider joining N3C as a data contributing site to analyze not only your site's data, but data pooled from multiple sites identified by data partner ID. This offers unique advantages such as measurement unit harmonization and increased power from a larger scale.
Small-scale data where Spark is not feasible:
We would like to work with sites to adapt the code to their data, please reach out to Sara Jones (sara.jones2@nih.gov) or Kate Bradwell (kbradwell@palantir.com) to develop a pandas version of the code if the size of data will remain stable and only needs to run on a single machine.

#### Cases where data is not in the OMOP Common Data Model
This will require source data mapping, e.g. a) conversion of concepts used by the algorithm from OMOP to your local Common Data Model (ACT, PCORnet etc), or b) conversion from source EHR to OMOP. Please reach out to Sara Jones (sara.jones2@nih.gov) or kbradwell@palantir.com to provide the mapping tools and assistance for these conversions.

We would like to offer assistance wherever necessary to ensure this resource is widely used. Any resulting adaptations of the code will be fully attributed to the relevant clinical sites and made widely available to benefit other sites intending to perform either research or operational workflows on their pregnant population.
