# n3c_pregnancy_cohort
Code used to define cohort eras (episodes) during which persons are pregnant in the National COVID Cohort Collaborative (N3C).

## Overview
Here we infer pregnancy episodes and gestational age from electronic health records in the OMOP Common Data Model using pooled data from the National COVID Cohort Collaborative (N3C) across multiple data partner sites.

Our comprehensive approach, named **Hierarchy and rule-based pregnancy episode Inference integrated with Pregnancy Progression Signatures (HIPPS)** combines the following algorithms: 
1. **Hierarchy-based Inference of Pregnancy (HIP) Algorithm**: A rule-based algorithm that is an extension of a previously published pregnancy episode algorithm ([Matcho et al](https://journals.plos.org/plosone/article?id=10.1371/journal.pone.0192033)). 
2. **Pregnancy Progression Signature (PPS) Algorithm**: A novel algorithm that uses temporal sequence analysis for detecting gestational age-specific signatures of a progressing pregnancy for further episode support.
3. **Estimated Start Date (ESD) Algorithm**: A rigorous algorithm to estimate pregnancy episode start dates and their precision level.

## Steps for Reuse

We modified each script to include a main function that details the sequence or order of the functions. Please modify the paths to any external files within the scripts if not using the external files contained in this repository. 

1. Run [1_Map_external_concepts_to_standard_OMOP_concepts.py](https://github.com/jonessarae/n3c_pregnancy_cohort/blob/main/1_Map_external_concepts_to_standard_OMOP_concepts.py) 
     * OMOP tables *concept* and *concept_relationship*
     * [Matcho_concepts](https://github.com/jonessarae/n3c_pregnancy_cohort/blob/main/Matcho_concepts.xlsx)
2. Run [2_Get_concept_prevalence_and_specificity_in_preg_women.py](https://github.com/jonessarae/n3c_pregnancy_cohort/blob/main/2_Get_concept_prevalence_and_specificity_in_preg_women.py)
     * OMOP tables *procedure_occurrence*, *measurement*, *observation*, *condition_occurrence*, *drug_exposure*, and *person*
     * Standardized OMOP pregnancy concepts from Step 1 
3. Run [3_Define_pregnancy_episodes_with_HIP_algorithm.py](https://github.com/jonessarae/n3c_pregnancy_cohort/blob/main/3_Define_pregnancy_episodes_with_HIP_algorithm.py)
     * OMOP tables *procedure_occurrence*, *measurement*, *observation*, *condition_occurrence*, and *person*
     * [HIP_concepts](https://github.com/jonessarae/n3c_pregnancy_cohort/blob/main/HIP_concepts.xlsx)
     * [Matcho_outcome_limits](https://github.com/jonessarae/n3c_pregnancy_cohort/blob/main/Matcho_outcome_limits.xlsx)
     * [Matcho_term_durations](https://github.com/jonessarae/n3c_pregnancy_cohort/blob/main/Matcho_term_durations.xlsx)
4. Run [4_Identify_gestational_timing_specific_concepts_for_PPS.py](https://github.com/jonessarae/n3c_pregnancy_cohort/blob/main/4_Identify_gestational_timing_specific_concepts_for_PPS.py)
     * OMOP tables *procedure_occurrence*, *measurement*, *observation*, *condition_occurrence*, and *drug_exposure*
     * Highly specific concepts from Step 2 (also available [here](https://github.com/jonessarae/n3c_pregnancy_cohort/blob/main/Highly_specific_concepts.xlsx)) 
5. Run [5_Define_pregnancy_episodes_with_PPS_algorithm.py](https://github.com/jonessarae/n3c_pregnancy_cohort/blob/main/5_Define_pregnancy_episodes_with_PPS_algorithm.py)
     * OMOP tables *procedure_occurrence*, *measurement*, *observation*, *condition_occurrence*, *visit_occurrence*, and *person*
     * [PPS_concepts](https://github.com/jonessarae/n3c_pregnancy_cohort/blob/main/PPS_concepts.xlsx)
6. Run [6_Merge_pregnancy_episodes.py](https://github.com/jonessarae/n3c_pregnancy_cohort/blob/main/6_Merge_pregnancy_episodes.py)
     * OMOP tables *procedure_occurrence*, *measurement*, *observation*, *condition_occurrence*, and *person*
     * [HIP_concepts](https://github.com/jonessarae/n3c_pregnancy_cohort/blob/main/HIP_concepts.xlsx)
     * HIP episodes from Step 3
     * PPS episodes from Step 5
7. Run [7_Infer_start_date_with_ESD_algorithm_and_add_metadata.py](https://github.com/jonessarae/n3c_pregnancy_cohort/blob/main/7_Infer_start_date_with_ESD_algorithm_and_add_metadata.py)
     * OMOP tables *procedure_occurrence*, *measurement*, *observation*, and *condition_occurrence*
     * [PPS_concepts](https://github.com/jonessarae/n3c_pregnancy_cohort/blob/main/PPS_concepts.xlsx)
     * [Matcho_term_durations](https://github.com/jonessarae/n3c_pregnancy_cohort/blob/main/Matcho_term_durations.xlsx)
     * Merged episodes from Step 6

#### Cases where data is in the OMOP Common Data Model (https://www.ohdsi.org/data-standardization/) and a Spark environment is available
Please run the scripts in the order indicated by the filenames. Python language and a Spark environment is required to directly run the code. 

#### Cases where data is in the OMOP Common Data Model (https://www.ohdsi.org/data-standardization/) and a Spark environment is not available
In the case that a Spark environment is not available for your patient data, we recommend options depending on the size of your data.

 * **Large-scale data (billions of total patient data rows):**

     * Option 1 - Install Spark (https://spark.apache.org/docs/latest/api/python/getting_started/install.html) to run the code in a distributed manner locally.
     * Option 2 - Consider joining N3C as a data contributing site to analyze not only your site's data, but data pooled from multiple sites identified by data partner ID. This offers unique advantages such as measurement unit harmonization and increased power from a larger scale.

* **Small-scale data:**

    We would like to work with sites to adapt the code to their data, please reach out to Sara Jones (sara.jones2@nih.gov) or Kate Bradwell (kbradwell@palantir.com) to develop a pandas version of the code if the size of data will remain stable and only needs to run on a single machine.

#### Cases where data is not in the OMOP Common Data Model
This will require source data mapping, e.g. a) conversion of concepts used by the algorithm from OMOP to your local Common Data Model (ACT, PCORnet etc), or b) conversion from source EHR to OMOP. Please reach out to Sara Jones (sara.jones2@nih.gov) or Kate Bradwell (kbradwell@palantir.com) to provide the mapping tools and assistance for these conversions.

We would like to offer assistance wherever necessary to ensure this resource is widely used. Any resulting adaptations of the code will be fully attributed to the relevant clinical sites and made widely available to benefit other sites intending to perform either research or operational workflows on their pregnant population.
