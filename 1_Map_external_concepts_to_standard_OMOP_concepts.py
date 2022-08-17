@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f9be0a53-e0b8-40f1-a3db-32976387f18b"),
    Matcho_concepts=Input(rid="ri.foundry.main.dataset.016535b1-e5e2-4ebb-936a-8d28dc31c573"),
    concept=Input(rid="ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772")
)
def concepts_in_enclave(Matcho_concepts, concept):
    """
    Extract concepts from concept table.

    Parameters:
    Matcho_concepts - external dataset of OMOP concepts
    concept 
    """
    df = Matcho_concepts
    keep_columns = ["concept_id","category"]
    df = df.select(*keep_columns)
    return df.join(concept, "concept_id", "inner")

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6d3f5850-eb02-4789-a214-a753709fac44"),
    concepts_in_enclave=Input(rid="ri.foundry.main.dataset.f9be0a53-e0b8-40f1-a3db-32976387f18b")
)
def filter_non_standard(concepts_in_enclave):
    """
    Filter to non-standard concepts.

    Parameters:
    concepts_in_enclave - OMOP concepts of interest from external dataset
    """
    df = concepts_in_enclave
    df = df.filter(df.standard_concept.isNull())
    return df  
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4ed4a74e-a49b-42a2-a7f3-b87052f06e06"),
    concepts_in_enclave=Input(rid="ri.foundry.main.dataset.f9be0a53-e0b8-40f1-a3db-32976387f18b")
)
def filter_standard(concepts_in_enclave):
    """
    Filter to standard concepts.

    Parameters:
    concepts_in_enclave - OMOP concepts of interest from external dataset
    """
    df = concepts_in_enclave
    df = df.filter(df.standard_concept=="S")
    return df  
    
   
@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1ac04e5c-d261-4895-9bc5-95a7975ec8c2"),
    concept_relationship=Input(rid="ri.foundry.main.dataset.0469a283-692e-4654-bb2e-26922aff9d71"),
    filter_non_standard=Input(rid="ri.foundry.main.dataset.6d3f5850-eb02-4789-a214-a753709fac44")
)
def map_to_standard(filter_non_standard, concept_relationship):
    """
    Map non-standard concepts to standard concepts using the concept_relationship table.

    Parameters:
    filter_non_standard - non-standard concepts
    concept_relationship
    """
    concepts = filter_non_standard
    relationships = concept_relationship
    # remove columns that appear in both tables
    relationships = relationships.drop("valid_start_date","valid_end_date","invalid_reason")
    mapped_concepts = concepts.join(relationships, (concepts.concept_id == relationships.concept_id_1), "inner")
    # filter to concepts with relationship of "Maps to"
    mapped_concepts = mapped_concepts.filter(mapped_concepts.relationship_id=="Maps to")
    return mapped_concepts


@transform_pandas(
    Output(rid="ri.foundry.main.dataset.fe926552-29fa-4e90-82ac-72af0636e802"),
    concept=Input(rid="ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772"),
    map_to_standard=Input(rid="ri.foundry.main.dataset.1ac04e5c-d261-4895-9bc5-95a7975ec8c2")
)
def get_standard_columns(map_to_standard, concept):
    """
    Get concept info from concept table for concepts mapped to standard concepts.

    Parameters:
    map_to_standard - concepts mapped to standard concepts
    concept
    """
    updated_concepts = map_to_standard
    concepts = concept
    keep_columns = ["concept_id_2","category"]
    updated_concepts = updated_concepts.select(*keep_columns)
    updated_concepts = updated_concepts.withColumnRenamed("concept_id_2","concept_id")
    updated_final = updated_concepts.join(concepts, "concept_id", "inner")
    return updated_final


@transform_pandas(
    Output(rid="ri.foundry.main.dataset.dc089337-ee33-432d-81db-1526c8b9f3d6"),
    filter_standard=Input(rid="ri.foundry.main.dataset.4ed4a74e-a49b-42a2-a7f3-b87052f06e06"),
    get_standard_columns=Input(rid="ri.foundry.main.dataset.fe926552-29fa-4e90-82ac-72af0636e802")
)
def combine_tables(get_standard_columns, filter_standard):
    """
    Combine tables of standard concepts.

    Parameters:
    get_standard_columns - non-standard concepts mapped to standard concepts
    filter_standard - standard concepts
    """
    updated = get_standard_columns
    standard = filter_standard
    union_df = standard.union(updated)
    return union_df    

