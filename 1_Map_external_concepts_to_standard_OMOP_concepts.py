# external files
Matcho_concepts = "Matcho_concepts.xlsx"

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

def filter_non_standard(concepts_in_enclave):
    """
    Filter to non-standard concepts.

    Parameters:
    concepts_in_enclave - OMOP concepts of interest from external dataset
    """
    df = concepts_in_enclave
    df = df.filter(df.standard_concept.isNull())
    return df  
    
def filter_standard(concepts_in_enclave):
    """
    Filter to standard concepts.

    Parameters:
    concepts_in_enclave - OMOP concepts of interest from external dataset
    """
    df = concepts_in_enclave
    df = df.filter(df.standard_concept=="S")
    return df  
    
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

def main():
    concepts_in_enclave_df = concepts_in_enclave(Matcho_concepts, concept)
    filter_non_standard_df = filter_non_standard(concepts_in_enclave_df)
    filter_standard_df = filter_standard(concepts_in_enclave_df)
    map_to_standard_df = map_to_standard(filter_non_standard_df, concept_relationship)
    get_standard_columns_df = get_standard_columns(map_to_standard_df, concept)
    Matcho_concepts_df = combine_tables(get_standard_columns_df, filter_standard_df)

if __name__ == "__main__":
    main()

