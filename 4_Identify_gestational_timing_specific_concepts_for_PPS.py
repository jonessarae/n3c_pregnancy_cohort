from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.5caee4b7-2879-49e4-8e49-c600bc958ee8"),
    Highly_specific_concepts=Input(rid="ri.foundry.main.dataset.6fa29bb6-ec86-4d4d-b564-95fb21d486ca"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.db571f18-bdff-4310-92ed-5e71625f40a3"),
    drug_exposure=Input(rid="ri.foundry.main.dataset.a1779c72-990c-4b7f-925a-2422b7c423b5"),
    final_episodes_with_length=Input(rid="ri.foundry.main.dataset.1da2194d-3770-4152-a468-8b4d0661ba86"),
    measurement=Input(rid="ri.foundry.main.dataset.8ce02960-4ca2-4adf-b96e-f8d55e14f548"),
    observation=Input(rid="ri.foundry.main.dataset.d44db8a2-709a-4265-b59f-bb929ebf1d54"),
    procedure_occurrence=Input(rid="ri.foundry.main.dataset.b6a1e256-c72f-4a66-9192-643fe063c631")
)
def get_domain_concepts_per_episode(final_episodes_with_length, drug_exposure, observation, measurement, procedure_occurrence, condition_occurrence, Highly_specific_concepts):
    '''
    for every HIP episode:
        1. get all the domain concepts and their dates within the pregnancy dates
        2. preserve the final outcome date and category, and pregnancy start date, per concept
    '''
    pregnant_dates = final_episodes_with_length.select('person_id','episode','estimated_start_date','visit_date','category').filter(~(F.col('category') == 'PREG')).filter(~(F.col('category') == 'DELIV'))
    pregnant_dates = pregnant_dates.withColumnRenamed('estimated_start_date','start_date_of_pregnancy')
    pregnant_dates = pregnant_dates.withColumnRenamed('visit_date','end_date')

    c_o = condition_occurrence
    p_o = procedure_occurrence
    o_df = observation
    m_df = measurement
    d_e = drug_exposure
    preg_concepts = Highly_specific_concepts

    def get_preg_concept_dfs(preg_concepts, df, domain_col):
        preg_concepts_df = df.join(preg_concepts,F.col(domain_col) == preg_concepts.concept_id,'inner')
        return preg_concepts_df

    c_o = get_preg_concept_dfs(preg_concepts,c_o, 'condition_concept_id')
    p_o = get_preg_concept_dfs(preg_concepts,p_o, 'procedure_concept_id')
    o_df = get_preg_concept_dfs(preg_concepts,o_df, 'observation_concept_id')
    m_df = get_preg_concept_dfs(preg_concepts,m_df, 'measurement_concept_id')
    d_e = get_preg_concept_dfs(preg_concepts,d_e, 'drug_exposure_id')

    def get_preg_related_concepts(preg_dates,df,start_date_col,id_col,name_col):

        pregnant_persons = df.join(preg_dates, (df.person_id == preg_dates.person_id) & (F.col(start_date_col) > preg_dates.start_date_of_pregnancy) & (F.col(start_date_col) < preg_dates.end_date),'inner').drop(preg_dates.person_id)

        pregnant_persons = pregnant_persons.withColumnRenamed(start_date_col, 'domain_concept_start_date')
        pregnant_persons = pregnant_persons.withColumnRenamed(id_col, 'domain_concept_id')
        pregnant_persons = pregnant_persons.withColumnRenamed(name_col, 'domain_concept_name')

        pregnant_persons = pregnant_persons.select('person_id','domain_concept_start_date','domain_concept_id','domain_concept_name','episode','start_date_of_pregnancy','end_date','category')
        return pregnant_persons
    
    c_o = get_preg_related_concepts(pregnant_dates,c_o,'condition_start_date','condition_concept_id','condition_concept_name')
    p_o = get_preg_related_concepts(pregnant_dates,p_o,'procedure_date','procedure_concept_id','procedure_concept_name')
    o_df = get_preg_related_concepts(pregnant_dates,o_df,'observation_date','observation_concept_id','observation_concept_name')
    m_df = get_preg_related_concepts(pregnant_dates,m_df,'measurement_date','measurement_concept_id','measurement_concept_name')
    d_e = get_preg_related_concepts(pregnant_dates,d_e,'drug_exposure_start_date','drug_concept_id','drug_concept_name')
    preg_related_concepts = (((c_o.unionByName(p_o)).unionByName(o_df)).unionByName(m_df)).unionByName(d_e)
    return preg_related_concepts

    
@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a9060139-2270-4923-84a1-a749053bbc6f"),
    get_domain_concepts_per_episode=Input(rid="ri.foundry.main.dataset.5caee4b7-2879-49e4-8e49-c600bc958ee8")
)
def get_gestational_timing_info_per_concept(get_domain_concepts_per_episode):

    '''
    GET GESTATIONAL TIMING INFORMATION FOR CONCEPTS
    1. for all outcomes: get the gestation month of the concept based on the start date
    2. for just full-term: additionally get the gestation month of the concept based on working back 9 months from the delivery date
    '''

    # get gestation month based on pregnancy start date
    df = get_domain_concepts_per_episode.withColumn('gestation_month', (F.datediff('domain_concept_start_date','start_date_of_pregnancy')/30))

    # get pregnancy start date based on full-term end date
    df = df.withColumn('start_date_of_pregnancy_FT', (F.when(F.col('category') == 'LB', F.expr("add_months(end_date, -9)")).otherwise('')))

    # get gestation month based on full-term start date
    df = df.withColumn('gestation_month_FT', (F.datediff('domain_concept_start_date','start_date_of_pregnancy_FT')/30))

    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.19269079-c479-4a63-b914-2c4db58238f2"),
    get_gestational_timing_info_per_concept=Input(rid="ri.foundry.main.dataset.a9060139-2270-4923-84a1-a749053bbc6f")
)
def reformat_and_rem_highly_variable_timing_concepts(get_gestational_timing_info_per_concept):
    '''
    1. for each outcome and concept, get percent of episodes that have that concept 
    2. select relevant cols, reformat to obtain LB_FT rows instead of separate column, and 
       calculate stdev of month for each concept per outcome category
    3. remove any concept where stdev > 2 in any category that it's in (highly variable)
    '''

    df_freq_temp1 = get_gestational_timing_info_per_concept.select('person_id','episode','category').distinct()
    df_freq_temp1 = df_freq_temp1.groupBy('category').agg(F.count(F.lit(1)).alias('total_episodes_for_outcome'))

    df_freq_temp2 = get_gestational_timing_info_per_concept.select('person_id','episode','domain_concept_id', 'domain_concept_name','category').distinct()
    df_freq_temp2 = df_freq_temp2.groupBy('domain_concept_id', 'domain_concept_name','category').agg(F.count(F.lit(1)).alias('concept_episodes_for_outcome'))

    df_freq_temp = df_freq_temp2.join(df_freq_temp1, 'category', 'left')

    df_freq_temp = df_freq_temp.withColumn('percent_episodes',(F.col('concept_episodes_for_outcome') / F.col('total_episodes_for_outcome')) *100)

    new_df = get_gestational_timing_info_per_concept.join(df_freq_temp,['domain_concept_id', 'domain_concept_name','category'],'left')

    df1 = new_df.select('domain_concept_id', 'domain_concept_name','category', 'percent_episodes', 'gestation_month')
    df2 = new_df.select('domain_concept_id', 'domain_concept_name','category', 'percent_episodes', 'gestation_month_FT').filter(F.col('gestation_month_FT').isNotNull()) \
                                                 .withColumnRenamed('gestation_month_FT','gestation_month').withColumn('category',F.lit('LB_FT'))

    df = df1.unionByName(df2)

    # calculate stdev
    df = df.groupBy('category','domain_concept_id', 'domain_concept_name', 'percent_episodes').agg(F.mean(F.col('gestation_month')).alias('mean_month'),F.stddev(F.col('gestation_month')).alias('stddev_of_month'),F.count(F.col('gestation_month')).cast(IntegerType()).alias('num_appearances'))

    # keep concepts where the percent of episodes is greater than 3
    df = df.filter(F.col('percent_episodes')>3)

    # find concepts where there's a stddev > 2 (don't want these because in some outcome it DOESN'T have a specific gestational timing)
    df_temp = df.filter(F.col('stddev_of_month') > 2).select('domain_concept_id').distinct()
    df = df.join(df_temp, on=['domain_concept_id'], how='left_anti')

    # find concepts that have a large stddev ACROSS outcomes (we also don't want these)
    df_temp2 = df.groupBy('domain_concept_id', 'domain_concept_name').agg(F.stddev(F.col('mean_month')).alias('stddev_across_outcomes'))
    df_temp2 = df_temp2.filter(F.col('stddev_across_outcomes') > 1.5).select('domain_concept_id').distinct()

    df = df.join(df_temp2, on=['domain_concept_id'], how='left_anti')

    df = df.filter(~df.domain_concept_id.isin([4250175, 4012558, 45757490, 4058562])) # remove "Pregnancy Detection Evaluation", "Possible pregnancy", "Pregnancy not yet confirmed", "H/O: infertility - female"

    sdoh = [
    40766945, # Current smoker
    3022828, #  Mother's race
    3007191, #  Age - Reported
    3022304, # Age
    3046853, # Race
    ]

    df = df.filter(~df.domain_concept_id.isin(sdoh))# remove SDoH concepts - may have used particular codes at maternity clinics

    return df


@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c521bcb8-7ab7-473a-8ef3-e16404247662"),
    get_gestational_timing_info_per_concept=Input(rid="ri.foundry.main.dataset.a9060139-2270-4923-84a1-a749053bbc6f"),
    reformat_and_rem_highly_variable_timing_concepts=Input(rid="ri.foundry.main.dataset.19269079-c479-4a63-b914-2c4db58238f2")
)
def get_unique_GT_specific_concepts_for_curation(reformat_and_rem_highly_variable_timing_concepts, get_gestational_timing_info_per_concept):
    df = reformat_and_rem_highly_variable_timing_concepts.select('domain_concept_id','domain_concept_name').distinct()
    df_overall_mean_and_stddev = df.join(get_gestational_timing_info_per_concept,['domain_concept_id','domain_concept_name'],'inner')
    df_overall_mean_and_stddev = df_overall_mean_and_stddev.groupBy('domain_concept_id','domain_concept_name').agg(F.mean(F.col('gestation_month')).alias('mean_gestation_month'), F.stddev(F.col('gestation_month')).alias('stddev_gestation_month'),F.countDistinct('person_id').alias('distinct_patients'))
    return df_overall_mean_and_stddev
