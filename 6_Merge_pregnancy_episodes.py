@transform_pandas(
    Output(rid="ri.foundry.main.dataset.243bd07d-5d64-4236-b1b3-6a3d1fa28e21"),
    HIP_concepts=Input(rid="ri.foundry.main.dataset.9abc8da4-cd3c-43d6-a6a5-f52a5baf398e")
)
def filter_to_any_outcome(HIP_concepts):
    df = HIP_concepts
    df = df.where(F.col("category").isin({"LB", "SB", "DELIV", "ECT", "AB", "SA"}))
    df = df.select('concept_id','concept_name','category').distinct()
    return df
    
#################################################
## Global imports and functions included below ##
#################################################

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DateType, DoubleType,ArrayType, StringType, IntegerType 


@transform_pandas(
    Output(rid="ri.foundry.main.dataset.45be46d3-ea37-444b-a25d-02c6ba33fc08"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.db571f18-bdff-4310-92ed-5e71625f40a3"),
    filter_to_any_outcome=Input(rid="ri.foundry.main.dataset.243bd07d-5d64-4236-b1b3-6a3d1fa28e21"),
    get_episode_max_min_dates=Input(rid="ri.foundry.main.dataset.6412d917-6d85-4690-9aab-cd41c879a3fc"),
    get_episodes=Input(rid="ri.foundry.main.dataset.98d36cb4-5706-4489-b714-4ab930310b4c"),
    measurement=Input(rid="ri.foundry.main.dataset.8ce02960-4ca2-4adf-b96e-f8d55e14f548"),
    observation=Input(rid="ri.foundry.main.dataset.d44db8a2-709a-4265-b59f-bb929ebf1d54"),
    procedure_occurrence=Input(rid="ri.foundry.main.dataset.b6a1e256-c72f-4a66-9192-643fe063c631")
)
def outcomes_per_episode( procedure_occurrence, condition_occurrence, measurement, observation, get_episode_max_min_dates, filter_to_any_outcome, get_episodes):
    """
    Get outcomes for Algorithm 2:
    Outcomes are collected from a 'lookback to lookahead window', which is the episode max date minus 14d to the earliest out of i) the next closest episode start date or ii) a number of months of length (10 months - the earliest concept month that could relate to the end of the episode)
    """
    from pyspark.sql.functions import pandas_udf, PandasUDFType

    df = filter_to_any_outcome
    concept_list = list(df.select('concept_id').toPandas()['concept_id'])
    from pyspark.sql.types import DateType, DoubleType

    pregnant_dates = get_episode_max_min_dates

    # for each episode, get the date corresponding to the next closest episode, in a new column called 'next_closest_episode_date'
    tmp_pregnant_dates = pregnant_dates.sort(F.col('personID'),F.col('person_episode_number'),F.col('episode_min_date')).filter(F.col('person_episode_number') > 1).withColumn('new_person_episode_number', (F.col('person_episode_number') - 1)) \
                                       .withColumnRenamed('episode_min_date','next_closest_episode_date').drop('person_episode_number').drop('episode_max_date_plus_two_months').drop('episode_max_date')
    
    tmp_pregnant_dates = tmp_pregnant_dates.withColumnRenamed('new_person_episode_number','person_episode_number')
    
    pregnant_dates = pregnant_dates.join(tmp_pregnant_dates, ['personID','person_episode_number'],'left')

    pregnant_dates = pregnant_dates.withColumn('next_closest_episode_date', F.date_add(pregnant_dates['next_closest_episode_date'], -1))

    preg_episode_concept_GA = get_episodes

    # get the max number of months to look ahead from the episode itself, in a new column called 'max_pregnancy_date'
    ''' do this by saving the concept date relating to the last episode concept (if multiple on the same date then the one containing max month), of which the min month is used out of the tuple of month values where necessary and subtracted from 10,
    this then is added onto the concept date to get 'max_pregnancy_date'
    '''
    w = Window.partitionBy(F.col('personID'),F.col('person_episode_number')).orderBy(F.col('conceptDate').desc(),F.col('conceptMonth')[1].desc(),F.col('conceptMonth')[0])
    tmp_preg_episode_concept_GA = preg_episode_concept_GA.withColumn("row",F.row_number().over(w)).filter(F.col("row") == 1).drop("row") \
                                                     .withColumn('max_pregnancy_date', F.expr("add_months(conceptDate, (11 - conceptMonth[0]))")) \
                                                     .select('personID','person_episode_number','max_pregnancy_date')

    pregnant_dates = pregnant_dates.join(tmp_preg_episode_concept_GA, ['personID','person_episode_number'],'left')

    # add a column for the look ahead date to search for outcomes until, called 'episode_max_date_plus_lookahead_window', from the earliest date out of 'next_closest_episode_date' and 'max_pregnancy_date'
    pregnant_dates = pregnant_dates.withColumn('next_closest_episode_date', F.when(F.col('next_closest_episode_date').isNull(), dt.date(2999, 1, 1)).otherwise(F.col('next_closest_episode_date')))

    pregnant_dates = pregnant_dates.withColumn('episode_max_date_plus_lookahead_window_dates_tmp',(F.array(['next_closest_episode_date','max_pregnancy_date'])))

    pregnant_dates = pregnant_dates.withColumn('episode_max_date_plus_lookahead_window', F.array_min(pregnant_dates.episode_max_date_plus_lookahead_window_dates_tmp))

    # add column that's a buffer for the start time to search for outcomes 'episode_max_date_minus_lookback_window'

    pregnant_dates = pregnant_dates.withColumn('episode_max_date_minus_lookback_window', F.date_add(pregnant_dates.episode_max_date,-14)) \
                                   .drop('episode_max_date_plus_lookahead_window_dates_tmp')

    # begin searching for outcomes within the relevant lookback and lookahead dates
    c_o = condition_occurrence
    o_df = observation
    m_df = measurement
    p_o = procedure_occurrence

    c_o = c_o.filter(F.lower(F.col('condition_concept_id')).isin(concept_list))
    o_df = o_df.filter(F.col('observation_concept_id').isin(concept_list))
    m_df = m_df.filter(F.col('measurement_concept_id').isin(concept_list))
    p_o = p_o.filter(F.col('procedure_concept_id').isin(concept_list))

    pregnant_dates = pregnant_dates.withColumnRenamed('person_id','personID')

    def get_preg_related_concepts(preg_dates,df,df_date_col,id_col,name_col,val_col):

        pregnant_persons = df.join(preg_dates, (F.col('person_id') == preg_dates.personID) & (F.col(df_date_col) >= preg_dates.episode_max_date_minus_lookback_window) & (F.col(df_date_col) <= preg_dates.episode_max_date_plus_lookahead_window),'inner').drop('personID')

        pregnant_persons = pregnant_persons.withColumnRenamed(df_date_col, 'domain_concept_start_date')
        pregnant_persons = pregnant_persons.withColumnRenamed(id_col, 'domain_concept_id')
        pregnant_persons = pregnant_persons.withColumnRenamed(name_col, 'domain_concept_name')
        pregnant_persons = pregnant_persons.withColumnRenamed(val_col, 'domain_value')

        pregnant_persons = pregnant_persons.select('person_id','domain_concept_start_date','domain_concept_id','domain_concept_name','person_episode_number','episode_min_date','episode_max_date','episode_max_date_minus_lookback_window','episode_max_date_plus_lookahead_window','domain_value')
        
        return pregnant_persons
    
    c_o = c_o.withColumn('value_col',F.col('condition_concept_name'))
    p_o = p_o.withColumn('value_col',F.col('procedure_concept_name'))
    c_o = get_preg_related_concepts(pregnant_dates,c_o,'condition_start_date','condition_concept_id','condition_concept_name','value_col')
    o_df = get_preg_related_concepts(pregnant_dates,o_df,'observation_date','observation_concept_id','observation_concept_name','value_as_string')
    p_o = get_preg_related_concepts(pregnant_dates,p_o,'procedure_date','procedure_concept_id','procedure_concept_name','value_col')
    m_df = get_preg_related_concepts(pregnant_dates,m_df,'measurement_date','measurement_concept_id','measurement_concept_name','value_as_number')

    preg_related_concepts = (((c_o.unionByName(o_df)).unionByName(m_df)).unionByName(p_o))

    preg_related_concepts = preg_related_concepts.join(df,preg_related_concepts.domain_concept_id == df.concept_id,'left')

    preg_related_concepts = preg_related_concepts.withColumn('lst', F.concat(preg_related_concepts['domain_concept_start_date'], F.lit(','), preg_related_concepts['domain_concept_id'], F.lit(','), preg_related_concepts['domain_concept_name'], F.lit(','), preg_related_concepts['category']).alias('lst')).groupBy('person_id','person_episode_number','episode_min_date','episode_max_date','episode_max_date_minus_lookback_window','episode_max_date_plus_lookahead_window').agg( F.collect_list('lst').alias('outcomes_list'))

    df1 = preg_related_concepts.withColumn('outcomes_list',F.array_remove(preg_related_concepts.outcomes_list, ''))
    df1 = df1.withColumn('outcomes_list',F.array_sort(df1.outcomes_list))

    @pandas_udf(returnType=ArrayType(StringType()))
    def delivery_udf(v,deliv_cat):
        return v.apply(lambda arr: [x for x in arr if deliv_cat[0] in x], "array<string>")

    # LIVE BIRTH
    df1 = df1.withColumn('LB_delivery_date',delivery_udf(F.col('outcomes_list'),F.lit('LB')))
    df1 = df1.withColumn('LB_delivery_date',F.col('LB_delivery_date')[0])
    split_col = F.split(df1['LB_delivery_date'], ',')
    df1 = df1.withColumn('LB_delivery_date',split_col.getItem(0))

    # STILL BIRTH
    df1 = df1.withColumn('SB_delivery_date',delivery_udf(F.col('outcomes_list'),F.lit('SB')))
    df1 = df1.withColumn('SB_delivery_date',F.col('SB_delivery_date')[0])
    split_col = F.split(df1['SB_delivery_date'], ',')
    df1 = df1.withColumn('SB_delivery_date',split_col.getItem(0))

    # ECTOPIC PREGNANCY
    df1 = df1.withColumn('ECT_delivery_date',delivery_udf(F.col('outcomes_list'),F.lit('ECT')))
    df1 = df1.withColumn('ECT_delivery_date',F.col('ECT_delivery_date')[0])
    split_col = F.split(df1['ECT_delivery_date'], ',')
    df1 = df1.withColumn('ECT_delivery_date',split_col.getItem(0))

    # SPONTANEOUS ABORTION
    df1 = df1.withColumn('SA_delivery_date',delivery_udf(F.col('outcomes_list'),F.lit('SA')))
    df1 = df1.withColumn('SA_delivery_date',F.col('SA_delivery_date')[0])
    split_col = F.split(df1['SA_delivery_date'], ',')
    df1 = df1.withColumn('SA_delivery_date',split_col.getItem(0))

    # ABORTION
    df1 = df1.withColumn('AB_delivery_date',delivery_udf(F.col('outcomes_list'),F.lit('AB')))
    df1 = df1.withColumn('AB_delivery_date',F.col('AB_delivery_date')[0])
    split_col = F.split(df1['AB_delivery_date'], ',')
    df1 = df1.withColumn('AB_delivery_date',split_col.getItem(0))

    # DELIVERY
    df1 = df1.withColumn('DELIV_delivery_date',delivery_udf(F.col('outcomes_list'),F.lit('DELIV')))
    df1 = df1.withColumn('DELIV_delivery_date',F.col('DELIV_delivery_date')[0])
    split_col = F.split(df1['DELIV_delivery_date'], ',')
    df1 = df1.withColumn('DELIV_delivery_date',split_col.getItem(0))

    df1 = df1.withColumn('algo2_category',F.when(F.col('LB_delivery_date').isNotNull(),'LB').otherwise(F.when(F.col('SB_delivery_date').isNotNull(),'SB').otherwise(F.when(F.col('ECT_delivery_date').isNotNull(),'ECT').otherwise(F.when(F.col('SA_delivery_date').isNotNull(),'SA').otherwise(F.when(F.col('AB_delivery_date').isNotNull(),'AB').otherwise(F.when(F.col('DELIV_delivery_date').isNotNull(),'DELIV')))))))

    df1 = df1.withColumn('algo2_outcome_date',F.when(F.col('LB_delivery_date').isNotNull(),F.col('LB_delivery_date')).otherwise(F.when(F.col('SB_delivery_date').isNotNull(),F.col('SB_delivery_date')).otherwise(F.when(F.col('ECT_delivery_date').isNotNull(),F.col('ECT_delivery_date')).otherwise(F.when(F.col('SA_delivery_date').isNotNull(),F.col('SA_delivery_date')).otherwise(F.when(F.col('AB_delivery_date').isNotNull(),F.col('AB_delivery_date')).otherwise(F.when(F.col('DELIV_delivery_date').isNotNull(),F.col('DELIV_delivery_date'))))))))

    return df1

#################################################
## Global imports and functions included below ##
#################################################

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DateType, DoubleType,ArrayType, StringType, IntegerType
import datetime as dt


@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a415921d-7092-464c-9177-b0be50b775fd"),
    get_episode_max_min_dates=Input(rid="ri.foundry.main.dataset.6412d917-6d85-4690-9aab-cd41c879a3fc"),
    outcomes_per_episode=Input(rid="ri.foundry.main.dataset.45be46d3-ea37-444b-a25d-02c6ba33fc08")
)
def add_outcomes(outcomes_per_episode, get_episode_max_min_dates):
    """
    Join outcomes for Algo 2 to main Algo 2 table.
    """
    out_df = outcomes_per_episode.select("person_id","person_episode_number","episode_min_date","algo2_category","algo2_outcome_date")
    out_df = out_df.withColumnRenamed("person_episode_number", "person_episode_num")
    out_df = out_df.withColumnRenamed("episode_min_date", "episode_minDate")
    df = get_episode_max_min_dates
    df = df.join(out_df, on = [df.personID == out_df.person_id, df.person_episode_number == out_df.person_episode_num, df.episode_min_date == out_df.episode_minDate], how="left")
    df = df.drop("person_id","person_episode_num","episode_minDate")

    return df

#################################################
## Global imports and functions included below ##
#################################################

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DateType, DoubleType,ArrayType, StringType, IntegerType


@transform_pandas(
    Output(rid="ri.foundry.main.dataset.002ba763-6d24-4737-83c9-277fd24353ce"),
    add_outcomes=Input(rid="ri.foundry.main.dataset.a415921d-7092-464c-9177-b0be50b775fd"),
    final_episodes_with_length=Input(rid="ri.foundry.main.dataset.1da2194d-3770-4152-a468-8b4d0661ba86")
)
def final_merged_episodes(final_episodes_with_length, add_outcomes):
    """
    Merge episodes by checking for any overlap of episodes between the two algorithms.
    
    algo1 = HIP episodes
    algo2 = PPS episodes

    The following is for checking overlap:
    - complete overlap
    - algo1 contains algo2
    - algo2 contains algo1
    - start in algo1 is within algo2
    - start in algo1 is within algo2
    - start in algo2 is within algo1
    - end in algo1 is within algo2
    - end in algo2 is within algo1
    """
    algo1_pregnancy = final_episodes_with_length
    algo2 = add_outcomes

    # rename columns in algorithm 1
    algo1_pregnancy = algo1_pregnancy.withColumnRenamed('estimated_start_date','pregnancy_start')
    algo1_pregnancy = algo1_pregnancy.withColumnRenamed('visit_date','pregnancy_end')
    algo1_pregnancy = algo1_pregnancy.withColumnRenamed('gest_date','first_gest_date') 

    # add unique id for each gestation visit
    algo1_pregnancy = algo1_pregnancy.withColumn("algo1_id", F.concat(F.col("person_id"), F.lit("_"), F.col("episode"), F.lit("_1")))
    algo2 = algo2.withColumn("algo2_id", F.concat(F.col("personID"), F.lit("_"), F.col("person_episode_number"), F.lit("_2")))

    # join episodes
    all_episodes = algo1_pregnancy.join(algo2, (algo1_pregnancy.person_id == algo2.personID) & \
    (((algo1_pregnancy.pregnancy_start == algo2.episode_min_date) & (algo1_pregnancy.pregnancy_end == algo2.episode_max_date_plus_two_months)) | \
    ((algo1_pregnancy.pregnancy_start < algo2.episode_min_date) & (algo1_pregnancy.pregnancy_end > algo2.episode_max_date_plus_two_months)) | \
    ((algo1_pregnancy.pregnancy_start > algo2.episode_min_date) & (algo1_pregnancy.pregnancy_end < algo2.episode_max_date_plus_two_months)) | \
    ((algo1_pregnancy.pregnancy_start >= algo2.episode_min_date) & (algo1_pregnancy.pregnancy_start <= algo2.episode_max_date_plus_two_months)) | \
    ((algo2.episode_min_date >= algo1_pregnancy.pregnancy_start) & (algo2.episode_min_date <= algo1_pregnancy.pregnancy_end)) | \
    ((algo1_pregnancy.pregnancy_end >= algo2.episode_min_date) & (algo1_pregnancy.pregnancy_end <= algo2.episode_max_date_plus_two_months)) | \
    ((algo2.episode_max_date_plus_two_months >= algo1_pregnancy.pregnancy_start) & (algo2.episode_max_date_plus_two_months <= algo1_pregnancy.pregnancy_end))), \
    'outer')

    # add on the final episode date (not inferred pregnancy dates but actual dates the concepts occur in the data) from the merge of algorithm 1 and algorithm 2
    all_episodes = all_episodes.withColumn('merged_episode_start', F.least(F.col('first_gest_date'), F.col('episode_min_date'), F.col('pregnancy_end')))
    all_episodes = all_episodes.withColumn('merged_episode_end', F.greatest(F.col('episode_max_date'),F.col('pregnancy_end')))

    all_episodes = all_episodes.withColumn('merged_episode_length',F.months_between(F.col("merged_episode_end"), F.col("merged_episode_start")))

    # check for duplicated algorithm 1 episodes
    all_episodes = all_episodes.join(all_episodes.groupBy("algo1_id").agg((F.count("*") > 1).cast("int").alias("algo1_dup")), on=["algo1_id"], how="outer")
    print("Count of duplicated algorithm 1 episodes")
    all_episodes.filter(F.col("algo1_dup") != 0).select(F.countDistinct("algo1_id")).show()
        
    # check for duplicated algorithm 2 episodes
    all_episodes = all_episodes.join(all_episodes.groupBy("algo2_id").agg((F.count("*") > 1).cast("int").alias("algo2_dup")), on=["algo2_id"], how="outer")
    print("Count of duplicated algorithm 2 episodes")
    all_episodes.filter(F.col("algo2_dup") != 0).select(F.countDistinct("algo2_id")).show()

    print("Total number of episodes for Algorithm 1.")
    print(algo1_pregnancy.count())
    print("Total number of episodes for Algorithm 2.")
    print(algo2.count())
    print("Total number of Algorithm 1 episodes after merging.")
    all_episodes.select(F.countDistinct("algo1_id")).show()
    print("Total number of Algorithm 2 episodes after merging.")
    all_episodes.select(F.countDistinct("algo2_id")).show()

    return all_episodes

#################################################
## Global imports and functions included below ##
#################################################

from pyspark.sql import functions as F
 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3dc8d251-4a04-4ab8-ba77-b7464774ed3e"),
    final_merged_episodes_old=Input(rid="ri.foundry.main.dataset.39d38731-1dba-40fe-903e-95f91c4fb720")
)
def final_merged_episodes_no_duplicates(final_merged_episodes):
    """
    Remove any episodes that overlap with more than one episode. 

    1. Keep algorithm 1 episodes with an end date closest to algorithm 2's end date. Starting with duplicated algorithm 1 episodes, find the date difference in days between each algorithm's end date. Find the minimum date difference in days. If an algorithm 1 episode date difference in days does not equal the minimum date difference in days, flag that episode for removal by converting algorithm 1 episode info to null.
    2. Any remaining duplicated algorithm 1 episodes may have more than one algorithm 2 episodes with the same date difference in days. Calculate the length of algorithm 2 episodes and keep only the longest algorithm 2 episode. For any algorithm 2 episode that doesn't meet this criteria, both the algorithm 1 and 2 episode info are converted to null.
    3. Next repeat the same process described in Step 1 for duplicated algorithm 2 episodes.
    """

    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number

    df = final_merged_episodes
    df = df.withColumn("person_identifier", F.coalesce("person_id","personID"))
    df = df.drop("person_id","person_ID")
    df = df.withColumnRenamed("person_identifier","person_id")

    # filter to episodes without duplicate episode info
    no_dup_df = df.filter(((df.algo1_dup == 0) & (df.algo2_dup == 0)) | ((df.algo1_dup == 0) & (df.algo2_dup.isNull())) | ((df.algo1_dup.isNull()) & (df.algo2_dup == 0)))

    # filter to episodes with duplicate episode info
    dup_df = df.filter((df.algo1_dup != 0) | (df.algo2_dup != 0))

    # get difference in days between algorithm episode end dates
    dup_df = dup_df.withColumn("date_diff", F.abs(F.datediff(dup_df.pregnancy_end, dup_df.episode_max_date)))

    # flag algorithm 1 episode to remove - keep episode with the smallest difference in days between algorithm episode end dates
    dup_df = dup_df.join(dup_df.groupBy("algo1_id").agg(F.min("date_diff").alias("best_algo1")), on=["algo1_id"], how="outer")
    dup_df = dup_df.withColumn("remove_episode", F.when((F.col("algo1_dup") != 0) & (F.abs("date_diff") != F.col("best_algo1")), 1).otherwise(0))

    # convert to null values for any algorithm 1 episode flagged for removal
    dup_df = dup_df.withColumn("algo1_id", F.when(F.col("remove_episode") == 1, F.lit(None)).otherwise(F.col("algo1_id")))
    dup_df = dup_df.withColumn("pregnancy_end", F.when(F.col("remove_episode") == 1, F.lit(None)).otherwise(F.col("pregnancy_end")))
    dup_df = dup_df.withColumn("pregnancy_start", F.when(F.col("remove_episode") == 1, F.lit(None)).otherwise(F.col("pregnancy_start")))
    dup_df = dup_df.withColumn("first_gest_date", F.when(F.col("remove_episode") == 1, F.lit(None)).otherwise(F.col("first_gest_date")))
    dup_df = dup_df.withColumn("category", F.when(F.col("remove_episode") == 1, F.lit(None)).otherwise(F.col("category")))

    # for any algorithm 1 episodes with more than one algorithm 2 episodes that overlap, keep the algorithm 2 episode with the longer episode length
    # get difference in days between algorithm 2 episode start and end dates
    dup_df = dup_df.withColumn("new_date_diff", F.abs(F.datediff(dup_df.episode_max_date, dup_df.episode_min_date)))
    dup_df = dup_df.join(dup_df.groupBy("algo1_id").agg(F.max("new_date_diff").alias("longest_algo2")), on=["algo1_id"], how="outer")
    # flag episodes to remove
    dup_df = dup_df.withColumn("remove_episode_both", F.when((F.col("algo1_dup") != 0) & (F.col("new_date_diff") != F.col("longest_algo2")), 1).otherwise(0))

    # convert to null values for both episodes flagged to remove
    dup_df = dup_df.withColumn("algo1_id", F.when(F.col("remove_episode_both") == 1, F.lit(None)).otherwise(F.col("algo1_id")))
    dup_df = dup_df.withColumn("pregnancy_end", F.when(F.col("remove_episode_both") == 1, F.lit(None)).otherwise(F.col("pregnancy_end")))
    dup_df = dup_df.withColumn("pregnancy_start", F.when(F.col("remove_episode_both") == 1, F.lit(None)).otherwise(F.col("pregnancy_start")))
    dup_df = dup_df.withColumn("first_gest_date", F.when(F.col("remove_episode_both") == 1, F.lit(None)).otherwise(F.col("first_gest_date")))
    dup_df = dup_df.withColumn("category", F.when(F.col("remove_episode_both") == 1, F.lit(None)).otherwise(F.col("category")))
    dup_df = dup_df.withColumn("algo2_id", F.when(F.col("remove_episode_both") == 1, F.lit(None)).otherwise(F.col("algo2_id")))
    dup_df = dup_df.withColumn("episode_min_date", F.when(F.col("remove_episode_both") == 1, F.lit(None)).otherwise(F.col("episode_min_date")))
    dup_df = dup_df.withColumn("episode_max_date", F.when(F.col("remove_episode_both") == 1, F.lit(None)).otherwise(F.col("episode_max_date")))
    dup_df = dup_df.withColumn("algo2_category", F.when(F.col("remove_episode_both") == 1, F.lit(None)).otherwise(F.col("algo2_category")))
    dup_df = dup_df.withColumn("algo2_outcome_date", F.when(F.col("remove_episode_both") == 1, F.lit(None)).otherwise(F.col("algo2_outcome_date")))

    # recalculate difference in days between algorithm episode end dates
    dup_df = dup_df.withColumn("date_diff", F.abs(F.datediff(dup_df.pregnancy_end, dup_df.episode_max_date)))
    # flag algorithm 2 episode to remove
    dup_df = dup_df.join(dup_df.groupBy("algo2_id").agg(F.min("date_diff").alias("best_algo2")), on=["algo2_id"], how="outer")
    dup_df = dup_df.withColumn("remove_episode2", F.when((F.col("algo2_dup") != 0) & (F.abs("date_diff") != F.col("best_algo2")), 1).otherwise(0))
    # remove any duplicated rows with null info for algorithm 1
    dup_df = dup_df.withColumn("remove_episode2", F.when((F.col("algo2_dup") != 0) & (F.col("pregnancy_end").isNull()), 1).otherwise(F.col("remove_episode2")))

    # convert to null values for any algorithm 2 episode flagged to remove
    dup_df = dup_df.withColumn("algo2_id", F.when(F.col("remove_episode2") == 1, F.lit(None)).otherwise(F.col("algo2_id")))
    dup_df = dup_df.withColumn("episode_min_date", F.when(F.col("remove_episode2") == 1, F.lit(None)).otherwise(F.col("episode_min_date")))
    dup_df = dup_df.withColumn("episode_max_date", F.when(F.col("remove_episode2") == 1, F.lit(None)).otherwise(F.col("episode_max_date")))
    dup_df = dup_df.withColumn("algo2_category", F.when(F.col("remove_episode2") == 1, F.lit(None)).otherwise(F.col("algo2_category")))
    dup_df = dup_df.withColumn("algo2_outcome_date", F.when(F.col("remove_episode2") == 1, F.lit(None)).otherwise(F.col("algo2_outcome_date")))

    # if any remaining duplicated rows, keep the latter max episode date
    dup_df = dup_df.join(dup_df.groupBy("algo2_id").agg(F.max("pregnancy_end").alias("max_algo2")), on=["algo2_id"], how="outer")
    dup_df = dup_df.withColumn("remove_episode2_other", F.when((F.col("algo2_dup") != 0) & (F.col("pregnancy_end") != F.col("max_algo2")), 1).otherwise(0))

    # convert to null values for any algorithm 2 episode flagged to remove
    dup_df = dup_df.withColumn("algo2_id", F.when(F.col("remove_episode2_other") == 1, F.lit(None)).otherwise(F.col("algo2_id")))
    dup_df = dup_df.withColumn("episode_min_date", F.when(F.col("remove_episode2_other") == 1, F.lit(None)).otherwise(F.col("episode_min_date")))
    dup_df = dup_df.withColumn("episode_max_date", F.when(F.col("remove_episode2_other") == 1, F.lit(None)).otherwise(F.col("episode_max_date")))
    dup_df = dup_df.withColumn("algo2_category", F.when(F.col("remove_episode2_other") == 1, F.lit(None)).otherwise(F.col("algo2_category")))
    dup_df = dup_df.withColumn("algo2_outcome_date", F.when(F.col("remove_episode2_other") == 1, F.lit(None)).otherwise(F.col("algo2_outcome_date")))

    # select columns to keep and drop any duplicate rows
    dup_df = dup_df.select("algo1_id","algo2_id","person_id","pregnancy_end","pregnancy_start","first_gest_date","category","episode_min_date","episode_max_date","algo2_category","algo2_outcome_date").drop_duplicates()
    # drop any rows with duplicate algorithm 1 episodes due to algorithm 2 episodes sharing the same dates
    dup_df = dup_df.dropDuplicates(["person_id","algo1_id","episode_min_date","episode_max_date"])

    # check for any duplicated episodes
    dup_df = dup_df.join(dup_df.groupBy("algo1_id").agg((F.count("*") > 1).cast("int").alias("algo1_dup")), on=["algo1_id"], how="outer")
    dup_df = dup_df.join(dup_df.groupBy("algo2_id").agg((F.count("*") > 1).cast("int").alias("algo2_dup")), on=["algo2_id"], how="outer")
    print("Count of duplicated algorithm 1 episodes")
    dup_df.filter(F.col("algo1_dup") != 0).select(F.countDistinct("algo1_id")).show()
    print("Count of duplicated algorithm 2 episodes")
    dup_df.filter(F.col("algo2_dup") != 0).select(F.countDistinct("algo2_id")).show()

    # join tables
    final_df = dup_df.unionByName(no_dup_df.select("algo1_id","algo2_id","person_id","pregnancy_end","pregnancy_start","first_gest_date","category","episode_min_date","episode_max_date","algo1_dup","algo2_dup","algo2_category","algo2_outcome_date"))

    # recalculate merged dates, episode number, and episode length
    final_df = final_df.withColumn("merged_episode_start", F.least(F.col("first_gest_date"), F.col("episode_min_date"), F.col("pregnancy_end")))
    final_df = final_df.withColumn("merged_episode_end", F.greatest(F.col("episode_max_date"),F.col("pregnancy_end")))
    final_df = final_df.withColumn("episode_num", row_number().over(Window.partitionBy("person_id").orderBy("person_id","merged_episode_start")))
    final_df = final_df.withColumn("merged_episode_length", F.months_between(F.col("merged_episode_end"), F.col("merged_episode_start")))

    # remove any rows with null values for person_id
    final_df = final_df.filter(~(F.col("merged_episode_start").isNull()))

    print("Total number of Algorithm 1 episodes after clean up.")
    final_df.select(F.countDistinct("algo1_id")).show()
    print("Total number of Algorithm 2 episodes after clean up.")
    final_df.select(F.countDistinct("algo2_id")).show()

    return final_df

#################################################
## Global imports and functions included below ##
#################################################

from pyspark.sql import functions as F
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.bcba58af-82f9-4611-a35f-a2ce80e99e75"),
    final_merged_episodes_no_duplicates_old=Input(rid="ri.foundry.main.dataset.3dc8d251-4a04-4ab8-ba77-b7464774ed3e"),
    person=Input(rid="ri.foundry.main.dataset.14c52391-0344-41ac-909a-71f8e19704d6")
)
def final_merged_episode_detailed(final_merged_episodes_no_duplicates, person):
    """
    Add demographic details for each patient.
    """
    df = final_merged_episodes_no_duplicates
    person_df = person

    df = df.withColumnRenamed("pregnancy_end","HIP_end_date")
    df = df.withColumnRenamed("category","HIP_outcome_category")
    df = df.withColumnRenamed("algo2_category","PPS_outcome_category")
    df = df.withColumnRenamed("algo2_outcome_date","PPS_end_date")
    df = df.withColumnRenamed("merged_episode_start","recorded_episode_start")
    df = df.withColumnRenamed("merged_episode_end","recorded_episode_end")
    df = df.withColumnRenamed("merged_episode_length","recorded_episode_length")

    # add columns marking if episode was identified either algorithm
    df = df.withColumn("HIP_flag", F.when(F.col("algo1_id").isNotNull(), 1).otherwise(0))
    df = df.withColumn("PPS_flag", F.when(F.col("algo2_id").isNotNull(), 1).otherwise(0))

    # join tables
    df = df.join(person_df,'person_id','left')

    df = df.select('person_id','data_partner_id','year_of_birth','month_of_birth','race_concept_name','ethnicity_concept_name','location_id','HIP_flag','PPS_flag','HIP_outcome_category','HIP_end_date','PPS_outcome_category','PPS_end_date','recorded_episode_start','recorded_episode_end','recorded_episode_length')

    # add date of birth
    df = df.withColumn("date_of_birth",F.concat_ws("-", F.col("year_of_birth"), F.col("month_of_birth"), F.lit(1)).cast("date"))

    # renumber episodes
    final_df = df.withColumn("episode_number", row_number().over(Window.partitionBy("person_id").orderBy("person_id","recorded_episode_start")))
    
    return final_df
    
#################################################
## Global imports and functions included below ##
#################################################

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
