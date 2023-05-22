import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DateType, StringType
from pyspark.sql.window import Window
import pandas as pd

# external files
HIP_concepts = "HIP_concepts.xlsx"
Matcho_outcome_limits = "Matcho_outcome_limits.xlsx"
Matcho_term_durations = "Matcho_term_durations.xlsx"

def initial_pregnant_cohort(procedure_occurrence, measurement, observation, condition_occurrence, HIP_concepts, person):
    """
    Get concepts specific for pregnancy from domain tables.
    """
    # concepts
    preg_df = HIP_concepts

    # filter observation table
    observation_df = observation.join(preg_df, (preg_df.concept_id == observation.observation_concept_id), "inner")
    # select columns
    observation_df = observation_df.select("person_id","observation_concept_id","observation_concept_name","data_partner_id","observation_date","category","gest_value","value_as_number")
    # rename columns
    observation_df = observation_df.withColumnRenamed("observation_concept_id", "concept_id")
    observation_df = observation_df.withColumnRenamed("observation_concept_name", "concept_name")
    observation_df = observation_df.withColumnRenamed("observation_date", "visit_date")

    # filter measurement table
    measurement_df = measurement.join(preg_df, (preg_df.concept_id == measurement.measurement_concept_id), "inner")
    # select columns
    measurement_df = measurement_df.select("person_id","measurement_concept_id","measurement_concept_name","data_partner_id","measurement_date","category","gest_value","value_as_number")
    # rename columns
    measurement_df = measurement_df.withColumnRenamed("measurement_concept_id", "concept_id")
    measurement_df = measurement_df.withColumnRenamed("measurement_concept_name", "concept_name")
    measurement_df = measurement_df.withColumnRenamed("measurement_date", "visit_date")

    # filter procedure table
    procedure_df = procedure_occurrence.join(preg_df, (preg_df.concept_id == procedure_occurrence.procedure_concept_id), "inner")
    # select columns
    procedure_df = procedure_df.select("person_id","procedure_concept_id","procedure_concept_name","data_partner_id","procedure_date","category","gest_value")
    # rename columns
    procedure_df = procedure_df.withColumnRenamed("procedure_concept_id", "concept_id")
    procedure_df = procedure_df.withColumnRenamed("procedure_concept_name", "concept_name")
    procedure_df = procedure_df.withColumnRenamed("procedure_date", "visit_date")
    # add column
    procedure_df = procedure_df.withColumn("value_as_number", F.lit(None).cast(IntegerType()))

    # filter condition table
    condition_df = condition_occurrence.join(preg_df, (preg_df.concept_id == condition_occurrence.condition_concept_id), "inner")
    # select columns
    condition_df = condition_df.select("person_id","condition_concept_id","condition_concept_name","data_partner_id","condition_start_date","category","gest_value")
     # rename columns
    condition_df = condition_df.withColumnRenamed("condition_concept_id", "concept_id")
    condition_df = condition_df.withColumnRenamed("condition_concept_name", "concept_name")
    condition_df = condition_df.withColumnRenamed("condition_start_date", "visit_date")
    # add column
    condition_df = condition_df.withColumn("value_as_number", F.lit(None).cast(IntegerType()))

    # combine tables
    union_df = measurement_df.union(procedure_df)
    union_df = union_df.union(observation_df)
    union_df = union_df.union(condition_df)

    # get unique person ids for women of reproductive age    
    person_df = person
    person_df = person_df.filter(person_df.gender_concept_name == "FEMALE")
    person_df = person_df.filter((person_df.year_of_birth >= 1965) & (person_df.year_of_birth <= 2005))
    women_reproductive_age = person_df.select("person_id", "year_of_birth","race_concept_name","ethnicity_concept_name").distinct()

    # keep only person_ids of women of reproductive age 
    union_df = union_df.join(women_reproductive_age, "person_id", "inner")
    # drop duplicates
    union_df = union_df.dropDuplicates()
    # sort by person_id and visit_date
    union_df = union_df.orderBy(["person_id","visit_date"])

    # filter out any dates before 1/1/2018
    union_df = union_df.filter(F.col("visit_date") > "2018-01-01")

    # filter out any dates after dataset version
    union_df = union_df.filter(F.col("visit_date") <= "2022-04-07")

    return union_df

def stillbirth_visits(initial_pregnant_cohort):
    """
    Get all stillbirth visits.
    """
    df = initial_pregnant_cohort
    df = df.filter(df.category == "SB")
    # drop duplicate rows on person_id and visit_date
    df = df.dropDuplicates(["person_id", "visit_date"])
    return df

def stillbirth_temp(stillbirth_visits):
    """
    Get number of days between each visit.
    """
    df = stillbirth_visits

    df = df.withColumn("days", F.datediff(df.visit_date, F.lag(df.visit_date, 1).over(Window.partitionBy("person_id").orderBy("visit_date"))))
 
    return df

def final_stillbirth_visits(stillbirth_temp, stillbirth_visits, Matcho_outcome_limits):
    """
    Get earliest stillbirth visit per patient and any stillbirth visit after minimum outcome days.
    """
    temp_df = stillbirth_temp
    df = stillbirth_visits

    outcome_df = Matcho_outcome_limits

    # get minimum days between outcomes
    min_day = outcome_df.filter((outcome_df.first_preg_category=="SB") & (outcome_df.outcome_preg_category=="SB")).select("min_days").collect()[0]["min_days"]

    # rename columns
    temp_df = temp_df.withColumnRenamed("person_id","temp_person_id")
    temp_df = temp_df.withColumnRenamed("visit_date","temp_visit_date")

    # identify first visit
    first_visits = temp_df.groupBy("temp_person_id").agg(F.min("temp_visit_date").alias("delivery_date"))

    # get first visit per patient
    first_df = df.join(first_visits, (df.person_id==first_visits.temp_person_id) & (df.visit_date==first_visits.delivery_date),"inner").drop("temp_person_id","delivery_date")

    # identify other visits with at least minimum outcome days 
    other_visits = temp_df.filter(temp_df.days >= min_day).select("temp_person_id","temp_visit_date")
    
    # get other visits per patient
    other_df = df.join(other_visits, (df.person_id==other_visits.temp_person_id) & (df.visit_date==other_visits.temp_visit_date),"inner").drop("temp_person_id","temp_visit_date")

    # merge dataframes and drop any duplicate rows
    all_df = first_df.union(other_df)
    all_df = all_df.dropDuplicates()

    # sort by person_id and visit_date
    all_df = all_df.orderBy(["person_id","visit_date"])

    print("Total number of episodes.")
    print(all_df.count())

    return all_df
    
def livebirth_visits(initial_pregnant_cohort):
    """
    Get all livebirth visits.
    """
    df = initial_pregnant_cohort
    df = df.filter(df.category == "LB")
    # drop duplicate rows on person_id and visit_date
    df = df.dropDuplicates(["person_id", "visit_date"])
    return df

def livebirth_temp(livebirth_visits):
    """
    Get number of days between each visit.
    """
    
    df = livebirth_visits

    df = df.withColumn("days", F.datediff(df.visit_date, F.lag(df.visit_date, 1).over(Window.partitionBy("person_id").orderBy("visit_date"))))
 
    return df

def final_livebirth_visits(livebirth_temp, livebirth_visits, Matcho_outcome_limits):
    """
    Get earliest livebirth visit per patient and any livebirth visit after minimum outcome days.
    """

    temp_df = livebirth_temp
    df = livebirth_visits

    outcome_df = Matcho_outcome_limits
    
    # get minimum days between outcomes
    min_day = outcome_df.filter((outcome_df.first_preg_category=="LB") & (outcome_df.outcome_preg_category=="LB")).select("min_days").collect()[0]["min_days"]

    # rename columns
    temp_df = temp_df.withColumnRenamed("person_id","temp_person_id")
    temp_df = temp_df.withColumnRenamed("visit_date","temp_visit_date")

    # identify first visit
    first_visits = temp_df.groupBy("temp_person_id").agg(F.min("temp_visit_date").alias("delivery_date"))

    # get first visit per patient
    first_df = df.join(first_visits, (df.person_id==first_visits.temp_person_id) & (df.visit_date==first_visits.delivery_date),"inner").drop("temp_person_id","delivery_date")

    # identify other visits with at least minimum outcome days 
    other_visits = temp_df.filter(temp_df.days >= min_day).select("temp_person_id","temp_visit_date")
    
    # get other visits per patient
    other_df = df.join(other_visits, (df.person_id==other_visits.temp_person_id) & (df.visit_date==other_visits.temp_visit_date),"inner").drop("temp_person_id","temp_visit_date")

    # merge dataframes and drop any duplicate rows
    all_df = first_df.union(other_df)
    all_df = all_df.dropDuplicates()

    # sort by person_id and visit_date
    all_df = all_df.orderBy(["person_id","visit_date"])

    print("Total number of episodes.")
    print(all_df.count())

    return all_df

def ectopic_visits(initial_pregnant_cohort):
    """
    Get ectopic pregnancy visits.
    """
    df = initial_pregnant_cohort
    df = df.filter(df.category=="ECT")
    # drop duplicate rows on person_id and visit_date
    df = df.dropDuplicates(["person_id", "visit_date"])
    return df
    
def ectopic_temp(ectopic_visits):
    """
    Get number of days between each visit.
    """    
    df = ectopic_visits

    df = df.withColumn("days", F.datediff(df.visit_date, F.lag(df.visit_date, 1).over(Window.partitionBy("person_id").orderBy("visit_date"))))
 
    return df

def final_ectopic_visits(ectopic_temp, ectopic_visits, Matcho_outcome_limits):
    """
    Get earliest ectopic pregnancy visit per patient and any ectopic pregnancy visit after minimum outcome days.
    """

    temp_df = ectopic_temp
    df = ectopic_visits

    outcome_df = Matcho_outcome_limits

    # get minimum days between outcomes
    min_day = outcome_df.filter((outcome_df.first_preg_category=="ECT") & (outcome_df.outcome_preg_category=="ECT")).select("min_days").collect()[0]["min_days"]

    # rename columns
    temp_df = temp_df.withColumnRenamed("person_id","temp_person_id")
    temp_df = temp_df.withColumnRenamed("visit_date","temp_visit_date")

    # identify first visit
    first_visits = temp_df.groupBy("temp_person_id").agg(F.min("temp_visit_date").alias("delivery_date"))

    # get first visit per patient
    first_df = df.join(first_visits, (df.person_id==first_visits.temp_person_id) & (df.visit_date==first_visits.delivery_date),"inner").drop("temp_person_id","delivery_date")

    # identify other visits with at least minimum outcome days 
    other_visits = temp_df.filter(temp_df.days >= min_day).select("temp_person_id","temp_visit_date")
    
    # get other visits per patient
    other_df = df.join(other_visits, (df.person_id==other_visits.temp_person_id) & (df.visit_date==other_visits.temp_visit_date),"inner").drop("temp_person_id","temp_visit_date")

    # merge dataframes and drop any duplicate rows
    all_df = first_df.union(other_df)
    all_df = all_df.dropDuplicates()

    # sort by person_id and visit_date
    all_df = all_df.orderBy(["person_id","visit_date"])

    print("Total number of episodes.")
    print(all_df.count())

    return all_df

def delivery_visits(initial_pregnant_cohort):
    """
    Get all delivery record visits only.
    """
    df = initial_pregnant_cohort
    df = df.filter(df.category == "DELIV")
    # drop duplicate rows on person_id and visit_date
    df = df.dropDuplicates(["person_id", "visit_date"])
    return df
    
def delivery_temp(delivery_visits):
    """
    Get number of days between each visit.
    """
    df = delivery_visits

    df = df.withColumn("days", F.datediff(df.visit_date, F.lag(df.visit_date, 1).over(Window.partitionBy("person_id").orderBy("visit_date"))))
 
    return df

def final_delivery_visits(delivery_temp, delivery_visits, Matcho_outcome_limits):
    """
    Get earliest delivery visit per patient and any delivery visit after minimum outcome days.
    """
    temp_df = delivery_temp
    df = delivery_visits

    outcome_df = Matcho_outcome_limits

    # get minimum days between outcomes
    min_day = outcome_df.filter((outcome_df.first_preg_category=="DELIV") & (outcome_df.outcome_preg_category=="DELIV")).select("min_days").collect()[0]["min_days"]

    # rename columns
    temp_df = temp_df.withColumnRenamed("person_id","temp_person_id")
    temp_df = temp_df.withColumnRenamed("visit_date","temp_visit_date")

    # identify first visit
    first_visits = temp_df.groupBy("temp_person_id").agg(F.min("temp_visit_date").alias("delivery_date"))

    # get first visit per patient
    first_df = df.join(first_visits, (df.person_id==first_visits.temp_person_id) & (df.visit_date==first_visits.delivery_date),"inner").drop("temp_person_id","delivery_date")

    # identify other visits with at least minimum outcome days 
    other_visits = temp_df.filter(temp_df.days >= min_day).select("temp_person_id","temp_visit_date")
    
    # get other visits per patient
    other_df = df.join(other_visits, (df.person_id==other_visits.temp_person_id) & (df.visit_date==other_visits.temp_visit_date),"inner").drop("temp_person_id","temp_visit_date")

    # merge dataframes and drop any duplicate rows
    all_df = first_df.union(other_df)
    all_df = all_df.dropDuplicates()

    # sort by person_id and visit_date
    all_df = all_df.orderBy(["person_id","visit_date"])

    print("Total number of episodes.")
    print(all_df.count())

    return all_df

def abortion_visits(initial_pregnant_cohort):
    """
    Get abortion visits - AB (abortion) and SA (spontaneous abortion).
    """
    df = initial_pregnant_cohort
    df = df.filter(df.category.isin(["AB","SA"]))
    # drop duplicate rows on person_id and visit_date
    df = df.dropDuplicates(["person_id", "visit_date"])
    return df

def abortion_temp(abortion_visits):
    """
    Get number of days between each visit.
    """
    
    df = abortion_visits

    df = df.withColumn("days", F.datediff(df.visit_date, F.lag(df.visit_date, 1).over(Window.partitionBy("person_id").orderBy("visit_date"))))
 
    return df

def final_abortion_visits(abortion_temp, abortion_visits, Matcho_outcome_limits):
    """
    Get earliest abortion visit per patient and any abortion visit after minimum outcome days.
    """

    temp_df = abortion_temp
    df = abortion_visits

    outcome_df = Matcho_outcome_limits

    # get minimum days between outcomes
    min_day = outcome_df.filter((outcome_df.first_preg_category=="AB") & (outcome_df.outcome_preg_category=="AB")).select("min_days").collect()[0]["min_days"]

    # rename columns
    temp_df = temp_df.withColumnRenamed("person_id","temp_person_id")
    temp_df = temp_df.withColumnRenamed("visit_date","temp_visit_date")

    # identify first visit
    first_visits = temp_df.groupBy("temp_person_id").agg(F.min("temp_visit_date").alias("delivery_date"))

    # get first visit per patient
    first_df = df.join(first_visits, (df.person_id==first_visits.temp_person_id) & (df.visit_date==first_visits.delivery_date),"inner").drop("temp_person_id","delivery_date")

    # identify other visits with at least minimum outcome days 
    other_visits = temp_df.filter(temp_df.days >= min_day).select("temp_person_id","temp_visit_date")
    
    # get other visits per patient
    other_df = df.join(other_visits, (df.person_id==other_visits.temp_person_id) & (df.visit_date==other_visits.temp_visit_date),"inner").drop("temp_person_id","temp_visit_date")

    # merge dataframes and drop any duplicate rows
    all_df = first_df.union(other_df)
    all_df = all_df.dropDuplicates()

    # sort by person_id and visit_date
    all_df = all_df.orderBy(["person_id","visit_date"])

    print("Total number of episodes.")
    print(all_df.count())

    return all_df
                            
def add_stillbirth(final_stillbirth_visits, final_livebirth_visits, Matcho_outcome_limits):
    """
    Add stillbirth visits to livebirth visits table.
    """
    outcome_df = Matcho_outcome_limits
    
    # get minimum days between outcomes
    before_min = outcome_df.filter((outcome_df.first_preg_category=="LB") & (outcome_df.outcome_preg_category=="SB")).select("min_days").collect()[0]["min_days"]    
    after_min = outcome_df.filter((outcome_df.first_preg_category=="SB") & (outcome_df.outcome_preg_category=="LB")).select("min_days").collect()[0]["min_days"]

    # drop unneccessary columns
    stillbirth_df = final_stillbirth_visits.drop("gest_value","value_as_number")
    livebirth_df = final_livebirth_visits.drop("gest_value","value_as_number")

    # join both tables
    temp_df = livebirth_df.union(stillbirth_df)   
 
    # get difference in days with subsequent visit
    temp_df = temp_df.withColumn("after_days", F.datediff(temp_df.visit_date, F.lag(temp_df.visit_date, 1).over(Window.partitionBy("person_id").orderBy("visit_date"))))

    # get previous category if available
    temp_df = temp_df.withColumn("previous_category", F.lag(temp_df.category, 1).over(Window.partitionBy("person_id").orderBy("visit_date")))

    # get difference in days with previous visit
    temp_df = temp_df.withColumn("before_days", F.datediff(temp_df.visit_date, F.lag(temp_df.visit_date, -1).over(Window.partitionBy("person_id").orderBy("visit_date"))))

    # get subsequent category if available
    temp_df = temp_df.withColumn("next_category", F.lag(temp_df.category, -1).over(Window.partitionBy("person_id").orderBy("visit_date")))

    # filter to stillbirth visits
    temp_outcome_df = temp_df.filter(temp_df.category=="SB")
 
    # keep visits with days containing null values - indicates single event
    temp_null_df = temp_outcome_df.filter((temp_outcome_df.before_days.isNull()) & (temp_outcome_df.after_days.isNull()))

    # keep visits not preceded by and/or followed by LB only
    temp_sb_prev_df = temp_outcome_df.filter(~(temp_outcome_df.previous_category=="LB"))
    temp_sb_prev_df = temp_sb_prev_df.filter(temp_sb_prev_df.next_category.isNull())
    temp_sb_next_df = temp_outcome_df.filter(~(temp_outcome_df.next_category=="LB"))
    temp_sb_next_df = temp_sb_next_df.filter(temp_sb_next_df.previous_category.isNull())
    temp_sb_both_df = temp_outcome_df.filter(~(temp_outcome_df.previous_category=="LB"))
    temp_sb_both_df = temp_sb_both_df.filter(~(temp_sb_both_df.next_category=="LB"))
    # merge tables
    temp_sb_df = temp_sb_prev_df.union(temp_sb_next_df)
    temp_sb_df = temp_sb_df.union(temp_sb_both_df)

    # identify visits with at least minimum days between outcomes

    # record is preceded by livebirth only
    temp_before_df = temp_outcome_df.filter((temp_outcome_df.previous_category=="LB") & 
                                            (temp_outcome_df.after_days >= before_min) &
                                            (temp_outcome_df.next_category.isNull()))

    # record is followed by livebirth only
    temp_after_df = temp_outcome_df.filter((temp_outcome_df.next_category=="LB") & 
                                            (F.abs(temp_outcome_df.before_days) >= after_min) &
                                            (temp_outcome_df.previous_category.isNull()))

    # record is both preceded and followed by livebirth
    temp_both_df = temp_outcome_df.filter((temp_outcome_df.next_category=="LB") & 
                                            (F.abs(temp_outcome_df.before_days) >= after_min) &
                                            (temp_outcome_df.previous_category=="LB") & 
                                            (temp_outcome_df.after_days >= before_min))

    # combine tables
    final_temp_df = temp_after_df.union(temp_before_df)
    final_temp_df = final_temp_df.union(temp_both_df)
    final_temp_df = final_temp_df.union(temp_null_df)
    final_temp_df = final_temp_df.union(temp_sb_df)
    
    # list of columns to drop
    drop_list = ["previous_category","next_category","before_days","after_days"]
    # combine with livebirth table and drop columns 
    final_df =  livebirth_df.union(final_temp_df.drop(*drop_list)) 

    # drop any duplicate rows
    final_df = final_df.dropDuplicates()

    print("Total number of episodes in live birth table.")
    print(final_livebirth_visits.count())
    print("Total number of live birth episodes.")
    print(final_df.filter(final_df.category == "LB").count())
    print("Total number of stillbirth episodes.")
    print(final_df.filter(final_df.category == "SB").count())

    return final_df

def add_ectopic(add_stillbirth, Matcho_outcome_limits, final_ectopic_visits):
    """
    Add ectopic pregnancy visits.
    """    
    outcome_df = Matcho_outcome_limits

    # get minimum days between outcomes
    # minimum number of days that ECT can follow LB and SB; LB and SB have the same days 
    before_min = outcome_df.filter((outcome_df.first_preg_category=="LB") & 
        (outcome_df.outcome_preg_category=="ECT")).select("min_days").collect()[0]["min_days"]
    # minimum number of days that LB can follow ECT
    after_min_lb = outcome_df.filter((outcome_df.first_preg_category=="ECT") & 
        (outcome_df.outcome_preg_category=="LB")).select("min_days").collect()[0]["min_days"]
    # minimum number of days that SB can follow ECT
    after_min_sb = outcome_df.filter((outcome_df.first_preg_category=="ECT") & 
        (outcome_df.outcome_preg_category=="SB")).select("min_days").collect()[0]["min_days"]

    # drop unneccessary columns
    ect_df = final_ectopic_visits.drop("gest_value","value_as_number")

    # join both tables
    pregnant_df = add_stillbirth
    temp_df = pregnant_df.union(ect_df)
   
    # get difference in days with subsequent visit
    temp_df = temp_df.withColumn("after_days", F.datediff(temp_df.visit_date, F.lag(temp_df.visit_date, 1).over(Window.partitionBy("person_id").orderBy("visit_date"))))

    # get previous category if available
    temp_df = temp_df.withColumn("previous_category", F.lag(temp_df.category, 1).over(Window.partitionBy("person_id").orderBy("visit_date")))

    # get difference in days with previous visit
    temp_df = temp_df.withColumn("before_days", F.datediff(temp_df.visit_date, F.lag(temp_df.visit_date, -1).over(Window.partitionBy("person_id").orderBy("visit_date"))))

    # get subsequent category if available
    temp_df = temp_df.withColumn("next_category", F.lag(temp_df.category, -1).over(Window.partitionBy("person_id").orderBy("visit_date")))

    # filter to ectopic visits
    temp_outcome_df = temp_df.filter(temp_df.category=="ECT")

    # keep visits with days containing null values - indicates single event
    temp_null_df = temp_outcome_df.filter((temp_outcome_df.before_days.isNull()) & (temp_outcome_df.after_days.isNull()))

    # keep visits not preceded by or followed by LB or SB 
    temp_ect_prev_df = temp_outcome_df.filter(~(temp_outcome_df.previous_category.isin(["LB","SB"])))
    temp_ect_prev_df = temp_ect_prev_df.filter(temp_ect_prev_df.next_category.isNull())
    temp_ect_next_df = temp_outcome_df.filter(~(temp_outcome_df.next_category.isin(["LB","SB"])))
    temp_ect_next_df = temp_ect_next_df.filter(temp_ect_next_df.previous_category.isNull())
    temp_ect_both_df = temp_outcome_df.filter(~(temp_outcome_df.previous_category.isin(["LB","SB"])))
    temp_ect_both_df = temp_ect_both_df.filter(~(temp_ect_both_df.next_category.isin(["LB","SB"])))
    temp_ect_df = temp_ect_prev_df.union(temp_ect_next_df)
    temp_ect_df = temp_ect_df.union(temp_ect_both_df)

    # record is preceded by livebirth/stillbirth only
    temp_before_df = temp_outcome_df.filter((temp_outcome_df.previous_category.isin(["LB","SB"])) & 
                                            (temp_outcome_df.after_days >= before_min) &
                                            (temp_outcome_df.next_category.isNull()))

    # record is followed by livebirth only
    temp_lb_after_df = temp_outcome_df.filter((temp_outcome_df.next_category=="LB") & 
                                            (F.abs(temp_outcome_df.before_days) >= after_min_lb) &
                                            (temp_outcome_df.previous_category.isNull()))

    # record is followed by stillbirth only
    temp_sb_after_df = temp_outcome_df.filter((temp_outcome_df.next_category=="SB") & 
                                            (F.abs(temp_outcome_df.before_days) >= after_min_sb) &
                                            (temp_outcome_df.previous_category.isNull()))

    # record is followed by LB and preceded by livebirth/stillbirth
    temp_both_lb_df = temp_outcome_df.filter((temp_outcome_df.next_category=="LB") & 
                                            (F.abs(temp_outcome_df.before_days) >= after_min_lb) &
                                            (temp_outcome_df.previous_category.isin(["LB","SB"])) & 
                                            (temp_outcome_df.after_days >= before_min))

    # record is followed by SB and preceded by livebirth/stillbirth
    temp_both_sb_df = temp_outcome_df.filter((temp_outcome_df.next_category=="SB") & 
                                            (F.abs(temp_outcome_df.before_days) >= after_min_sb) &
                                            (temp_outcome_df.previous_category.isin(["LB","SB"])) & 
                                            (temp_outcome_df.after_days >= before_min))

    # combine tables and drop columns
    drop_list = ["previous_category","next_category","before_days","after_days"]
    final_temp_df = temp_null_df.union(temp_before_df)
    final_temp_df = final_temp_df.union(temp_lb_after_df)
    final_temp_df = final_temp_df.union(temp_sb_after_df)
    final_temp_df = final_temp_df.union(temp_both_lb_df)
    final_temp_df = final_temp_df.union(temp_both_sb_df)
    final_temp_df = final_temp_df.union(temp_ect_df)
    final_df = pregnant_df.union(final_temp_df.drop(*drop_list))

    # drop any duplicate rows
    final_df = final_df.dropDuplicates()

    print("Total number of live birth episodes.")
    print(final_df.filter(final_df.category == "LB").count())
    print("Total number of stillbirth episodes.")
    print(final_df.filter(final_df.category == "SB").count())
    print("Total number of ectopic episodes")
    print(final_df.filter(final_df.category == "ECT").count())

    return final_df

def add_abortion(add_ectopic, Matcho_outcome_limits, final_abortion_visits):
    """
    Add abortion visits - SA and AB are treated the same.
    """    
    outcome_df = Matcho_outcome_limits
    
    # get minimum days between each outcome 
    # minimum number of days that AB/SA can follow LB and SB; LB and SB have the same days
    before_min_lb = outcome_df.filter((outcome_df.first_preg_category=="LB") & 
        (outcome_df.outcome_preg_category=="AB")).select("min_days").collect()[0]["min_days"]
    # minimum number of days that AB/SA can follow ECT     
    before_min_ect = outcome_df.filter((outcome_df.first_preg_category=="ECT") & 
        (outcome_df.outcome_preg_category=="AB")).select("min_days").collect()[0]["min_days"]
    # minimum number of days that LB can follow AB/SA     
    after_min_lb = outcome_df.filter((outcome_df.first_preg_category=="AB") & 
        (outcome_df.outcome_preg_category=="LB")).select("min_days").collect()[0]["min_days"]
    # minimum number of days that SB can follow AB/SA 
    after_min_sb = outcome_df.filter((outcome_df.first_preg_category=="AB") & 
        (outcome_df.outcome_preg_category=="SB")).select("min_days").collect()[0]["min_days"]
    # minimum number of days that ECT can follow AB/SA
    after_min_ect = outcome_df.filter((outcome_df.first_preg_category=="AB") & 
        (outcome_df.outcome_preg_category=="ECT")).select("min_days").collect()[0]["min_days"]

    # drop unneccessary columns
    abortion_df = final_abortion_visits.drop("gest_value","value_as_number")

    # join both tables
    pregnant_df = add_ectopic
    temp_df = pregnant_df.union(abortion_df)

    # create temporary category column and rename all categories with SA to AB
    temp_df = temp_df.withColumn("temp_category", F.when((F.col("category") == "SA"), "AB").otherwise(F.col("category")))
   
    # get difference in days with subsequent visit
    temp_df = temp_df.withColumn("after_days", F.datediff(temp_df.visit_date, F.lag(temp_df.visit_date, 1).over(Window.partitionBy("person_id").orderBy("visit_date"))))

    # get previous category if available
    temp_df = temp_df.withColumn("previous_category", F.lag(temp_df.temp_category, 1).over(Window.partitionBy("person_id").orderBy("visit_date")))

    # get difference in days with previous visit
    temp_df = temp_df.withColumn("before_days", F.datediff(temp_df.visit_date, F.lag(temp_df.visit_date, -1).over(Window.partitionBy("person_id").orderBy("visit_date"))))

    # get subsequent category if available
    temp_df = temp_df.withColumn("next_category", F.lag(temp_df.temp_category, -1).over(Window.partitionBy("person_id").orderBy("visit_date")))

    # filter to abortion visits
    temp_outcome_df = temp_df.filter(temp_df.temp_category == "AB")

    # keep visits with days containing null values - indicates single event
    temp_null_df = temp_outcome_df.filter((temp_outcome_df.before_days.isNull()) & (temp_outcome_df.after_days.isNull()))

    # keep visits not preceded by or followed by LB or SB or ECT 
    temp_ab_prev_df = temp_outcome_df.filter(~(temp_outcome_df.previous_category.isin(["LB","SB","ECT"])))
    temp_ab_prev_df = temp_ab_prev_df.filter(temp_ab_prev_df.next_category.isNull())
    temp_ab_next_df = temp_outcome_df.filter(~(temp_outcome_df.next_category.isin(["LB","SB","ECT"])))
    temp_ab_next_df = temp_ab_next_df.filter(temp_ab_next_df.previous_category.isNull())
    temp_ab_both_df = temp_outcome_df.filter(~(temp_outcome_df.previous_category.isin(["LB","SB","ECT"])))
    temp_ab_both_df = temp_ab_both_df.filter(~(temp_ab_both_df.next_category.isin(["LB","SB","ECT"])))
    temp_ab_df = temp_ab_prev_df.union(temp_ab_next_df)
    temp_ab_df = temp_ab_df.union(temp_ab_both_df)

    # identify visits with at least minimum days between outcomes

    # record is preceded by livebirth/stillbirth
    temp_before_df = temp_outcome_df.filter((temp_outcome_df.previous_category.isin(["LB","SB"])) & 
                                            (temp_outcome_df.after_days >= before_min_lb) &
                                            (temp_outcome_df.next_category.isNull()))

    # record is followed by livebirth only
    temp_lb_after_df = temp_outcome_df.filter((temp_outcome_df.next_category=="LB") & 
                                            (F.abs(temp_outcome_df.before_days) >= after_min_lb) &
                                            (temp_outcome_df.previous_category.isNull()))

    # record is followed by stillbirth only
    temp_sb_after_df = temp_outcome_df.filter((temp_outcome_df.next_category=="SB") & 
                                            (F.abs(temp_outcome_df.before_days) >= after_min_sb) &
                                            (temp_outcome_df.previous_category.isNull()))

    # record is followed by LB and preceded by livebirth/stillbirth
    temp_both_lb_df = temp_outcome_df.filter((temp_outcome_df.next_category=="LB") & 
                                            (F.abs(temp_outcome_df.before_days) >= after_min_lb) &
                                            (temp_outcome_df.previous_category.isin(["LB","SB"])) & 
                                            (temp_outcome_df.after_days >= before_min_lb))

    # record is followed by SB and preceded by livebirth/stillbirth
    temp_both_sb_df = temp_outcome_df.filter((temp_outcome_df.next_category=="SB") & 
                                            (F.abs(temp_outcome_df.before_days) >= after_min_sb) &
                                            (temp_outcome_df.previous_category.isin(["LB","SB"])) & 
                                            (temp_outcome_df.after_days >= before_min_lb))

    # record is preceded by ECT
    temp_ect_before_df = temp_outcome_df.filter((temp_outcome_df.previous_category=="ECT") & 
                                            (temp_outcome_df.after_days >= before_min_ect) &
                                            (temp_outcome_df.next_category.isNull()))

    # record is followed by ECT
    temp_ect_after_df = temp_outcome_df.filter((temp_outcome_df.next_category=="ECT") & 
                                            (F.abs(temp_outcome_df.before_days) >= after_min_ect) &
                                            (temp_outcome_df.previous_category.isNull()))

    # record is followed by ECT and preceded by ECT
    temp_ect_ect_df = temp_outcome_df.filter((temp_outcome_df.next_category=="ECT") & 
                                            (F.abs(temp_outcome_df.before_days) >= after_min_ect) &
                                            (temp_outcome_df.previous_category=="ECT") & 
                                            (temp_outcome_df.after_days >= before_min_ect))

    # record is followed by ECT and preceded by livebirth/stillbirth
    temp_both_ect_df = temp_outcome_df.filter((temp_outcome_df.next_category=="ECT") & 
                                            (F.abs(temp_outcome_df.before_days) >= after_min_ect) &
                                            (temp_outcome_df.previous_category.isin(["LB","SB"])) & 
                                            (temp_outcome_df.after_days >= before_min_lb))

    # record is followed by LB and preceded by ECT
    temp_ect_lb_df = temp_outcome_df.filter((temp_outcome_df.next_category=="LB") & 
                                            (F.abs(temp_outcome_df.before_days) >= after_min_lb) &
                                            (temp_outcome_df.previous_category=="ECT") & 
                                            (temp_outcome_df.after_days >= before_min_ect))

    # record is followed by SB and preceded by ECT
    temp_ect_sb_df = temp_outcome_df.filter((temp_outcome_df.next_category=="SB") & 
                                            (F.abs(temp_outcome_df.before_days) >= after_min_sb) &
                                            (temp_outcome_df.previous_category=="ECT") & 
                                            (temp_outcome_df.after_days >= before_min_ect))
    
    # combine tables and drop columns
    drop_list = ["temp_category", "previous_category","next_category","before_days","after_days"]
    final_temp_df = temp_null_df.union(temp_before_df)
    final_temp_df = final_temp_df.union(temp_lb_after_df)
    final_temp_df = final_temp_df.union(temp_sb_after_df)
    final_temp_df = final_temp_df.union(temp_both_lb_df)
    final_temp_df = final_temp_df.union(temp_both_sb_df)
    final_temp_df = final_temp_df.union(temp_ect_before_df)
    final_temp_df = final_temp_df.union(temp_ect_after_df)
    final_temp_df = final_temp_df.union(temp_ect_sb_df)
    final_temp_df = final_temp_df.union(temp_ect_lb_df)
    final_temp_df = final_temp_df.union(temp_both_ect_df)
    final_temp_df = final_temp_df.union(temp_ect_ect_df)
    final_temp_df = final_temp_df.union(temp_ab_df)
    final_df = pregnant_df.union(final_temp_df.drop(*drop_list))

    # drop any duplicate rows
    final_df = final_df.dropDuplicates()

    print("Total number of live birth episodes.")
    print(final_df.filter(final_df.category == "LB").count())
    print("Total number of stillbirth episodes.")
    print(final_df.filter(final_df.category == "SB").count())
    print("Total number of ectopic episodes.")
    print(final_df.filter(final_df.category == "ECT").count())
    print("Total number of abortion episodes.")
    print(final_df.filter(final_df.category.isin(["SA","AB"])).count())

    return final_df

def add_delivery(add_abortion, Matcho_outcome_limits, final_delivery_visits):
    """
    Add delivery record only visits.
    """
    outcome_df = Matcho_outcome_limits

    # get minimum days between each outcome 
    # minimum number of days that DELIV can follow LB and SB; LB and SB have the same days
    before_min_lb = outcome_df.filter((outcome_df.first_preg_category=="LB") & 
        (outcome_df.outcome_preg_category=="DELIV")).select("min_days").collect()[0]["min_days"]    
    # minimum number of days that DELIV can follow ECT and SA/AB; ECT and SA/AB are the same
    before_min_ect = outcome_df.filter((outcome_df.first_preg_category=="ECT") & 
        (outcome_df.outcome_preg_category=="DELIV")).select("min_days").collect()[0]["min_days"]
    # minimum number of days that LB can follow DELIV
    after_min_lb = outcome_df.filter((outcome_df.first_preg_category=="DELIV") & 
        (outcome_df.outcome_preg_category=="LB")).select("min_days").collect()[0]["min_days"]
    # minimum number of days that SB can follow DELIV
    after_min_sb = outcome_df.filter((outcome_df.first_preg_category=="DELIV") & 
        (outcome_df.outcome_preg_category=="SB")).select("min_days").collect()[0]["min_days"]
    # minimum number of days that ECT/SA/AB can follow DELIV; # ECT and SA/AB are the same 
    after_min_ect = outcome_df.filter((outcome_df.first_preg_category=="DELIV") & 
        (outcome_df.outcome_preg_category=="ECT")).select("min_days").collect()[0]["min_days"] # ECT and SA/AB are the same

    # drop unneccessary columns
    delivery_df = final_delivery_visits.drop("gest_value","value_as_number")

    # join both tables
    pregnant_df = add_abortion
    temp_df = pregnant_df.union(delivery_df)

    # create temporary category column and rename all categories with SA to AB
    temp_df = temp_df.withColumn("temp_category", F.when((F.col("category") == "SA"), "AB").otherwise(F.col("category")))
   
    # get difference in days with subsequent visit
    temp_df = temp_df.withColumn("after_days", F.datediff(temp_df.visit_date, F.lag(temp_df.visit_date, 1).over(Window.partitionBy("person_id").orderBy("visit_date"))))

    # get previous category if available
    temp_df = temp_df.withColumn("previous_category", F.lag(temp_df.temp_category, 1).over(Window.partitionBy("person_id").orderBy("visit_date")))

    # get difference in days with previous visit
    temp_df = temp_df.withColumn("before_days", F.datediff(temp_df.visit_date, F.lag(temp_df.visit_date, -1).over(Window.partitionBy("person_id").orderBy("visit_date"))))

    # get subsequent category if available
    temp_df = temp_df.withColumn("next_category", F.lag(temp_df.temp_category, -1).over(Window.partitionBy("person_id").orderBy("visit_date")))

    # filter to delivery visits
    temp_outcome_df = temp_df.filter(temp_df.temp_category == "DELIV")

    # keep visits with days containing null values - indicates single event
    temp_null_df = temp_outcome_df.filter((temp_outcome_df.before_days.isNull()) & (temp_outcome_df.after_days.isNull()))

    # keep visits not preceded by or followed by LB or SB or ECT or AB/SA
    temp_deliv_prev_df = temp_outcome_df.filter(~(temp_outcome_df.previous_category.isin(["LB","SB","ECT","AB"])))
    temp_deliv_prev_df = temp_deliv_prev_df.filter(temp_deliv_prev_df.next_category.isNull())
    temp_deliv_next_df = temp_outcome_df.filter(~(temp_outcome_df.next_category.isin(["LB","SB","ECT","AB"])))
    temp_deliv_next_df = temp_deliv_next_df.filter(temp_deliv_next_df.previous_category.isNull())
    temp_deliv_both_df = temp_outcome_df.filter(~(temp_outcome_df.previous_category.isin(["LB","SB","ECT","AB"])))
    temp_deliv_both_df = temp_deliv_both_df.filter(~(temp_deliv_both_df.next_category.isin(["LB","SB","ECT","AB"])))
    temp_deliv_df = temp_deliv_prev_df.union(temp_deliv_next_df)
    temp_deliv_df = temp_deliv_df.union(temp_deliv_both_df)
  
    # identify visits with at least minimum days between outcomes
    
    # record is preceded by livebirth/stillbirth
    temp_before_df = temp_outcome_df.filter((temp_outcome_df.previous_category.isin(["LB","SB"])) & 
                                            (temp_outcome_df.after_days >= before_min_lb) &
                                            (temp_outcome_df.next_category.isNull()))

    # record is followed by livebirth
    temp_lb_after_df = temp_outcome_df.filter((temp_outcome_df.next_category=="LB") & 
                                            (F.abs(temp_outcome_df.before_days) >= after_min_lb) &
                                            (temp_outcome_df.previous_category.isNull()))

    # record is followed by stillbirth
    temp_sb_after_df = temp_outcome_df.filter((temp_outcome_df.next_category=="SB") & 
                                            (F.abs(temp_outcome_df.before_days) >= after_min_sb) &
                                            (temp_outcome_df.previous_category.isNull()))

    # record is followed by LB and preceded by livebirth/stillbirth
    temp_both_lb_df = temp_outcome_df.filter((temp_outcome_df.next_category=="LB") & 
                                            (F.abs(temp_outcome_df.before_days) >= after_min_lb) &
                                            (temp_outcome_df.previous_category.isin(["LB","SB"])) & 
                                            (temp_outcome_df.after_days >= before_min_lb))

    # record is followed by SB and preceded by livebirth/stillbirth
    temp_both_sb_df = temp_outcome_df.filter((temp_outcome_df.next_category=="SB") & 
                                            (F.abs(temp_outcome_df.before_days) >= after_min_sb) &
                                            (temp_outcome_df.previous_category.isin(["LB","SB"])) & 
                                            (temp_outcome_df.after_days >= before_min_lb))

    # record is preceded by ECT/SA/AB
    temp_ect_before_df = temp_outcome_df.filter((temp_outcome_df.previous_category.isin(["ECT","AB"])) & 
                                            (temp_outcome_df.after_days >= before_min_ect) &
                                            (temp_outcome_df.next_category.isNull()))

    # record is followed by ECT/SA/AB
    temp_ect_after_df = temp_outcome_df.filter((temp_outcome_df.next_category.isin(["ECT","AB"])) & 
                                            (F.abs(temp_outcome_df.before_days) >= after_min_ect) &
                                            (temp_outcome_df.previous_category.isNull()))

    # record is followed by ECT/SA/AB and preceded by ECT/SA/AB
    temp_ect_ect_df = temp_outcome_df.filter((temp_outcome_df.next_category.isin(["ECT","AB"])) & 
                                            (F.abs(temp_outcome_df.before_days) >= after_min_ect) &
                                            (temp_outcome_df.previous_category.isin(["ECT","AB"])) & 
                                            (temp_outcome_df.after_days >= before_min_ect))

    # record is followed by ECT/SA/AB and preceded by livebirth/stillbirth
    temp_both_ect_df = temp_outcome_df.filter((temp_outcome_df.next_category.isin(["ECT","AB"])) & 
                                            (F.abs(temp_outcome_df.before_days) >= after_min_ect) &
                                            (temp_outcome_df.previous_category.isin(["LB","SB"])) & 
                                            (temp_outcome_df.after_days >= before_min_lb))

    # record is followed by LB and preceded by ECT/SA/AB
    temp_ect_lb_df = temp_outcome_df.filter((temp_outcome_df.next_category=="LB") & 
                                            (F.abs(temp_outcome_df.before_days) >= after_min_lb) &
                                            (temp_outcome_df.previous_category.isin(["ECT","AB"])) & 
                                            (temp_outcome_df.after_days >= before_min_ect))

    # record is followed by SB and preceded by ECT/SA/AB
    temp_ect_sb_df = temp_outcome_df.filter((temp_outcome_df.next_category=="SB") & 
                                            (F.abs(temp_outcome_df.before_days) >= after_min_sb) &
                                            (temp_outcome_df.previous_category.isin(["ECT","AB"])) & 
                                            (temp_outcome_df.after_days >= before_min_ect))

    # combine tables and drop columns
    drop_list = ["temp_category", "previous_category","next_category","before_days","after_days"]
    final_temp_df = temp_null_df.union(temp_before_df)
    final_temp_df = final_temp_df.union(temp_lb_after_df)
    final_temp_df = final_temp_df.union(temp_sb_after_df)
    final_temp_df = final_temp_df.union(temp_both_lb_df)
    final_temp_df = final_temp_df.union(temp_both_sb_df)
    final_temp_df = final_temp_df.union(temp_ect_before_df)
    final_temp_df = final_temp_df.union(temp_ect_after_df)
    final_temp_df = final_temp_df.union(temp_ect_sb_df)
    final_temp_df = final_temp_df.union(temp_ect_lb_df)
    final_temp_df = final_temp_df.union(temp_both_ect_df)
    final_temp_df = final_temp_df.union(temp_ect_ect_df)
    final_temp_df = final_temp_df.union(temp_deliv_df)
    final_df = pregnant_df.union(final_temp_df.drop(*drop_list))

    # drop any duplicate rows
    final_df = final_df.dropDuplicates()

    print("Total number of live birth episodes.")
    print(final_df.filter(final_df.category == "LB").count())
    print("Total number of stillbirth episodes.")
    print(final_df.filter(final_df.category == "SB").count())
    print("Total number of ectopic episodes.")
    print(final_df.filter(final_df.category == "ECT").count())
    print("Total number of abortion episodes.")
    print(final_df.filter(final_df.category.isin(["SA","AB"])).count())
    print("Total number of delivery episodes.")
    print(final_df.filter(final_df.category == "DELIV").count())

    return final_df

def gestation_visits(initial_pregnant_cohort):
    """
    Filter to visits with gestation period.

    Additional gestation concepts to use:
    3002209 - Gestational age Estimated
    3048230 - Gestational age in weeks
    3012266	- Gestational age   
    """
    df = initial_pregnant_cohort
    # get records with gestation period
    gest_df = df.filter(df.gest_value.isNotNull())
    # get records with gestational age in weeks
    other_gest_df = df.filter((df.concept_id.isin([3002209, 3048230, 3012266])) & (df.value_as_number.isNotNull()))
    # copy values from value_as_number column to gest_value column
    other_gest_df = other_gest_df.withColumn("gest_value", F.col("value_as_number").cast(IntegerType()))
    # combine tables
    all_gest_df = gest_df.union(other_gest_df)
    return all_gest_df
    
def gestation_episodes(gestation_visits):
    """
    Define pregnancy episode per patient by gestational records.

    Any record with a negative change or no change in the gestational age in weeks from the previous record is flagged as the start of a potential episode. This record is then checked if there is at least a separation of 70 days from the previous record. The number of days, 70, was determined by taking the minimum outcome limit in days from Matcho et al. (56) and adding a buffer of 14 days. If the record is not at least 70 days from the previous record, it is no longer flagged as the start of an episode. 

    For all records with a positive change in the gestational age in weeks from the previous record is then checked if the date difference in days is greater than the difference in days between the record's gestational age in weeks and the previous record's gestational age in weeks with a buffer of 28 days. The buffer of 28 days was determined by taking the minimum retry period in days from Matcho et al. (14) and adding 14 days as a buffer. If the date difference in days is greater than the difference in days between the record's gestational age in weeks and the previous record's gestational age in weeks with the buffer, then this record is flagged as a start of a new episode.
    """
    df = gestation_visits
    
    # minimum number of days to be new distinct episode 
    min_days = 70
    # number of days to use as a buffer
    buffer_days = 28

    # filter out any empty visit dates
    df = df.filter(~df.visit_date.isNull())
    # remove any records with incorrect gestational weeks (i.e. 9999999) 
    df = df.filter((df.gest_value > 0) & (df.gest_value <= 44))

    # keep max gest_value if two or more gestational records share same date
    max_df = df.groupBy("person_id","visit_date").agg(F.max("gest_value").alias("gest_week"))
    
    # join gest_week to table
    df = df.join(max_df, ["person_id","visit_date"])

    # filter out rows that are not the max gest_value at visit_date
    df = df.filter(df.gest_value == df.gest_week)

    # add column for gestation period in days
    df = df.withColumn("gest_day", (F.col("gest_week")*7))

    # get previous gestation week
    df = df.withColumn("prev_week", F.lag(df.gest_week, 1).over(Window.partitionBy("person_id").orderBy("visit_date")))

    # get previous date
    df = df.withColumn("prev_date", F.lag(df.visit_date, 1).over(Window.partitionBy("person_id").orderBy("visit_date")))

    # calculate difference between gestation weeks
    df = df.withColumn("week_diff", (F.col("gest_week") - F.col("prev_week")))

    # calculate number of days between gestation weeks with buffer
    df = df.withColumn("day_diff", (F.col("week_diff")*7 + buffer_days))

    # get difference in days between visit date and previous date
    df = df.withColumn("date_diff", F.datediff(df.visit_date, df.prev_date))

    # check if any negative or zero number in week_diff column corresponds to a new pregnancy episode, change to 1 if not;
    # difference in days between visit date and previous visit date must be >= minimum days between episodes
    df = df.withColumn("new_diff", F.when((F.col("date_diff") < min_days) & 
        (F.col("week_diff") <= 0), 1).otherwise(F.col("week_diff"))) 

    # check if any positive number in week_diff column has a date_diff >= day_diff
    # may correspond to new pregnancy episode, if so change to -1
    df = df.withColumn("new_diff2", F.when((F.col("date_diff") >= F.col("day_diff")) & 
        (F.col("week_diff") > 0), -1).otherwise(F.col("new_diff")))

    # define window by person_id and visit_date
    window = Window.partitionBy("person_id").orderBy("visit_date")

    # create new columns, index and episode; any zero or negative number in newdiff2 column indicates a new episode
    df = df.withColumn("index", F.rank().over(window).alias("index")).withColumn("episode", F.sum(((F.col("new_diff2") <= 0) | (F.col("index") == 1)).cast("int")).over(window))  

    return df

def get_min_max_gestation(gestation_episodes):
    """
    Get the min and max gestational age in weeks and the corresponding visit dates per pregnancy episode.
    Also get the first and last visits and their corresponding gestational age in weeks per pregnancy episode.
    For minimum gestatational age in weeks, the first and last occurrence and their dates were obtained. 
    """

    df = gestation_episodes

    ############ First Visit Date ############

    # identify first visit for each pregnancy episode
    first_df = df.groupBy("person_id","episode").agg(F.min("visit_date").alias("first_gest_date"))
    first_df = first_df.withColumnRenamed("person_id","first_person_id")
    first_df = first_df.withColumnRenamed("episode","first_episode")

    # join with original table to get rows with first visit date
    temp_first_df = df.join(first_df, (df.person_id == first_df.first_person_id) & (df.episode == first_df.first_episode) & (df.visit_date == first_df.first_gest_date),"inner")

    # get max gestation week at first visit date
    new_first_df = temp_first_df.groupBy("person_id","data_partner_id","year_of_birth","race_concept_name","ethnicity_concept_name","episode","visit_date").agg(F.max("gest_week").alias("first_gest_week"))

    # rename columns
    new_first_df = new_first_df.withColumnRenamed("visit_date","first_gest_date")

    ############ Min Gestation Week ############

    # identify minimum gestation week for each pregnancy episode
    min_df = df.groupBy("person_id","episode").agg(F.min("gest_week").alias("min_gest_week"))
    min_df = min_df.withColumnRenamed("person_id","min_person_id")
    min_df = min_df.withColumnRenamed("episode","min_episode")

    # join with original table to get rows with min gestation week
    temp_min_df = df.join(min_df, (df.person_id == min_df.min_person_id) & (df.episode == min_df.min_episode) & (df.gest_week == min_df.min_gest_week),"inner")

    # get first occurrence of min gestation week
    new_min_df = temp_min_df.groupBy("person_id","episode","gest_week").agg(F.min("visit_date").alias("min_gest_date"))
    
    # rename columns
    new_min_df = new_min_df.withColumnRenamed("gest_week","min_gest_week")
    new_min_df = new_min_df.withColumnRenamed("person_id","min_person_id")
    new_min_df = new_min_df.withColumnRenamed("episode","min_episode")

    # get last occurrence of min gestation week
    second_min_df = temp_min_df.groupBy("person_id","episode","gest_week").agg(F.max("visit_date").alias("min_gest_date_2"))

    # rename columns
    second_min_df = second_min_df.withColumnRenamed("gest_week","min_gest_week_2")
    second_min_df = second_min_df.withColumnRenamed("person_id","min_person_id_2")
    second_min_df = second_min_df.withColumnRenamed("episode","min_episode_2")

    ############ End Visit Date ############

    # identify end visit for each pregnancy episode
    end_df = df.groupBy("person_id","episode").agg(F.max("visit_date").alias("end_gest_date"))
    end_df = end_df.withColumnRenamed("person_id","end_person_id")
    end_df = end_df.withColumnRenamed("episode","end_episode")

    # join with original table to get rows with end visit date
    temp_end_df = df.join(end_df, (df.person_id == end_df.end_person_id) & (df.episode == end_df.end_episode) & (df.visit_date == end_df.end_gest_date),"inner")

    # get max gestation week at end visit date
    new_end_df = temp_end_df.groupBy("person_id","episode","visit_date").agg(F.max("gest_week").alias("end_gest_week"))

    # rename columns
    new_end_df = new_end_df.withColumnRenamed("person_id","end_person_id")
    new_end_df = new_end_df.withColumnRenamed("episode","end_episode")
    new_end_df = new_end_df.withColumnRenamed("visit_date","end_gest_date")

    ############ Max Gestation Week ############

    # identify max gestation week for each pregnancy episode
    max_df = df.groupBy("person_id","episode").agg(F.max("gest_week").alias("max_gest_week"))
    max_df = max_df.withColumnRenamed("person_id","max_person_id")
    max_df = max_df.withColumnRenamed("episode","max_episode")

    # join with original table to get rows with max gestation week
    temp_max_df = df.join(max_df, (df.person_id == max_df.max_person_id) & (df.episode == max_df.max_episode) & (df.gest_week == max_df.max_gest_week),"inner")

    # get first occurrence of max gestation week
    new_max_df = temp_max_df.groupBy("person_id","episode","gest_week").agg(F.min("visit_date").alias("max_gest_date"))
    
    # rename columns
    new_max_df = new_max_df.withColumnRenamed("gest_week","max_gest_week")
    new_max_df = new_max_df.withColumnRenamed("person_id","max_person_id")
    new_max_df = new_max_df.withColumnRenamed("episode","max_episode")

    ############ Join tables ############

    # join first and end tables
    all_df = new_first_df.join(new_end_df, (new_first_df.person_id == new_end_df.end_person_id) & (new_first_df.episode == new_end_df.end_episode),"inner")

    # join with min table
    all_df = all_df.join(new_min_df, (all_df.person_id == new_min_df.min_person_id) & (all_df.episode == new_min_df.min_episode),"inner")

    # join with min table version 2
    all_df = all_df.join(second_min_df, (all_df.person_id == second_min_df.min_person_id_2) & (all_df.episode == second_min_df.min_episode_2),"inner")

    # join with max table
    all_df = all_df.join(new_max_df, (all_df.person_id == new_max_df.max_person_id) & (all_df.episode == new_max_df.max_episode),"inner")

    # drop unneccessary columns
    all_df = all_df.drop("max_person_id","max_episode","end_person_id","end_episode","min_person_id","min_episode","min_gest_week_2","min_person_id_2", "min_episode_2")

    return all_df

def calculate_start(add_delivery, Matcho_term_durations):
    """
    Estimate start of pregnancies based on outcome type.
    """     
    df = add_delivery
    terms = Matcho_term_durations

    # join tables
    term_df = df.join(terms,"category","leftouter")

    # calculate min start date
    term_df = term_df.withColumn("min_start_date", F.expr("date_sub(visit_date, min_term)"))

    # calculate max start date
    term_df = term_df.withColumn("max_start_date", F.expr("date_sub(visit_date, max_term)"))

    return term_df
    
def add_gestation(calculate_start, get_min_max_gestation):
    """
    Add gestation-based episodes. Any gestation-based episode that overlaps with an outcome-based episode is removed as a distinct episode.
    """
    gest_df = get_min_max_gestation
    outcome_df = calculate_start

    # number of days for buffer
    buffer_days = 28

    # add unique id for each outcome visit
    outcome_df = outcome_df.withColumn("visit_id", F.concat(F.col("person_id"), F.col("visit_date"))) 

    # rename columns to join table
    gest_df = gest_df.withColumnRenamed("year_of_birth","gest_year_of_birth")
    gest_df = gest_df.withColumnRenamed("data_partner_id","gest_data_partner_id")
    gest_df = gest_df.withColumnRenamed("race_concept_name","gest_race_concept_name")
    gest_df = gest_df.withColumnRenamed("ethnicity_concept_name","gest_ethnicity_concept_name")

    # add unique id for each gestation visit
    gest_df = gest_df.withColumn("gest_id", F.concat(F.col("person_id"), F.col("max_gest_date"))) 

    # join both tables
    temp_df = outcome_df.join(gest_df,"person_id","outer")

    # patient records with no gestation 
    no_gest_df = temp_df.filter(temp_df.gest_data_partner_id.isNull())

    # patient records with no outcome
    no_outcome_df = temp_df.filter(temp_df.data_partner_id.isNull())

    # rename columns for joining table
    no_outcome_df = no_outcome_df.withColumn("category", F.lit("PREG"))
    no_outcome_df = no_outcome_df.withColumn("visit_date", F.lit(F.col("max_gest_date")))
    no_outcome_df = no_outcome_df.withColumn("data_partner_id", F.lit(F.col("gest_data_partner_id")))
    no_outcome_df = no_outcome_df.withColumn("year_of_birth", F.lit(F.col("gest_year_of_birth")))
    no_outcome_df = no_outcome_df.withColumn("race_concept_name", F.lit(F.col("gest_race_concept_name")))
    no_outcome_df = no_outcome_df.withColumn("ethnicity_concept_name", F.lit(F.col("gest_ethnicity_concept_name")))
    no_outcome_df = no_outcome_df.withColumn("concept_id",  F.lit(None))
    no_outcome_df = no_outcome_df.withColumn("concept_name", F.lit(None))

    ###### Filter to patient records with both gestation and outcome ######

    both_df = temp_df.filter((temp_df.gest_data_partner_id.isNotNull()) & temp_df.data_partner_id.isNotNull())

    # add column for gestation period in days for max gestation week on record
    both_df = both_df.withColumn("max_gest_day", (F.col("max_gest_week")*7))
    # add column for gestation period in days for min gestation week on record
    both_df = both_df.withColumn("min_gest_day", (F.col("min_gest_week")*7))
    # get date of estimated start date based on max gestation week on record
    both_df = both_df.withColumn("max_gest_start_date", F.expr("date_sub(max_gest_date, max_gest_day)"))
    # get date of estimated start date based on min gestation week on record
    both_df = both_df.withColumn("min_gest_start_date", F.expr("date_sub(min_gest_date, min_gest_day)"))
    # get difference in days between estimated start dates
    both_df = both_df.withColumn("gest_start_date_diff", F.datediff(both_df.max_gest_start_date, both_df.min_gest_start_date))

    # add column to check if max_gest_start_date is greater than or equal to max_start_date with buffer
    # 1 indicates yes
    both_df = both_df.withColumn("is_gest_start_after_outcome_start", F.when(
        F.col("max_gest_start_date") >= F.date_sub(F.col("max_start_date"), buffer_days), 1).otherwise(0))

    # get difference in days between max_gest_date and outcome visit_date
    both_df = both_df.withColumn("days_diff", F.datediff(both_df.visit_date, both_df.max_gest_date))

    # add column to check if max_gest_date is less than or equal to outcome visit_date with buffer 
    # 1 indicates yes
    both_df = both_df.withColumn("is_gest_end_before_outcome", F.when(
        F.col("max_gest_date") <= F.date_add(F.col("visit_date"), buffer_days), 1).otherwise(0))

    # add column to check if max_gest_date is greater than or equal to outcome visit_date with buffer 
    # 1 indicates yes
    both_df = both_df.withColumn("is_gest_end_after_outcome", F.when(
        F.col("max_gest_date") >= F.date_add(F.col("visit_date"), buffer_days), 1).otherwise(0))

    # add column to check if max_gest_date is greater than or equal to outcome max_start_date 
    # 1 indicates yes
    both_df = both_df.withColumn("is_gest_end_after_outcome_start", F.when(
        F.col("max_gest_date") >= F.col("max_start_date"), 1).otherwise(0))

    # add column to check if max_gest_start_date is less than or equal to outcome visit_date 
    # 1 indicates yes
    both_df = both_df.withColumn("is_gest_start_before_outcome", F.when(
        F.col("max_gest_start_date") <= F.col("visit_date"), 1).otherwise(0))

    # add column to check if max_gest_start_date is less than or equal to max_start_date with buffer
    # 1 indicates yes
    both_df = both_df.withColumn("is_gest_start_before_outcome_start", F.when(
        F.col("max_gest_start_date") <= F.date_sub(F.col("max_start_date"), buffer_days), 1).otherwise(0))

    ###### Check for any overlap of gestation-based episodes and outcome-based episodes ######

    # add column with 1 indicating if max_gest_date is less than or equal to outcome visit_date with buffer 
    # and max_gest_start_date is greater than or equal to max_start_date with buffer
    both_df = both_df.withColumn("within_outcome", F.when(
        (F.col("is_gest_start_after_outcome_start") == 1) & 
        (F.col("is_gest_end_before_outcome") == 1), 1).otherwise(0))

    # add column with 1 indicating if max_gest_start_date is less than or equal to outcome visit_date
    # and greater than or equal to max_start_date and max_gest_date is greater than or equal to outcome visit_date
    both_df = both_df.withColumn("overlap_outcome_end", F.when(
        (F.col("is_gest_start_after_outcome_start") == 1) & 
        (F.col("is_gest_start_before_outcome") == 1) & 
        (F.col("is_gest_end_after_outcome") == 1), 1).otherwise(0))

    # add column with 1 indicating if max_gest_start_date is less than or equal to outcome max_start_date
    # and max_gest_date is greater than or equal to max_start_date and and less then or equal to outcome visit_date
    both_df = both_df.withColumn("overlap_outcome_start", F.when(
        (F.col("is_gest_start_before_outcome_start") == 1) & 
        (F.col("is_gest_end_before_outcome") == 1) &
        (F.col("is_gest_end_after_outcome_start") == 1), 1).otherwise(0))

    # add column with 1 indicating if max_gest_start_date is less than or equal to max_start_date
    # and greater than or equal to outcome date
    both_df = both_df.withColumn("within_gestation", F.when(
        (F.col("max_gest_start_date") <= F.col("max_start_date")) & 
        (F.col("max_gest_date") >= F.col("visit_date")), 1).otherwise(0)) 

    ###### Filter to outcomes with gestational records ######

    outcome_gest_df = both_df.filter((both_df.within_outcome == 1) | (both_df.overlap_outcome_end == 1) | (both_df.overlap_outcome_start == 1) | (both_df.within_gestation == 1))
    # columns to drop
    gest_cols = ["is_gest_start_after_outcome_start","is_gest_end_before_outcome","is_gest_start_before_outcome","within_outcome","overlap_outcome_end", "overlap_outcome_start", "within_gestation","is_gest_start_before_outcome_start","is_gest_end_after_outcome_start","is_gest_end_after_outcome"]    
    # drop columns
    outcome_gest_df = outcome_gest_df.drop(*gest_cols)

    # get gest_ids with both outcome and gestation 
    gestids = outcome_gest_df.select("gest_id").toPandas().gest_id.tolist()
    # get visit_ids with both outcome and gestation 
    visitids = outcome_gest_df.select("visit_id").toPandas().visit_id.tolist()

    ###### Create table with gestation visits with no corresponding outcome record ######

    gest_only_df = both_df.filter(~both_df.gest_id.isin(gestids)).dropDuplicates(["gest_id"]).drop(*gest_cols)
    # rename columns to join table
    gest_only_df = gest_only_df.withColumn("category", F.lit("PREG"))
    # set visit date to max_gest_date
    gest_only_df = gest_only_df.withColumn("visit_date", F.lit(F.col("max_gest_date")))
    gest_only_df = gest_only_df.withColumn("data_partner_id", F.lit(F.col("gest_data_partner_id")))
    gest_only_df = gest_only_df.withColumn("year_of_birth", F.lit(F.col("gest_year_of_birth")))
    gest_only_df = gest_only_df.withColumn("race_concept_name", F.lit(F.col("gest_race_concept_name")))
    gest_only_df = gest_only_df.withColumn("ethnicity_concept_name", F.lit(F.col("gest_ethnicity_concept_name")))
    # replace the following columns with null values
    gest_only_df = gest_only_df.withColumn("visit_id", F.lit(None))
    gest_only_df = gest_only_df.withColumn("days_diff", F.lit(None))
    gest_only_df = gest_only_df.withColumn("concept_name", F.lit(None))
    gest_only_df = gest_only_df.withColumn("concept_id", F.lit(None))
    gest_only_df = gest_only_df.withColumn("max_term", F.lit(None))
    gest_only_df = gest_only_df.withColumn("min_term", F.lit(None))
    gest_only_df = gest_only_df.withColumn("retry", F.lit(None))
    gest_only_df = gest_only_df.withColumn("min_start_date", F.lit(None))
    gest_only_df = gest_only_df.withColumn("max_start_date", F.lit(None))

    ###### Create table with outcome visits with no corresponding gestation records ######

    outcome_only_df = both_df.filter(~both_df.visit_id.isin(visitids)).dropDuplicates(["visit_id"]).drop(*gest_cols)
    # replace gestation info with null values
    outcome_only_df = outcome_only_df.withColumn("gest_id", F.lit(None))
    outcome_only_df = outcome_only_df.withColumn("first_gest_date", F.lit(None))
    outcome_only_df = outcome_only_df.withColumn("first_gest_week", F.lit(None))
    outcome_only_df = outcome_only_df.withColumn("end_gest_date", F.lit(None))
    outcome_only_df = outcome_only_df.withColumn("end_gest_week", F.lit(None))
    outcome_only_df = outcome_only_df.withColumn("max_gest_date", F.lit(None))
    outcome_only_df = outcome_only_df.withColumn("max_gest_week", F.lit(None))
    outcome_only_df = outcome_only_df.withColumn("min_gest_week", F.lit(None))
    outcome_only_df = outcome_only_df.withColumn("min_gest_date", F.lit(None))
    outcome_only_df = outcome_only_df.withColumn("min_gest_date_2", F.lit(None))
    outcome_only_df = outcome_only_df.withColumn("max_gest_day", F.lit(None))
    outcome_only_df = outcome_only_df.withColumn("min_gest_day", F.lit(None))
    outcome_only_df = outcome_only_df.withColumn("max_gest_start_date", F.lit(None))
    outcome_only_df = outcome_only_df.withColumn("min_gest_start_date", F.lit(None))
    outcome_only_df = outcome_only_df.withColumn("gest_start_date_diff", F.lit(None))
    outcome_only_df = outcome_only_df.withColumn("days_diff", F.lit(None))

    # add columns for joining tables
    no_outcome_df = no_outcome_df.withColumn("max_gest_day", (F.col("max_gest_week")*7))
    no_outcome_df = no_outcome_df.withColumn("min_gest_day", (F.col("min_gest_week")*7))
    no_outcome_df = no_outcome_df.withColumn("max_gest_start_date", F.expr("date_sub(max_gest_date, max_gest_day)"))
    no_outcome_df = no_outcome_df.withColumn("min_gest_start_date", F.expr("date_sub(min_gest_date, min_gest_day)"))
    no_outcome_df = no_outcome_df.withColumn("gest_start_date_diff", F.datediff(no_outcome_df.max_gest_start_date, no_outcome_df.min_gest_start_date))
    no_outcome_df = no_outcome_df.withColumn("days_diff", F.lit(None))
    no_gest_df = no_gest_df.withColumn("max_gest_day", F.lit(None))
    no_gest_df = no_gest_df.withColumn("min_gest_day", F.lit(None))
    no_gest_df = no_gest_df.withColumn("max_gest_start_date", F.lit(None))
    no_gest_df = no_gest_df.withColumn("min_gest_start_date", F.lit(None))
    no_gest_df = no_gest_df.withColumn("gest_start_date_diff", F.lit(None))
    no_gest_df = no_gest_df.withColumn("days_diff", F.lit(None))

    ###### Join tables ######
    all_df = no_gest_df.union(no_outcome_df)
    all_df = all_df.union(gest_only_df)
    all_df = all_df.union(outcome_only_df)
    all_df = all_df.union(outcome_gest_df)

    # columns to drop
    drop_list = ["gest_data_partner_id","gest_year_of_birth","gest_race_concept_name","gest_ethnicity_concept_name","episode"]
    # drop columns and remove any duplicate rows
    all_df = all_df.drop(*drop_list).dropDuplicates()
    # sort by person_id and visit_date
    all_df = all_df.orderBy(["person_id","visit_date"])
    
    # define window by person_id and visit_date
    window = Window.partitionBy("person_id").orderBy("visit_date")
    # add column for episode
    all_df = all_df.withColumn("episode", F.row_number().over(window))

    print("Total number of outcome-based episodes.")
    print(outcome_df.count())
    print("Total number of gestation-based episodes.")
    print(gest_df.count())
    print("Total number of outcome-based episodes after merging.")
    all_df.select(F.countDistinct("visit_id")).show()
    print("Total number of gestation-based episodes after merging.")
    all_df.select(F.countDistinct("gest_id")).show()
    
    return all_df
    
def clean_episodes(add_gestation):
    """
    Clean up episodes by removing duplicate episodes and reclassifying outcome-based episodes as gestation-based episodes if the outcome containing gestational info does not fall within the term durations defined by Matcho et al. 
    """    
    df = add_gestation

    # number of days for buffer
    buffer_days = 28
    
    ###### Remove any duplicate gest_id and visit_id ######

    ###### Check for duplicated gest_id and keep only episodes where the outcome visit_date is closest to max_gest_date ######
    
    # filter out episodes with no duplicate gest_ids
    temp_df = df.join(df.groupBy("gest_id").agg((F.count("*") > 1).cast("int").alias("dup")), on=["gest_id"], how="inner").filter(F.col("dup") == 0)
    # filter out episodes with no gest_id
    no_gest_df = df.filter(df.gest_id.isNull())
    # filter for episodes with duplicates
    gest_temp_df = df.join(df.groupBy("gest_id").agg((F.count("*") > 1).cast("int").alias("dup")), on=["gest_id"], how="inner").filter(F.col("dup") != 0)
    # flag episode closest to outcome visit_date
    new_gest_temp_df = gest_temp_df.join(gest_temp_df.groupBy("gest_id").agg(F.min(F.abs("days_diff")).alias("best_gest")), on=["gest_id"], how="inner")
    # flag episode not closest to outcome visit_date
    new_gest_temp_df = new_gest_temp_df.withColumn("remove_episode", F.when(F.abs("days_diff") != F.col("best_gest"), 1).otherwise(0)).filter(F.col("remove_episode") == 0)
    # columns to drop
    cols_drop = ["best_gest","remove_episode"]
    # join all three tables
    new_df = temp_df.union(new_gest_temp_df.drop(*cols_drop)).drop(F.col("dup"))
    new_df = new_df.unionByName(no_gest_df)
    
    # filter out episodes with no duplicate visit_ids
    new_temp_df = new_df.join(new_df.groupBy("visit_id").agg((F.count("*") > 1).cast("int").alias("dup")), on=["visit_id"], how="inner").filter(F.col("dup") == 0)
    # filter out episodes with no visit_id
    no_visit_df = new_df.filter(new_df.visit_id.isNull())
    # filter for episodes with duplicates
    visit_temp_df = new_df.join(new_df.groupBy("visit_id").agg((F.count("*") > 1).cast("int").alias("dup")), on=["visit_id"], how="inner").filter(F.col("dup") != 0)
    # flag episode closest to max_gest_date
    new_visit_temp_df = visit_temp_df.join(visit_temp_df.groupBy("visit_id").agg(F.min(F.abs("days_diff")).alias("best_gest")), on=["visit_id"], how="inner")
    # flag episode not closest to max_gest_date   
    new_visit_temp_df = new_visit_temp_df.withColumn("remove_episode", F.when(F.abs("days_diff") != F.col("best_gest"), 1).otherwise(0)).filter(F.col("remove_episode") == 0)
    # columns to drop
    cols_drop = ["best_gest","remove_episode"]
    # join all three tables
    final_df = new_temp_df.union(new_visit_temp_df.drop(*cols_drop)).drop(F.col("dup"))
    final_df = final_df.unionByName(no_visit_df)
   
    ###### add columns to check if gestational info corresponds to outcome type ######

    # get estimated gestational age in days at outcome_visit_date using max_gest_start_date
    final_df = final_df.withColumn("gest_at_outcome", F.datediff(final_df.visit_date, final_df.max_gest_start_date))

    # add column to check if gest_at_outcome is less than or equal to max_term, 1 indicates yes
    final_df = final_df.withColumn("is_under_max", F.when(F.col("gest_at_outcome") <= F.col("max_term"), 1).otherwise(0))

    # add column to check if gest_at_outcome is less than or equal to min_term, 1 indicates yes
    final_df = final_df.withColumn("is_over_min", F.when(F.col("gest_at_outcome") >= F.col("min_term"), 1).otherwise(0))

    ###### remove any outcomes where the gestational age based on max_gest_date is over the max term duration defined by Matcho et al. ######

    # filter to episodes with max_gest_date is over the max term duration
    over_max_df = final_df.withColumn("remove_episode", F.when((F.col("gest_id").isNotNull()) & (F.col("visit_id").isNotNull()) & (F.col("is_under_max") == 0), 1).otherwise(0)).filter(F.col("remove_episode") == 1)
    # filter out these episodes from main table
    final_df = final_df.withColumn("remove_episode", F.when((F.col("gest_id").isNotNull()) & (F.col("visit_id").isNotNull()) & (F.col("is_under_max") == 0), 1).otherwise(0)).filter(F.col("remove_episode") == 0)
    # reclassify these episodes as PREG
    over_max_df = over_max_df.withColumn("removed_category", F.col("category"))
    over_max_df = over_max_df.withColumn("category", F.lit("PREG"))
    # set visit date to max_gest_date
    over_max_df = over_max_df.withColumn("visit_date", F.lit(F.col("max_gest_date")))
    # replace the following columns with null values
    over_max_df = over_max_df.withColumn("visit_id", F.lit(None))
    over_max_df = over_max_df.withColumn("days_diff", F.lit(None))
    over_max_df = over_max_df.withColumn("concept_name", F.lit(None))
    over_max_df = over_max_df.withColumn("concept_id", F.lit(None))
    over_max_df = over_max_df.withColumn("max_term", F.lit(None))
    over_max_df = over_max_df.withColumn("min_term", F.lit(None))
    over_max_df = over_max_df.withColumn("retry", F.lit(None))
    over_max_df = over_max_df.withColumn("min_start_date", F.lit(None))
    over_max_df = over_max_df.withColumn("max_start_date", F.lit(None))
    over_max_df = over_max_df.withColumn("removed_outcome", F.lit(1)) # flag episode if outcome was removed
    # add columns with null or 0 values
    final_df = final_df.withColumn("removed_category", F.lit(None))
    final_df = final_df.withColumn("removed_outcome", F.lit(0))
    print("Total number of episodes over maximum term duration.")
    print(over_max_df.count())
    # join episodes with new values to main table
    final_df = final_df.union(over_max_df).drop(F.col("remove_episode"))

    ###### remove any outcomes where the gestational age based on max_gest_date is under the min term duration defined by Matcho et al. ######

    # filter to episodes with max_gest_date is under the min term duration and where the number of days between the visit_date and 
    # max_gest_date is negative with buffer
    under_min_df = final_df.withColumn("remove_episode", F.when((F.col("gest_id").isNotNull()) & (F.col("visit_id").isNotNull()) & (F.col("is_over_min") == 0) & (F.col("days_diff") < buffer_days*-1), 1).otherwise(0)).filter(F.col("remove_episode") == 1)
    # filter out these episodes from main table
    final_df = final_df.withColumn("remove_episode", F.when((F.col("gest_id").isNotNull()) & (F.col("visit_id").isNotNull()) & (F.col("is_over_min") == 0) & (F.col("days_diff") < buffer_days*-1), 1).otherwise(0)).filter(F.col("remove_episode") == 0)
    # reclassify these episodes as PREG
    under_min_df = under_min_df.withColumn("removed_category", F.col("category"))
    under_min_df = under_min_df.withColumn("category", F.lit("PREG"))
    # set visit date to max_gest_date
    under_min_df = under_min_df.withColumn("visit_date", F.lit(F.col("max_gest_date")))    
    # replace the following columns with null values
    under_min_df = under_min_df.withColumn("visit_id", F.lit(None))
    under_min_df = under_min_df.withColumn("days_diff", F.lit(None))
    under_min_df = under_min_df.withColumn("concept_name", F.lit(None))
    under_min_df = under_min_df.withColumn("concept_id", F.lit(None))
    under_min_df = under_min_df.withColumn("max_term", F.lit(None))
    under_min_df = under_min_df.withColumn("min_term", F.lit(None))
    under_min_df = under_min_df.withColumn("retry", F.lit(None))
    under_min_df = under_min_df.withColumn("min_start_date", F.lit(None))
    under_min_df = under_min_df.withColumn("max_start_date", F.lit(None))
    under_min_df = under_min_df.withColumn("removed_outcome", F.lit(1)) # flag episode if outcome was removed
    print("Total number of episodes under minimum term duration.")
    print(under_min_df.count())
    # join episodes with new values to main table
    final_df = final_df.union(under_min_df).drop(F.col("remove_episode"))

    ###### remove any outcomes where the difference between max_gest_date in days is negative ######

    # filter to episodes with max_gest_date is after the outcome visit_date with buffer
    neg_days_df = final_df.withColumn("remove_episode", F.when((F.col("gest_id").isNotNull()) & (F.col("visit_id").isNotNull()) & (F.col("days_diff") < buffer_days*-1), 1).otherwise(0)).filter(F.col("remove_episode") == 1)
    # filter out these episodes from main table
    final_df = final_df.withColumn("remove_episode", F.when((F.col("gest_id").isNotNull()) & (F.col("visit_id").isNotNull()) & (F.col("days_diff") < buffer_days*-1), 1).otherwise(0)).filter(F.col("remove_episode") == 0)
    # reclassify these episodes as PREG
    neg_days_df = neg_days_df.withColumn("removed_category", F.col("category"))
    neg_days_df = neg_days_df.withColumn("category", F.lit("PREG"))
    # set visit date to max_gest_date
    neg_days_df = neg_days_df.withColumn("visit_date", F.lit(F.col("max_gest_date")))    
    # replace the following columns with null values
    neg_days_df = neg_days_df.withColumn("visit_id", F.lit(None))
    neg_days_df = neg_days_df.withColumn("days_diff", F.lit(None))
    neg_days_df = neg_days_df.withColumn("concept_name", F.lit(None))
    neg_days_df = neg_days_df.withColumn("concept_id", F.lit(None))
    neg_days_df = neg_days_df.withColumn("max_term", F.lit(None))
    neg_days_df = neg_days_df.withColumn("min_term", F.lit(None))
    neg_days_df = neg_days_df.withColumn("retry", F.lit(None))
    neg_days_df = neg_days_df.withColumn("min_start_date", F.lit(None))
    neg_days_df = neg_days_df.withColumn("max_start_date", F.lit(None))
    neg_days_df = neg_days_df.withColumn("removed_outcome", F.lit(1)) # flag episode if outcome was removed
    print("Total number of episodes with negative number of days between outcome and max_gest_date.")
    print(neg_days_df.count())
    # join episodes with new values to main table
    final_df = final_df.union(neg_days_df).drop(F.col("remove_episode"))     
    
    ###### add columns for quality check ######

    # get new gestational age at visit date
    final_df = final_df.withColumn("gest_at_outcome", F.datediff(final_df.visit_date, final_df.max_gest_start_date))
    final_df = final_df.withColumn("min_gest_date_diff", F.datediff(final_df.min_gest_date_2, final_df.min_gest_date))
    final_df = final_df.withColumn("date_diff_max_end", F.datediff(final_df.max_gest_date, final_df.end_gest_date))

    # define window by person_id and visit_date
    window = Window.partitionBy("person_id").orderBy("visit_date")
    # redo column for episode
    final_df = final_df.withColumn("episode", F.row_number().over(window))

    print("Total number of episodes with removed outcome.")
    print(final_df.filter(final_df.removed_outcome == 1).count())
    
    return final_df

def remove_overlaps(clean_episodes):
    """
    Identify episodes that overlap and keep only the latter episode if the previous episode is PREG. If the latter episode doesn't have gestational info, redefine the start date to be the previous date plus the retry period.  
    """ 
    df = clean_episodes

    # get previous date
    df = df.withColumn("prev_date", F.lag(df.visit_date, 1).over(Window.partitionBy("person_id").orderBy("visit_date")))

    # get previous category
    df = df.withColumn("prev_category", F.lag(df.category, 1).over(Window.partitionBy("person_id").orderBy("visit_date")))

    # get previous retry period
    df = df.withColumn("prev_retry", F.lag(df.retry, 1).over(Window.partitionBy("person_id").orderBy("visit_date")))

    # get previous gest_id
    df = df.withColumn("prev_gest_id", F.lag(df.gest_id, 1).over(Window.partitionBy("person_id").orderBy("visit_date")))

    # get difference in days between start date and previous visit date
    df = df.withColumn("prev_date_diff", F.when(F.col("max_gest_start_date").isNotNull(), F.datediff(df.max_gest_start_date, df.prev_date)).otherwise(F.datediff(df.max_start_date, df.prev_date)))

    # if the difference in days is negative, indicate overlap of episodes
    df = df.withColumn("has_overlap", F.when(F.col("prev_date_diff") < 0, 1).otherwise(0))

    print(df.filter(df.has_overlap == 1).count())

    # overlapped episodes to keep
    overlap_df = df.filter((df.has_overlap == 1) & (df.prev_category == "PREG"))

    # get list of gest_ids to remove
    gest_id_list = overlap_df.toPandas().prev_gest_id.tolist()

    # remove episodes that overlap
    final_df = df.filter(~((df.gest_id.isin(gest_id_list)) & (df.category == "PREG")))

    # get estimated start date 
    final_df = final_df.withColumn("estimated_start_date", F.when((F.col("has_overlap") == 1) & (F.col("prev_retry").isNotNull()), F.expr("date_add(prev_date, prev_retry)")).otherwise(F.when(F.col("max_gest_start_date").isNull(), F.col("max_start_date")).otherwise(F.col("max_gest_start_date"))))

    # get estimated gestational age in days at outcome_visit_date using estimated_start_date
    final_df = final_df.withColumn("gest_at_outcome", F.datediff(final_df.visit_date, final_df.estimated_start_date))
    # add column to check if gest_at_outcome is less than or equal to max_term, 1 indicates yes
    final_df = final_df.withColumn("is_under_max", F.when(F.col("gest_at_outcome") <= F.col("max_term"), 1).otherwise(0))
    # add column to check if gest_at_outcome is greater than or equal to min_term, 1 indicates yes
    final_df = final_df.withColumn("is_over_min", F.when(F.col("gest_at_outcome") >= F.col("min_term"), 1).otherwise(0))
    
    # define window by person_id and visit_date
    window = Window.partitionBy("person_id").orderBy("visit_date")
    # redo column for episode
    final_df = final_df.withColumn("episode", F.row_number().over(window))

    # check that there are no more overlapping episodes
    # get previous date
    final_df = final_df.withColumn("prev_date_2", F.lag(final_df.visit_date, 1).over(Window.partitionBy("person_id").orderBy("visit_date")))

    # get difference in days between start date and previous visit date
    final_df = final_df.withColumn("prev_date_diff_2", F.when(F.col("estimated_start_date").isNotNull(), F.datediff(final_df.estimated_start_date, final_df.prev_date_2)).otherwise(F.datediff(final_df.estimated_start_date, final_df.prev_date_2)))

    # if the difference in days is negative, indicate overlap of episodes
    final_df = final_df.withColumn("has_overlap_2", F.when(F.col("prev_date_diff_2") < 0, 1).otherwise(0))

    print(final_df.filter(final_df.has_overlap_2 == 1).count())

    print(final_df.filter((final_df.max_gest_week.isNotNull()) & (final_df.concept_name.isNotNull()) & (final_df.is_over_min == 0)).count())

    # if there are any remaining episodes with gestational age in weeks at outcome date not within the term durations, reclassify as PREG
    temp_df = final_df.filter((final_df.max_gest_week.isNotNull()) & (final_df.concept_name.isNotNull()) & (final_df.is_over_min == 0))
    final_df = final_df.filter(~((final_df.max_gest_week.isNotNull()) & (final_df.concept_name.isNotNull()) & (final_df.is_over_min == 0)))
    temp_df = temp_df.withColumn("removed_category", F.col("category"))
    temp_df = temp_df.withColumn("category", F.lit("PREG"))
    # set visit date to max_gest_date
    temp_df = temp_df.withColumn("visit_date", F.lit(F.col("max_gest_date")))    
    # replace the following columns with null values
    temp_df = temp_df.withColumn("visit_id", F.lit(None))
    temp_df = temp_df.withColumn("days_diff", F.lit(None))
    temp_df = temp_df.withColumn("concept_name", F.lit(None))
    temp_df = temp_df.withColumn("concept_id", F.lit(None))
    temp_df = temp_df.withColumn("max_term", F.lit(None))
    temp_df = temp_df.withColumn("min_term", F.lit(None))
    temp_df = temp_df.withColumn("retry", F.lit(None))
    temp_df = temp_df.withColumn("min_start_date", F.lit(None))
    temp_df = temp_df.withColumn("max_start_date", F.lit(None))
    temp_df = temp_df.withColumn("removed_outcome", F.lit(1)) # flag episode if outcome was removed
    print("Total number of episodes with outcome removed.")
    print(temp_df.count())
    # join episodes with new values to main table
    final_df = final_df.union(temp_df)

    print("Total number of episodes with removed outcome.")
    print(final_df.filter(final_df.removed_outcome == 1).count())

    return final_df

def final_episodes(remove_overlaps):
    """
    Keep subset of columns with episode start and end as well as category. Also include patient demographics.
    """
    df = remove_overlaps
    df = df.select("person_id","category","visit_date","estimated_start_date","episode","data_partner_id","year_of_birth","race_concept_name","ethnicity_concept_name")
    return df.drop_duplicates()

def final_episodes_with_length(final_episodes, gestation_visits):
    """
    Find the first gestation record within an episode and calculate the episode length based on the date of the first gestation record and the visit date.
    """
    df = final_episodes
    gest_df = gestation_visits.select("person_id","gest_value","visit_date")
    gest_df = gest_df.withColumnRenamed("visit_date","gest_date")

    # join dataframes
    df = df.join(gest_df, "person_id", "left")

    # check if date is between start and visit date
    df = df.withColumn("within_episode", F.when((F.col("gest_date") >= F.col("estimated_start_date")) & (F.col("gest_date") <= F.col("visit_date")), 1).otherwise(0))

    # filter to episodes without gestation records
    no_gest_df = df.filter(F.col("gest_date").isNull())
    # filter to episodes with gestation records within episodes
    has_gest_df = df.filter(F.col("within_episode") == 1)

    # define window for ranking gestation record dates
    w = Window().partitionBy("person_id","episode").orderBy("gest_date")

    # add column for ranks 
    has_gest_df = has_gest_df.withColumn("rank", F.dense_rank().over(w))

    # first gestation record
    first_gest_df = has_gest_df.filter(F.col("rank") == 1)

    # keep max gest_value if two or more gestation records share same date
    max_gest_df = first_gest_df.groupBy("person_id","episode").agg(F.max("gest_value").alias("max"))

    # join tables
    first_gest_df = first_gest_df.join(max_gest_df, ["person_id","episode"], "inner")

    # filter to records that match the max gest_value
    first_gest_df = first_gest_df.filter(F.col("gest_value") == F.col("max")).drop("rank","max")

    # flag episodes with gestational info
    first_gest_df = first_gest_df.withColumn("gest_flag", F.lit("yes"))

    # get episodes with no corresponding gestational info
    temp_df = df.join(first_gest_df.select("person_id","episode","gest_flag"), ["person_id","episode"], "left")
    # filter to episodes
    not_within_df = temp_df.filter((F.col("gest_date").isNotNull()) & (F.col("gest_flag").isNull())).drop("gest_flag")
    # set columns to none and drop duplicates
    not_within_df = not_within_df.withColumn("gest_date", F.lit(None))
    not_within_df = not_within_df.withColumn("gest_value", F.lit(None)).drop_duplicates()
    
    # join tables
    final_df = no_gest_df.unionByName(first_gest_df.drop("gest_flag"))
    final_df = final_df.unionByName(not_within_df).drop("within_episode")

    # get episode length if there is a gestation record date, otherwise impute 1
    final_df = final_df.withColumn("episode_length", F.when(F.col("gest_date").isNotNull(), F.datediff(final_df.visit_date, final_df.gest_date)).otherwise(1))
    # if an episode length is 0, change to 1
    final_df = final_df.withColumn("episode_length", F.when(F.col("episode_length") == 0, 1).otherwise(F.col("episode_length")))
    # drop columns and any duplicate rows
    final_df = final_df.drop("gest_value").drop_duplicates()

    return final_df    

def main():
    # initial cohort
    initial_pregnant_cohort_df = initial_pregnant_cohort(procedure_occurrence, measurement, observation, condition_occurrence, HIP_concepts, person)
    
    # get stillbirth episodes
    stillbirth_visits_df = stillbirth_visits(initial_pregnant_cohort_df)
    stillbirth_temp_df = stillbirth_temp(stillbirth_visits_df)
    final_stillbirth_visits_df = final_stillbirth_visits(stillbirth_temp_df, stillbirth_visits_df, Matcho_outcome_limits)
    
    # get live birth episodes
    livebirth_visits_df = livebirth_visits(initial_pregnant_cohort_df)
    livebirth_temp_df = livebirth_temp(livebirth_visits_df)
    final_livebirth_visits_df = final_livebirth_visits(livebirth_temp_df, livebirth_visits_df, Matcho_outcome_limits)

    # get ectopic episodes
    ectopic_visits_df = ectopic_visits(initial_pregnant_cohort_df)
    ectopic_temp_df = ectopic_temp(ectopic_visits_df)
    final_ectopic_visits_df = final_ectopic_visits(ectopic_temp_df, ectopic_visits_df, Matcho_outcome_limits)
    
    # get delivery episodes
    delivery_visits_df = delivery_visits(initial_pregnant_cohort_df)
    delivery_temp_df = delivery_temp(delivery_visits_df)
    final_delivery_visits_df = final_delivery_visits(delivery_temp_df, delivery_visits_df, Matcho_outcome_limits)

    # get abortion episodes
    abortion_visits_df = abortion_visits(initial_pregnant_cohort_df)
    abortion_temp_df = abortion_temp(abortion_visits_df)
    final_abortion_visits_df = final_abortion_visits(abortion_temp_df, abortion_visits_df, Matcho_outcome_limits)

    # get gestation based episodes
    gestation_visits_df = gestation_visits(initial_pregnant_cohort_df)
    gestation_episodes_df = gestation_episodes(gestation_visits_df)
    get_min_max_gestation_df = get_min_max_gestation(gestation_episodes_df)
    
    # combine episodes
    add_stillbirth_df = add_stillbirth(final_stillbirth_visits_df, final_livebirth_visits_df, Matcho_outcome_limits)
    add_ectopic_df = add_ectopic(add_stillbirth_df, Matcho_outcome_limits, final_ectopic_visits_df)
    add_abortion_df = add_abortion(add_ectopic_df, Matcho_outcome_limits, final_abortion_visits_df)
    add_delivery_df = add_delivery(add_abortion_df, Matcho_outcome_limits, final_delivery_visits_df)
    calculate_start_df = calculate_start(add_delivery_df, Matcho_term_durations)
    add_gestation_df = add_gestation(calculate_start_df, get_min_max_gestation_df)

    # finalize episodes
    clean_episodes_df = clean_episodes(add_gestation_df)
    remove_overlaps_df = remove_overlaps(clean_episodes_df)
    final_episodes_df = final_episodes(remove_overlaps_df)
    HIP_episodes_df = final_episodes_with_length(final_episodes_df, gestation_visits_df)
   
if __name__ == "__main__":
    main()
