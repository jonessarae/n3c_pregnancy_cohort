from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DateType, DoubleType, ArrayType, StringType, TimestampType, StructField, StructType
from pyspark.sql.functions import unix_timestamp, udf
import pandas as pd
import itertools
import time
from datetime import datetime
from datetime import timedelta
import numpy as np

# external files
Matcho_term_durations = "Matcho_term_durations.xlsx"
specific_gestational_timing_concepts = "PPS_concepts.xlsx"

def get_timing_concepts(condition_occurrence, observation, measurement, procedure_occurrence, final_merged_episode_detailed, specific_gestational_timing_concepts):
    """
    Get gestational timing related concepts for each pregnant person.
    """
    # obtain the gestational timing <= 3 month concept information to use as additional information for precision category designation
    algo2_timing_concepts_df = specific_gestational_timing_concepts.select('domain_concept_id','min_month','max_month')
    algo2_timing_concepts_id_list = algo2_timing_concepts_df.select('domain_concept_id').rdd.flatMap(lambda x: x).collect()

    pregnant_dates = final_merged_episode_detailed
    c_o = condition_occurrence
    o_df = observation
    m_df = measurement
    p_df = procedure_occurrence

    observation_concept_list = [3011536, 3026070, 3024261, 4260747,40758410, 3002549, 43054890, 46234792, 4266763, 40485048, 3048230, 3002209, 3012266]
    measurement_concept_list = [3036844, 3048230, 3001105, 3002209, 3050433, 3012266]

    est_date_of_delivery_concepts = [1175623, 1175623, 3001105, 3011536, 3024261, 3024973, 3026070, 3036322, 3038318, 3038608, 4059478, 4128833, 40490322, 40760182, 40760183, 42537958]
    est_date_of_conception_concepts = [3002314, 3043737, 4058439, 4072438, 4089559, 44817092]
    len_of_gestation_at_birth_concepts = [4260747, 43054890, 46234792, 4266763, 40485048]

    c_o = c_o.filter((F.lower(F.col('condition_concept_name')).contains('gestation period,')) | (F.col('condition_concept_id').isin(observation_concept_list)) | (F.col('condition_concept_id').isin(measurement_concept_list)) | (F.col('condition_concept_id').isin(algo2_timing_concepts_id_list)) | (F.col('condition_concept_id').isin(est_date_of_delivery_concepts)) | (F.col('condition_concept_id').isin(est_date_of_conception_concepts)) | (F.col('condition_concept_id').isin(len_of_gestation_at_birth_concepts)))

    o_df = o_df.filter((F.lower(F.col('observation_concept_name')).contains('gestation period,')) | (F.col('observation_concept_id').isin(observation_concept_list)) | (F.col('observation_concept_id').isin(measurement_concept_list)) | (F.col('observation_concept_id').isin(algo2_timing_concepts_id_list)) | (F.col('observation_concept_id').isin(est_date_of_delivery_concepts)) | (F.col('observation_concept_id').isin(est_date_of_conception_concepts)) | (F.col('observation_concept_id').isin(len_of_gestation_at_birth_concepts)))

    m_df = m_df.filter((F.lower(F.col('measurement_concept_name')).contains('gestation period,')) | (F.col('measurement_concept_id').isin(observation_concept_list)) | (F.col('measurement_concept_id').isin(measurement_concept_list)) | (F.col('measurement_concept_id').isin(algo2_timing_concepts_id_list)) | (F.col('measurement_concept_id').isin(est_date_of_delivery_concepts))| (F.col('measurement_concept_id').isin(est_date_of_conception_concepts))| (F.col('measurement_concept_id').isin(len_of_gestation_at_birth_concepts)))

    p_df = p_df.filter((F.lower(F.col('procedure_concept_name')).contains('gestation period,')) | (F.col('procedure_concept_id').isin(observation_concept_list)) | (F.col('procedure_concept_id').isin(measurement_concept_list)) | (F.col('procedure_concept_id').isin(algo2_timing_concepts_id_list))| (F.col('procedure_concept_id').isin(est_date_of_delivery_concepts))| (F.col('procedure_concept_id').isin(est_date_of_conception_concepts))| (F.col('procedure_concept_id').isin(len_of_gestation_at_birth_concepts)))

    pregnant_dates = pregnant_dates.withColumnRenamed('person_id','personID')

    def get_preg_related_concepts(preg_dates,df,df_date_col,id_col,name_col,val_col):

        pregnant_persons = df.join(preg_dates, (F.col('person_id') == preg_dates.personID) & (F.col(df_date_col) >= preg_dates.recorded_episode_start) & (F.col(df_date_col) <= preg_dates.recorded_episode_end),'inner').drop('personID')

        pregnant_persons = pregnant_persons.withColumnRenamed(df_date_col, 'domain_concept_start_date')
        pregnant_persons = pregnant_persons.withColumnRenamed(id_col, 'domain_concept_id')
        pregnant_persons = pregnant_persons.withColumnRenamed(name_col, 'domain_concept_name')
        pregnant_persons = pregnant_persons.withColumnRenamed(val_col, 'domain_value')

        pregnant_persons = pregnant_persons.select('person_id','domain_concept_start_date','domain_concept_id','domain_concept_name','episode_number','recorded_episode_start','recorded_episode_end','domain_value')
        return pregnant_persons
    
    c_o = c_o.withColumn('value_col',F.col('condition_concept_name'))
    p_df = p_df.withColumn('value_col',F.col('procedure_concept_name'))
    c_o = get_preg_related_concepts(pregnant_dates,c_o,'condition_start_date','condition_concept_id','condition_concept_name','value_col')
    o_df = get_preg_related_concepts(pregnant_dates,o_df,'observation_date','observation_concept_id','observation_concept_name','value_as_string')
    m_df = get_preg_related_concepts(pregnant_dates,m_df,'measurement_date','measurement_concept_id','measurement_concept_name','value_as_number')
    p_df = get_preg_related_concepts(pregnant_dates,p_df,'procedure_date','procedure_concept_id','procedure_concept_name','value_col')

    preg_related_concepts = ((c_o.unionByName(o_df)).unionByName(m_df)).unionByName(p_df)

    # get gestational timing values
    preg_related_concepts = preg_related_concepts.withColumn('domain_value', F.regexp_replace('domain_value', '|text_result_val:', ''))
    preg_related_concepts = preg_related_concepts.withColumn('domain_value', F.regexp_replace('domain_value', '|mapped_text_result_val:', ''))
    preg_related_concepts = preg_related_concepts.withColumn('domain_value', F.regexp_replace('domain_value', 'Gestation period, ', ''))
    preg_related_concepts = preg_related_concepts.withColumn('domain_value', F.regexp_replace('domain_value', 'gestation period, ', ''))
    preg_related_concepts = preg_related_concepts.withColumn('domain_value', F.regexp_replace('domain_value', ' weeks', ''))
   
    # cast values to integer type
    preg_related_concepts = preg_related_concepts.withColumn("domain_value",preg_related_concepts.domain_value.cast(IntegerType()))
    
    # 3048230 = Gestational age in weeks
    # 3002209 - Gestational age Estimated
    # 3012266- Gestational age 
    # extrapolate start date 
    preg_related_concepts = preg_related_concepts.withColumn('extrapolated_preg_start', F.when(F.lower(F.col('domain_concept_name')).contains('gestation period,'),F.expr("date_sub(domain_concept_start_date, (domain_value*7))")).when((F.col('domain_concept_id').isin([3048230,3002209,3012266])) & (F.col("domain_value") < 44), F.expr("date_sub(domain_concept_start_date, (domain_value*7))")).otherwise(''))
    
    # add on any range information where necessary (from the concepts that span <= 3 months during pregnancy)
    preg_related_concepts = preg_related_concepts.join(algo2_timing_concepts_df,'domain_concept_id','left')

    return preg_related_concepts

def episodes_with_gestational_timing_info(get_timing_concepts):
    """
    Estimated Start Date Algorithm - add on the following pieces of information: precision in days, precision category, and inferred start date.
    """
    df = get_timing_concepts
    
    # add on either GW or GR3m designation depending on whether the concept is present
    timing_designation_df = df.withColumn('GT_type',F.when(F.col("domain_concept_name").contains("Gestation period"), F.lit('GW')).otherwise(F.when(F.col('domain_concept_id').isin([3048230,3002209,3012266]), F.lit('GW')).otherwise(F.when(F.col('min_month').isNotNull(),F.lit('GR3m')).otherwise(F.lit('')))))

    ''' 
    Add on the max and min pregnancy start dates predicted by each concept
        - for the GW concepts, this has already been calculated in the column extrapolated_preg_start
        - add on the ranges from each of the GR3m concepts
    '''
    timing_designation_df = timing_designation_df.withColumn('min_days_to_pregnancy_start', F.when((F.col('GT_type') == 'GR3m'), (F.expr("round(min_month*30.4)")).cast(IntegerType()))) \
                                                 .withColumn('max_days_to_pregnancy_start', F.when((F.col('GT_type') == 'GR3m'), (F.expr("round(max_month*30.4)")).cast(IntegerType())))

    # add the max and min possible pregnancy start dates according to the GR3m concepts
    timing_designation_df = timing_designation_df.withColumn('min_pregnancy_start', F.when((F.col('GT_type') == 'GR3m'), F.expr("date_sub(domain_concept_start_date, min_days_to_pregnancy_start)"))) \
                                                 .withColumn('max_pregnancy_start', F.when((F.col('GT_type') == 'GR3m'), F.expr("date_sub(domain_concept_start_date, max_days_to_pregnancy_start)"))) \
                                                 .drop('min_days_to_pregnancy_start') \
                                                 .drop('max_days_to_pregnancy_start')

    # remove type if null values
    timing_designation_df = timing_designation_df.withColumn("GT_type", F.when((F.col("GT_type")=="GW") & (F.col("domain_value").isNull()), F.lit('')).otherwise(F.when((F.col("GT_type")=="GW") & (F.col("extrapolated_preg_start") == ''), F.lit('')).otherwise(F.col("GT_type"))))

    # filter to rows with GW or GR3m type
    timing_designation_df = timing_designation_df.filter((F.col('GT_type')=="GW") | (F.col('GT_type')=="GR3m"))

    # format dates
    timing_designation_df = timing_designation_df.withColumn('min_pregnancy_start', F.to_date(F.col('min_pregnancy_start'), "yyyy-MM-dd"))
    timing_designation_df = timing_designation_df.withColumn('max_pregnancy_start', F.to_date(F.col('max_pregnancy_start'), "yyyy-MM-dd"))
    timing_designation_df = timing_designation_df.withColumn('extrapolated_preg_start', F.to_date(F.col('extrapolated_preg_start'), "yyyy-MM-dd"))
    # convert dates to string type
    timing_designation_df = timing_designation_df.withColumn('min_pregnancy_start', F.col('min_pregnancy_start').cast(StringType()))
    timing_designation_df = timing_designation_df.withColumn('max_pregnancy_start', F.col('max_pregnancy_start').cast(StringType()))
    timing_designation_df = timing_designation_df.withColumn('extrapolated_preg_start', F.col('extrapolated_preg_start').cast(StringType()))
    # get start date range for GR3m
    timing_designation_df = timing_designation_df.withColumn('preg_start_range',F.array([F.col('min_pregnancy_start'), F.col('max_pregnancy_start')]))
    # get start dates for GW
    timing_designation_df = timing_designation_df.withColumn('extr', F.array(F.col('extrapolated_preg_start')))
    # create list of all dates
    timing_designation_df = timing_designation_df.withColumn('all_GT_info',F.when((F.col('extr')[0]).isNull(),F.col('preg_start_range')).otherwise(F.col('extr')))

    # add on a categorization column to ensure gestation week concepts are distinguished as a single entity and others are unique per concept (important for the next steps of removing duplicates)
    timing_designation_df = timing_designation_df.withColumn('domain_concept_name_rollup', F.when(F.col('domain_value').isNotNull(),F.lit('Gestation Week')).otherwise(F.col('domain_concept_name')))

    # IMPORTANT: sort by domain value (gest week concepts) from highest (latest in pregancy) to lowest (earliest in pregnancy) so that later on the first element of the GW list can be taken for 'latest in pregnancy' concept 
    timing_designation_df = timing_designation_df.orderBy(F.col("person_id").asc(), F.col("episode_number").asc(), F.col("domain_value").desc())

    # remove all rows but the first for each concept date ~ GT_type combination (as these are likely to be historical references)
    timing_designation_df = timing_designation_df.dropDuplicates(['person_id','episode_number','domain_concept_name_rollup','domain_concept_start_date', 'GT_type'])

    # IMPORTANT: re-sort by domain value (as the dropDuplicates process mixes up the order again)
    timing_designation_df = timing_designation_df.orderBy(F.col("person_id").asc(), F.col("episode_number").asc(), F.col("domain_value").desc())

    # group the dataset by patient and episode number and pass relevant columns to a function that adds inferred_start_date and precision
    new_timing_designation_df = timing_designation_df.groupBy('person_id','episode_number').agg(F.collect_list('all_GT_info').alias('GT_info_list'))

    # add flag for gestation week concepts
    gw_df = timing_designation_df.filter(F.col('GT_type') == 'GW').groupBy('person_id','episode_number').agg(F.collect_list('GT_type').alias('GW_flag'))

    # add flag for other concepts
    gr3m_df = timing_designation_df.filter(F.col('GT_type') == 'GR3m').groupBy('person_id','episode_number').agg(F.collect_list('GT_type').alias('GR3m_flag'))

    # join dataframes
    new_timing_designation_df = new_timing_designation_df.join(gw_df, on=['person_id','episode_number'], how='left')

    new_timing_designation_df = new_timing_designation_df.join(gr3m_df, on=['person_id','episode_number'], how='left')

    # create a unique pregnancy ID column
    timing_designation_df = new_timing_designation_df.withColumn('pregnancyID', F.concat(F.col("person_id"), F.lit("-"), F.col("episode_number"))) \
                                                 .select(F.col("pregnancyID"), F.col("GT_info_list"), F.col("GW_flag"), F.col("GR3m_flag"))

    def validate(date_text):
        try:
            datetime.strptime(date_text, '%Y-%m-%d')
        except ValueError:
            #raise ValueError("Incorrect data format, should be YYYY-MM-DD")
            return False

    # applying udf to the date array column to obtain the new column 'final_timing_info' (a list of [inferred_episode_start, precision_days, precision_category]) for each row
    def get_gt_timing(dateslist):
        GW_list = []
        GR3m_list = []
        timing_arr = []
        for i in range(0,len(dateslist)):
            gtEntry = dateslist[i]
            dto = [datetime.strptime(date_str, '%Y-%m-%d').date() for date_str in gtEntry if ((date_str is not None) and (validate(date_str) is not False))]
            # sort the dates earliest to latest
            dto = sorted(dto)
            if len(dto) > 0:
                timing_arr.append(dto)
                if len(dto) == 1:
                    GW_list.append(dto)
                elif len(dto) == 2:
                    GR3m_list.append(dto)

        # accessible date info is now saved per pregnancy
        # and we now have our GW and GR3m info distinguished and separated into two lists, and can parse through them each as well as compare them
        inferred_start_date = datetime(2000, 1, 1)
        precision_days = 999
        precision_category = '-999'
        intervalsCount = 0
        majorityOverlapCount = 0
        # obtain the intersection of all GR3m date ranges
        def findIntersection(intervals,N):

            # sort intervals
            intervals = sorted(intervals)
        
            # first remove outlier ranges via the IQR*1.5 approach. Outlier ranges are determined by the number of overlaps each range has with other ranges.
            overlapCountDict = {}
            for j in range(N):
                overlapCountDict[j] = 0
                for m in range(N):
                    if j != m:
                        # First interval
                        last = intervals[j][1]
                        first = intervals[j][0]
                        # 1st condition - does first date equal either the first or last date
                        if (intervals[m][0] == last) or (intervals[m][0] == first):
                            overlapCountDict[j] += 1
                            continue
                        # 2nd condition - does second date equal either the first or last date
                        elif (intervals[m][1] == last) or (intervals[m][1] == first):
                            overlapCountDict[j] += 1
                            continue
                        # 3rd condition - does second date fall between the first and last date
                        elif (intervals[m][1] < last) and (intervals[m][1] > first):
                            overlapCountDict[j] += 1
                            continue
                        # 4th condition - does first date fall between the first and last date
                        elif (intervals[m][0] < last) and (intervals[m][0] > first):
                            overlapCountDict[j] += 1
                            continue
                        else:
                            continue

            allCounts = [overlapCountDict[k] for k in overlapCountDict]
            countsQ1 = np.quantile(allCounts, .25)
            countsQ3 = np.quantile(allCounts, .75)
            outlierMetric = (countsQ3 - countsQ1) * 1.5
            # get absolute value
            outlierThreshold = np.abs(countsQ1 - outlierMetric) 

            # get indices of outliers to remove
            outlierPositionsToRemove = []
            for trange in overlapCountDict:
                oc = overlapCountDict[trange]
                if outlierThreshold == 0:
                    if oc == outlierThreshold:
                        outlierPositionsToRemove.append(trange)
                else:
                    if oc < outlierThreshold:
                        outlierPositionsToRemove.append(trange)

            # sort list based on number of overlaps
            sort_list = sorted(overlapCountDict, key=overlapCountDict.get, reverse=True)

            # get new index of outliers to remove
            newOutlierPositionsToRemove = []
            for idx,i in enumerate(sort_list):
                for j in outlierPositionsToRemove:
                    if i == j:
                        newOutlierPositionsToRemove.append(idx)

            intervalsToSort =  [intervals[i] for i in sort_list]
            intervalsToRemove = [intervalsToSort[i] for i in newOutlierPositionsToRemove]
            filteredIntervals = [i for i in intervalsToSort if i not in intervalsToRemove]

            ### now the outliers are removed, proceed with obtaining the overlaps ###

            # get new N
            N = len(filteredIntervals)

            # if only one interval
            if N == 1:
                # First interval
                last = filteredIntervals[0][1] # last day of overlapping ranges
                min_start = filteredIntervals[0][1] # minimum day of intersection of ranges
                first = filteredIntervals[0][0] # first day of overlapping ranges
                max_start = filteredIntervals[0][0] # maximum day of intersection of ranges
                intersection_list = [last,first,min_start,max_start]
            # if no intervals, take the first only if more than one
            elif N == 0:
                # First interval
                last = intervalsToSort[0][1]
                min_start = intervalsToSort[0][1]
                first = intervalsToSort[0][0]
                max_start = intervalsToSort[0][0]
                intersection_list = [last,first,min_start,max_start]
            else:
                # First interval
                last = filteredIntervals[0][1] # last day of overlapping ranges
                min_start = filteredIntervals[0][1] # minimum (last) day of intersection of ranges
                first = filteredIntervals[0][0] # first day of overlapping ranges
                max_start = filteredIntervals[0][0] # maximum (first) day of intersection of ranges

                # check rest of the intervals and find the intersection
                for i in range(1,N):
    
                    if filteredIntervals[i][0] < first:
                        first = filteredIntervals[i][0]
                    if filteredIntervals[i][1] > last:
                        last = filteredIntervals[i][1]
                    if (filteredIntervals[i][1] < min_start) and (filteredIntervals[i][1] > max_start):
                        min_start = filteredIntervals[i][1]
                    if (filteredIntervals[i][0] > max_start) and (filteredIntervals[i][0] < min_start):
                        max_start = filteredIntervals[i][0]
                                
                intersection_list = [last,first,min_start,max_start]
            
            return intersection_list
        
        # get length of list with GR3m ranges
        N = len(GR3m_list)
        if N > 0:
            common_GR3m_interval = findIntersection(GR3m_list, N)
        else:
            common_GR3m_interval = None

        plausibleDays = 0
        maxRangeDays = 0
        range_s = None
        range_e = None
        interval_s = None
        interval_e = None
        daterangeMidpoint = None
        if common_GR3m_interval: # if it's not an empty array
            range_e = common_GR3m_interval[0] # end date of range
            range_s = common_GR3m_interval[1] # start date of range
            interval_e = common_GR3m_interval[2] # end date of intersection
            interval_s = common_GR3m_interval[3] # start date of intersection
            plausibleDays = (interval_e - interval_s).days
            maxRangeDays = (range_e - range_s).days
            daterangeMidpoint = interval_s + timedelta(days=(plausibleDays/2)) # get midpoint of intersection
            # utilize the overlap more when it gets narrowed down to less than a week by taking the midpoint and adding 3 days either side (otherwise unlikely to overlap much with GW concepts and thus will be ignored)
            if (plausibleDays < 7):
                interval_s = daterangeMidpoint - timedelta(days=3)
                interval_e = daterangeMidpoint + timedelta(days=3)
                plausibleDays = (interval_e - interval_s).days

        # check for GW concept overlap to the intervals
        def remove_GW_outliers(lol_of_GW_concepts):
            list_of_GW_concepts = [item for sublist in lol_of_GW_concepts for item in sublist]
            # obtain median
            median_startdate = sorted(list_of_GW_concepts)[int(np.floor(len(list_of_GW_concepts)/2))]

            distFromMedianDict = {}
            for j in range(len(list_of_GW_concepts)):
                distFromMedianDict[j] = (max(list_of_GW_concepts[j],median_startdate) - min(list_of_GW_concepts[j],median_startdate)).days
            allDistances = [distFromMedianDict[k] for k in distFromMedianDict]
            distQ1 = np.quantile(allDistances, .25)
            distQ3 = np.quantile(allDistances, .75)
            outlierMetric = (distQ3 - distQ1) * 1.5
            outlierLowerThreshold = distQ1 - outlierMetric
            outlierUpperThreshold = distQ3 + outlierMetric
            outlierThresholds = [outlierLowerThreshold,outlierUpperThreshold]

            outlierPositionsToRemove = []
            for trange in distFromMedianDict:
                d = distFromMedianDict[trange]
                if ((d < outlierThresholds[0]) or (d > outlierThresholds[1])):
                    outlierPositionsToRemove.append(trange)
            datesToRemove = [list_of_GW_concepts[i] for i in outlierPositionsToRemove]
            filteredDates = [i for i in list_of_GW_concepts if i not in datesToRemove]
            return filteredDates

        if len(GW_list) > 0:
            if interval_s: # GR3m interval is not null
                intervalsCount += 1
                # check for overlaps with GR3m concept ranges
                gwConceptCount = len(GW_list)
                overlapping_gwConcepts = []
                for gwlist in GW_list:
                    gw_concept_start_date = gwlist[0]
                    if ((gw_concept_start_date >= interval_s) and (gw_concept_start_date <= interval_e)):
                        overlapping_gwConcepts.append([gw_concept_start_date])
                overlapping_gwConcepts_count = len(overlapping_gwConcepts)
                perc_overlapping = (overlapping_gwConcepts_count / gwConceptCount) * 100
                if (perc_overlapping > 50):
                    majorityOverlapCount += 1
                    # remove outliers
                    filtDates = remove_GW_outliers(overlapping_gwConcepts)
                    inferred_start_date = filtDates[0] # first element is the latest GW concept (sorted by domain value above)
                    precision_days = (max(filtDates) - min(filtDates)).days
                else: # don't bother using the overlap
                    filtDates = remove_GW_outliers(GW_list)
                    inferred_start_date = filtDates[0] # first element is the latest GW concept (sorted by domain value above)
                    precision_days = (max(filtDates) - min(filtDates)).days
                    if (len(filtDates) == 1):
                        precision_days = -1
            else: # no GR3m intersection
                filtDates = remove_GW_outliers(GW_list)
                inferred_start_date = filtDates[0] # first element is the latest GW concept (sorted by domain value above)
                precision_days = (max(filtDates) - min(filtDates)).days
                if (len(filtDates) == 1):
                    precision_days = -1
        else: # no GW concepts
            inferred_start_date = daterangeMidpoint
            precision_days = maxRangeDays
       
        def assign_precision_category(precision):
            if precision_days == -1:
                precision_category = 'week_poor-support'
            elif ((precision_days >= 0) and (precision_days <= 7)):
                precision_category = 'week'
            elif ((precision_days > 7) and (precision_days <= 14)):
                precision_category = 'two-week'
            elif ((precision_days > 14) and (precision_days <= 21)):
                precision_category = 'three-week'
            elif ((precision_days > 21) and (precision_days <= 28)):
                precision_category = 'month'
            elif ((precision_days > 28) and (precision_days <= 56)):
                precision_category = 'two-month'
            elif ((precision_days > 56) and (precision_days <= 84)):
                precision_category = 'three-month'
            else:
                precision_category = 'non-specific'
            return precision_category

        inferred_start_date = inferred_start_date.strftime("%Y-%m-%d")
        precision_category = assign_precision_category(precision_days)
        precision_days = str(precision_days)
        intervalsCount = str(intervalsCount)
        majorityOverlapCount = str(majorityOverlapCount)
        single_episode_timingres = [inferred_start_date,precision_days,precision_category,intervalsCount,majorityOverlapCount]
        return single_episode_timingres

    get_gt_timingUDF = udf(lambda z:get_gt_timing(z),ArrayType(StringType()))
    timing_designation_df = timing_designation_df.withColumn("final_timing_info", get_gt_timingUDF(F.col("GT_info_list")))

    # split the array into columns
    timing_designation_df = timing_designation_df.select('pregnancyID', 'GT_info_list','GW_flag','GR3m_flag', F.col("final_timing_info")[0], F.col("final_timing_info")[1], F.col("final_timing_info")[2],F.col("final_timing_info")[3],F.col("final_timing_info")[4]) \
                                                 .withColumnRenamed('final_timing_info[0]','inferred_episode_start') \
                                                 .withColumnRenamed('final_timing_info[1]','precision_days') \
                                                 .withColumnRenamed('final_timing_info[2]','precision_category') \
                                                 .withColumnRenamed('final_timing_info[3]','intervalsCount') \
                                                 .withColumnRenamed('final_timing_info[4]','majorityOverlapCount') \

    timing_designation_df = timing_designation_df.select(F.split(timing_designation_df.pregnancyID, '-').alias('pregIDlist'),'GT_info_list','GW_flag','GR3m_flag','inferred_episode_start','precision_days','precision_category','intervalsCount','majorityOverlapCount')

    timing_designation_df = timing_designation_df.select(F.col("pregIDlist")[0],F.col("pregIDlist")[1],'GT_info_list','GW_flag','GR3m_flag','inferred_episode_start','precision_days','precision_category','intervalsCount','majorityOverlapCount') \
                                                 .withColumnRenamed('pregIDlist[0]','person_id') \
                                                 .withColumnRenamed('pregIDlist[1]','episode_number')

    # print the GW and GR3m concept overlap information to log
    timing_designation_df = timing_designation_df.withColumn('majorityOverlapCount',F.col('majorityOverlapCount').cast(IntegerType())) \
                                                 .withColumn('intervalsCount',F.col('intervalsCount').cast(IntegerType()))
    majorityOverlapCountTotal = timing_designation_df.agg(F.sum(F.col('majorityOverlapCount'))).collect()[0][0]
    intervalsCountTotal = timing_designation_df.agg(F.sum(F.col('intervalsCount'))).collect()[0][0]
    percMajority = (majorityOverlapCountTotal / intervalsCountTotal)*100
    print('number of episodes with GR3m intervals:')
    print(intervalsCountTotal)
    print('percent of cases that contain a GR3m intersection that ALSO have majority GW overlap:')
    print(percMajority)

    timing_designation_df = timing_designation_df.drop('intervalsCount','majorityOverlapCount')
    
    # convert GW_flag and GR3m_flag to 0 or 1
    timing_designation_df = timing_designation_df.withColumn("GW_flag", F.when(F.col("GW_flag").isNotNull(), 1).otherwise(0))
    timing_designation_df = timing_designation_df.withColumn("GR3m_flag", F.when(F.col("GR3m_flag").isNotNull(), 1).otherwise(0))

    return timing_designation_df
    
def merged_episodes_with_metadata(episodes_with_gestational_timing_info, final_merged_episode_detailed, Matcho_term_durations):
    """
    Add other pregnancy and demographic related info for each episode.
    """
    demographics_df = final_merged_episode_detailed
    timing_df = episodes_with_gestational_timing_info.drop('GT_info_list')
    term_max_min = Matcho_term_durations

    final_df = demographics_df.join(timing_df,['person_id','episode_number'],'left').distinct()

    # add missing GW_flag and GR3m_flag
    final_df = final_df.withColumn("GW_flag", F.when(F.col("GW_flag").isNull(), 0).otherwise(F.col("GW_flag")))
    final_df = final_df.withColumn("GR3m_flag", F.when(F.col("GR3m_flag").isNull(), 0).otherwise(F.col("GR3m_flag")))

    # check if categories match between algorithms and dates are within 14 days of each other for outcomes only
    final_df = final_df.withColumn("outcome_match", F.when((F.col("HIP_outcome_category") == F.col("PPS_outcome_category")) & (F.col("HIP_outcome_category") != "PREG") & (F.abs(F.datediff(F.col("HIP_end_date"), F.col("PPS_end_date"))) <= 14), 1).otherwise(F.when((F.col("HIP_outcome_category") == "PREG") & (F.col("PPS_outcome_category") == "PREG"), 1).otherwise(0)))

    # if categories don't match, take the category that occurs second (outcome category from HIP algorithm is prioritized)
    final_df = final_df.withColumn("final_outcome_category", F.when(F.col("outcome_match") == 1, F.col("HIP_outcome_category")).otherwise(F.when((F.col("outcome_match") == 0) & (F.col("PPS_outcome_category").isNull()), F.col("HIP_outcome_category")).otherwise(F.when((F.col("outcome_match") == 0) & (F.col("HIP_outcome_category").isNull()), F.col("PPS_outcome_category")).otherwise(F.when((F.col("outcome_match") == 0) &
(F.col("HIP_outcome_category") != "PREG") & (F.col("PPS_outcome_category") != "PREG") & (F.col("HIP_end_date") <= F.expr("date_sub(PPS_end_date, 7)")), F.col("PPS_outcome_category")).otherwise(F.col("HIP_outcome_category"))))))

    # if categories don't match, take the end date that occurs second (outcome date from HIP is prioritized)
    final_df = final_df.withColumn("inferred_episode_end", F.when(F.col("outcome_match") == 1, F.col("HIP_end_date")).otherwise(F.when((F.col("outcome_match") == 0) & (F.col("PPS_outcome_category").isNull()), F.col("HIP_end_date")).otherwise(F.when((F.col("outcome_match") == 0) & (F.col("HIP_outcome_category").isNull()), F.col("PPS_end_date")).otherwise(F.when((F.col("outcome_match") == 0) & (F.col("HIP_outcome_category") != "PREG") & (F.col("PPS_outcome_category") != "PREG") & (F.col("HIP_end_date") <= F.expr("date_sub(PPS_end_date, 7)")), F.col("PPS_end_date")).otherwise(F.col("HIP_end_date"))))))

    final_df = final_df.join(term_max_min,final_df.final_outcome_category == term_max_min.category,'left').drop('retry')

    # if no start date, substract the max term in days from inferred episode end
    final_df = final_df.withColumn("inferred_episode_start", F.when(final_df.inferred_episode_start.isNull(), F.expr("date_sub(inferred_episode_end, max_term)")).otherwise(F.col("inferred_episode_start")))

    # definition to get accuracy categroy 
    def assign_precision_category(precision_days):
        if precision_days == -1:
            precision_category = 'week_poor-support'
        elif ((precision_days >= 0) and (precision_days <= 7)):
            precision_category = 'week'
        elif ((precision_days > 7) and (precision_days <= 14)):
            precision_category = 'two-week'
        elif ((precision_days > 14) and (precision_days <= 21)):
            precision_category = 'three-week'
        elif ((precision_days > 21) and (precision_days <= 28)):
            precision_category = 'month'
        elif ((precision_days > 28) and (precision_days <= 56)):
            precision_category = 'two-month'
        elif ((precision_days > 56) and (precision_days <= 84)):
            precision_category = 'three-month'
        else:
            precision_category = 'non-specific'
        return precision_category

    # convert to UDF definition 
    get_precision_UDF = F.udf(lambda z:assign_precision_category(z))

    # convert to integer type
    final_df = final_df.withColumn("precision_days", F.col("precision_days").cast(IntegerType()))
    
    # add missing accuracy info for remaining episodes
    final_df = final_df.withColumn("precision_days", F.when(F.col("precision_days").isNull(), F.col("max_term")-F.col("min_term")).otherwise(F.col("precision_days")))

    # add missing accuracy category for remaining episodes
    final_df = final_df.withColumn("precision_category", F.when(F.col("precision_category").isNull(), get_precision_UDF(F.col("precision_days"))).otherwise(F.col("precision_category")))

    # calculate gestational age at inferred episode end
    final_df = final_df.withColumn("gestational_age_days_calculated", F.datediff(final_df.inferred_episode_end, final_df.inferred_episode_start))

    # check if outcome aligns with term duration expected of that outcome
    final_df = final_df.withColumn('term_duration_flag', F.when((F.col('gestational_age_days_calculated') >= F.col('min_term')) & (F.col('gestational_age_days_calculated') <= F.col('max_term')), 1).otherwise(F.when((F.col("final_outcome_category") == "PREG") & (F.col("gestational_age_days_calculated") <= 301), 1).otherwise(0))).drop(*['min_term','max_term','category'])

    # add outcome concordance score - 2 highly concordant, 1 somewhat concordant, 0 not accurate/not enough info
    final_df = final_df.withColumn("outcome_concordance_score", F.when((F.col("outcome_match") == 1) & (F.col("term_duration_flag") == 1) & (F.col("GW_flag").isNotNull()), 2).otherwise(F.when((F.col("outcome_match") == 0) & (F.col("term_duration_flag") == 1) & (F.col("GW_flag").isNotNull()), 1).otherwise(0)))

    # create day of birth column and fill it with 1
    final_df = final_df.withColumn("day_of_birth", F.lit(1))
    # create date_of_birth column
    final_df = final_df.withColumn("date_of_birth",F.concat_ws("-", F.col("year_of_birth"), F.col("month_of_birth"), F.col("day_of_birth")).cast("date"))
    # get difference in days between start date of pregnancy and date of birth
    final_df = final_df.withColumn("birth_date_diff", F.datediff(final_df.inferred_episode_start, final_df.date_of_birth))
    # calculate age from date_diff
    final_df = final_df.withColumn("age_at_inferred_start", (F.col("birth_date_diff")/365))

    # add race
    final_df = final_df.withColumn("race", F.when(final_df.race_concept_name.isin(["Asian","Asian Indian","Chinese","Korean","Vietnamese","Filipino","Japanese"]),"Asian").otherwise(
                               F.when(final_df.race_concept_name.isin(["Black","Black or African American"]),"Black").otherwise(
                               F.when(final_df.race_concept_name.isin(["Other Pacific Islander","Native Hawaiian or Other Pacific Islander","Polynesian"]), "Native Hawaiian or Other Pacific Islander").otherwise(
                               F.when(final_df.race_concept_name.isin(["No information", "No matching concept","Refuse to answer","Unknown","Unknown racial group"]),"Unknown").otherwise(
                               F.when(final_df.race_concept_name.isNull(),"Unknown").otherwise(
                               F.when(final_df.race_concept_name=="White","White")
                               .otherwise("Other")))))))                           

    # add ethnicity                           
    final_df = final_df.withColumn("ethnicity", F.when(final_df.ethnicity_concept_name=="Not Hispanic or Latino","Not Hispanic or Latino").otherwise(
                                    F.when(final_df.ethnicity_concept_name=="Hispanic or Latino","Hispanic or Latino").otherwise(
                                    F.when(final_df.race_concept_name=="Hispanic","Hispanic or Latino")
                                    .otherwise("Unknown"))))

    # add race_ethnicity 
    final_df = final_df.withColumn("race_ethnicity", F.when(final_df.ethnicity == "Hispanic or Latino", "Hispanic or Latino Any Race").otherwise(
                                         F.when(final_df.race == "Unknown", "Unknown").otherwise(
                                         F.concat_ws(" ", final_df.race, F.lit("Non-Hispanic")))))

    final_df = final_df.withColumn("preterm_status_from_calculation", F.when((final_df.gestational_age_days_calculated < 259),1).otherwise(0)) # less than 37 weeks

    # convert dates from string type to date type
    final_df = final_df.withColumn('inferred_episode_start', F.to_date(F.col('inferred_episode_start'), "yyyy-MM-dd"))
    final_df = final_df.withColumn('inferred_episode_end', F.to_date(F.col('inferred_episode_end'), "yyyy-MM-dd"))

    # print the min episode/pregnancy start and max episode/pregnancy end to check on time period of dataset 
    min_episode_date, max_episode_date = final_df.select(F.min("recorded_episode_start"), F.max("recorded_episode_end")).first()
    min_pregnancy_date, max_pregnancy_date = final_df.select(F.min("inferred_episode_start"), F.max("inferred_episode_end")).first()
    print('min episode start date')
    print(min_episode_date)
    print('max episode end date')
    print(max_episode_date)
    print('min pregnancy start date')
    print(min_pregnancy_date)
    print('max pregnancy end date')
    print(max_pregnancy_date)

    return final_df

def main():
    get_timing_concepts_df = get_timing_concepts(condition_occurrence, observation, measurement, procedure_occurrence, final_merged_episode_detailed_df, specific_gestational_timing_concepts)
    episodes_with_gestational_timing_info_df = episodes_with_gestational_timing_info(get_timing_concepts_df)
    merged_episodes_with_metadata_df = merged_episodes_with_metadata(episodes_with_gestational_timing_info_df, final_merged_episode_detailed_df, Matcho_term_durations)

if __name__ == "__main__":
    main()
