from pyspark.sql import functions as F, Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

from .utils import jaccard_distance


def build_eligible_from_eras(era_df, observation_period, death, have_death, cfg):
    washout_days = cfg["analysis"]["washout_days"]
    followup_months = cfg["analysis"]["followup_months"]

    first_era = (
        era_df
        .groupBy("person_id")
        .agg(F.min("era_start_date").alias("index_date"))
    )

    obs_candidates = (
        first_era.alias("f")
        .join(
            observation_period.alias("o"),
            (F.col("f.person_id") == F.col("o.person_id")) &
            (F.col("f.index_date") >= F.col("o.observation_period_start_date")) &
            (F.col("f.index_date") <= F.col("o.observation_period_end_date")) &
            (F.datediff(F.col("f.index_date"), F.col("o.observation_period_start_date")) >= washout_days),
            "inner"
        )
        .select(
            F.col("f.person_id"),
            "index_date",
            "observation_period_start_date",
            "observation_period_end_date"
        )
    )

    w_obs = Window.partitionBy("person_id").orderBy(F.col("observation_period_end_date").desc())

    eligible = (
        obs_candidates
        .withColumn("rn", F.row_number().over(w_obs))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    if have_death:
        eligible = (
            eligible
            .join(death, on="person_id", how="left")
            .withColumn("max_followup_date", F.date_sub(F.add_months(F.col("index_date"), followup_months), 1))
            .withColumn("death_or_far_future", F.coalesce(F.col("death_date"), F.to_date(F.lit("2100-01-01"))))
            .withColumn("censor_date", F.least("observation_period_end_date", "max_followup_date", "death_or_far_future"))
            .drop("max_followup_date", "death_or_far_future")
        )
    else:
        eligible = (
            eligible
            .withColumn(
                "censor_date",
                F.least(
                    F.col("observation_period_end_date"),
                    F.date_sub(F.add_months(F.col("index_date"), followup_months), 1)
                )
            )
        )

    return eligible.filter(F.col("censor_date") >= F.col("index_date"))


def build_primary_eras(drug_era):
    return drug_era.select(
        "person_id",
        "ingredient_concept_id",
        F.col("drug_era_start_date").alias("era_start_date"),
        F.col("drug_era_end_date").alias("era_end_date"),
        "drug_exposure_count",
        "gap_days"
    )


def build_exposure_derived_eras(drug_exposure, concept, concept_ancestor, cfg):
    gap_days = cfg["analysis"]["exposure_gap_days"]

    de = (
        drug_exposure
        .withColumn(
            "exposure_end_date",
            F.coalesce(
                F.col("drug_exposure_end_date"),
                F.when(
                    F.col("days_supply").isNotNull() & (F.col("days_supply") > 0),
                    F.date_add(F.col("drug_exposure_start_date"), F.col("days_supply") - 1)
                ),
                F.col("drug_exposure_start_date")
            )
        )
        .filter(F.col("exposure_end_date") >= F.col("drug_exposure_start_date"))
    )

    ingredient_map = (
        concept_ancestor.alias("ca")
        .join(
            concept.alias("c"),
            F.col("ca.ancestor_concept_id") == F.col("c.concept_id"),
            "inner"
        )
        .filter(
            (F.col("c.domain_id") == "Drug") &
            (F.col("c.concept_class_id") == "Ingredient")
        )
        .select(
            F.col("ca.descendant_concept_id").alias("drug_concept_id"),
            F.col("ca.ancestor_concept_id").alias("ingredient_concept_id")
        )
        .distinct()
    )

    de_ing = (
        de.alias("d")
        .join(ingredient_map.alias("m"), on="drug_concept_id", how="inner")
        .select(
            F.col("d.person_id"),
            F.col("m.ingredient_concept_id"),
            F.col("d.drug_exposure_start_date").alias("start_date"),
            F.col("d.exposure_end_date").alias("end_date")
        )
        .filter(F.col("start_date").isNotNull() & F.col("end_date").isNotNull())
        .filter(F.col("end_date") >= F.col("start_date"))
    )

    de_ing = (
        de_ing
        .groupBy("person_id", "ingredient_concept_id", "start_date")
        .agg(F.max("end_date").alias("end_date"))
    )

    w_ord = Window.partitionBy("person_id", "ingredient_concept_id").orderBy("start_date", "end_date")
    w_run = w_ord.rowsBetween(Window.unboundedPreceding, 0)

    merged = (
        de_ing
        .withColumn("running_end", F.max("end_date").over(w_run))
        .withColumn("prev_running_end", F.lag("running_end").over(w_ord))
        .withColumn(
            "new_group",
            F.when(
                F.col("prev_running_end").isNull() |
                (F.col("start_date") > F.date_add(F.col("prev_running_end"), gap_days)),
                1
            ).otherwise(0)
        )
        .withColumn("era_group", F.sum("new_group").over(w_ord))
    )

    return (
        merged
        .groupBy("person_id", "ingredient_concept_id", "era_group")
        .agg(
            F.min("start_date").alias("era_start_date"),
            F.max("end_date").alias("era_end_date"),
            F.count("*").alias("drug_exposure_count")
        )
        .withColumn("gap_days", F.lit(gap_days))
        .select(
            "person_id", "ingredient_concept_id",
            "era_start_date", "era_end_date",
            "drug_exposure_count", "gap_days"
        )
    )


def run_trajectory_pipeline(era_input_df, observation_period, death, have_death, ingredient_concepts, cfg, label):
    followup_months = cfg["analysis"]["followup_months"]
    maintenance_min_total_days = cfg["analysis"]["maintenance_min_total_days"]
    early_discontinuation_days = cfg["analysis"]["early_discontinuation_days"]
    restart_window_days = cfg["analysis"]["restart_window_days"]
    switch_window_days = cfg["analysis"]["switch_window_days"]
    polypharmacy_threshold = cfg["analysis"]["polypharmacy_threshold"]
    turnover_low = cfg["analysis"]["turnover_low"]
    turnover_high = cfg["analysis"]["turnover_high"]
    k_grid = cfg["clustering"]["k_grid"]
    seed = cfg["clustering"]["seed"]
    outdir = cfg["project"]["output_dir"]
    top_n = cfg["run"]["save_top_ingredients_per_cluster"]

    eligible = build_eligible_from_eras(
        era_df=era_input_df,
        observation_period=observation_period,
        death=death,
        have_death=have_death,
        cfg=cfg
    )

    eras = (
        era_input_df.alias("d")
        .join(
            eligible.select("person_id", "index_date", "censor_date").alias("e"),
            on="person_id",
            how="inner"
        )
        .filter(
            (F.col("d.era_start_date") <= F.col("e.censor_date")) &
            (F.col("d.era_end_date") >= F.col("e.index_date"))
        )
        .withColumn("era_start_date", F.greatest(F.col("d.era_start_date"), F.col("e.index_date")))
        .withColumn("era_end_date", F.least(F.col("d.era_end_date"), F.col("e.censor_date")))
        .filter(F.col("era_end_date") >= F.col("era_start_date"))
        .select(
            "person_id", "ingredient_concept_id", "era_start_date", "era_end_date",
            "drug_exposure_count", "gap_days"
        )
    )

    person_months = (
        eligible
        .withColumn("month_index", F.explode(F.sequence(F.lit(1), F.lit(followup_months))))
        .withColumn("month_start", F.add_months(F.col("index_date"), F.col("month_index") - 1))
        .withColumn("month_end", F.least(F.date_sub(F.add_months(F.col("index_date"), F.col("month_index")), 1), F.col("censor_date")))
        .filter(F.col("month_start") <= F.col("censor_date"))
        .select("person_id", "month_index", "month_start", "month_end")
    )

    month_overlap = (
        person_months.alias("m")
        .join(
            eras.alias("e"),
            (F.col("m.person_id") == F.col("e.person_id")) &
            (F.col("e.era_start_date") <= F.col("m.month_end")) &
            (F.col("e.era_end_date") >= F.col("m.month_start")),
            "left"
        )
        .select(
            F.col("m.person_id").alias("person_id"),
            "month_index",
            F.col("e.ingredient_concept_id").alias("ingredient_concept_id"),
            F.when(
                (F.col("e.era_start_date") >= F.col("m.month_start")) &
                (F.col("e.era_start_date") <= F.col("m.month_end")), 1
            ).otherwise(0).alias("started_flag"),
            F.when(
                (F.col("e.era_end_date") >= F.col("m.month_start")) &
                (F.col("e.era_end_date") <= F.col("m.month_end")), 1
            ).otherwise(0).alias("stopped_flag")
        )
    )

    person_month_summary = (
        month_overlap
        .groupBy("person_id", "month_index")
        .agg(
            F.countDistinct("ingredient_concept_id").alias("active_n"),
            F.sum("started_flag").alias("starts_n"),
            F.sum("stopped_flag").alias("stops_n"),
            F.collect_set("ingredient_concept_id").alias("active_set")
        )
    )

    person_month_summary = (
        person_months.select("person_id", "month_index")
        .join(person_month_summary, on=["person_id", "month_index"], how="left")
        .withColumn("active_n", F.coalesce(F.col("active_n"), F.lit(0)))
        .withColumn("starts_n", F.coalesce(F.col("starts_n"), F.lit(0)))
        .withColumn("stops_n", F.coalesce(F.col("stops_n"), F.lit(0)))
    )

    w_month = Window.partitionBy("person_id").orderBy("month_index")

    person_month_summary = (
        person_month_summary
        .withColumn("prev_active_n", F.coalesce(F.lag("active_n", 1).over(w_month), F.lit(0)))
        .withColumn("prev_active_set", F.lag("active_set", 1).over(w_month))
        .withColumn("turnover", jaccard_distance(F.col("active_set"), F.col("prev_active_set")))
        .withColumn(
            "state",
            F.when(F.col("active_n") == 0, "NoRx")
             .when((F.col("prev_active_n") == 0) & (F.col("active_n") > 0), "Initiation")
             .when((F.col("active_n") == 1) & (F.col("turnover") < turnover_low) & (F.col("starts_n") == 0) & (F.col("stops_n") == 0), "StableMono")
             .when((F.col("active_n").between(2, polypharmacy_threshold - 1)) & (F.col("turnover") < turnover_low), "StableLowPoly")
             .when((F.col("active_n") >= polypharmacy_threshold) & (F.col("turnover") < turnover_low), "StablePolypharmacy")
             .when((F.col("starts_n") > 0) & (F.col("stops_n") > 0) & (F.col("turnover") >= turnover_high), "HighTurnover")
             .when((F.col("active_n") > F.col("prev_active_n")) & (F.col("starts_n") > 0), "Intensifying")
             .otherwise("Deintensifying")
        )
    )

    maintenance = (
        eras
        .withColumn("era_days", F.datediff("era_end_date", "era_start_date") + 1)
        .groupBy("person_id", "ingredient_concept_id")
        .agg(
            F.count("*").alias("n_eras"),
            F.sum("era_days").alias("total_era_days")
        )
        .withColumn(
            "maintenance_eligible",
            F.when(
                (F.col("n_eras") >= 2) | (F.col("total_era_days") >= maintenance_min_total_days),
                1
            ).otherwise(0)
        )
    )

    w_era = Window.partitionBy("person_id", "ingredient_concept_id").orderBy("era_start_date", "era_end_date")

    eras_for_events = (
        eras
        .join(eligible.select("person_id", "censor_date"), on="person_id", how="left")
        .withColumn("era_days", F.datediff("era_end_date", "era_start_date") + 1)
        .withColumn("era_number", F.row_number().over(w_era))
        .withColumn("next_same_ingredient_start", F.lead("era_start_date", 1).over(w_era))
        .join(maintenance, on=["person_id", "ingredient_concept_id"], how="left")
        .withColumn(
            "restarted_within_180d",
            F.when(
                (F.col("next_same_ingredient_start").isNotNull()) &
                (F.col("next_same_ingredient_start") <= F.date_add(F.col("era_end_date"), restart_window_days)),
                1
            ).otherwise(0)
        )
        .withColumn(
            "observed_for_restart_window",
            F.when(F.date_add(F.col("era_end_date"), restart_window_days) <= F.col("censor_date"), 1).otherwise(0)
        )
        .withColumn(
            "observed_for_switch_window",
            F.when(F.date_add(F.col("era_end_date"), switch_window_days) <= F.col("censor_date"), 1).otherwise(0)
        )
        .withColumn("era_row_id", F.monotonically_increasing_id())
    )

    other_starts = eras.select(
        F.col("person_id").alias("person_id2"),
        F.col("ingredient_concept_id").alias("other_ingredient_concept_id"),
        F.col("era_start_date").alias("other_start_date")
    )

    switch_flags = (
        eras_for_events.alias("a")
        .join(
            other_starts.alias("b"),
            (F.col("a.person_id") == F.col("b.person_id2")) &
            (F.col("a.ingredient_concept_id") != F.col("b.other_ingredient_concept_id")) &
            (F.col("b.other_start_date") > F.col("a.era_end_date")) &
            (F.col("b.other_start_date") <= F.date_add(F.col("a.era_end_date"), switch_window_days)),
            "left"
        )
        .groupBy("a.era_row_id")
        .agg(F.max(F.when(F.col("b.other_start_date").isNotNull(), 1).otherwise(0)).alias("switched_within_60d"))
    )

    era_events = (
        eras_for_events
        .join(switch_flags, on="era_row_id", how="left")
        .fillna({"switched_within_60d": 0, "maintenance_eligible": 0})
        .withColumn(
            "early_discontinuation_90d",
            F.when(
                (F.col("era_number") == 1) &
                (F.col("maintenance_eligible") == 1) &
                (F.col("observed_for_restart_window") == 1) &
                (F.col("era_days") < early_discontinuation_days) &
                (F.col("restarted_within_180d") == 0),
                1
            ).otherwise(0)
        )
    )

    burden = (
        person_month_summary
        .groupBy("person_id")
        .agg(
            F.avg("month_index").alias("mx"),
            F.avg("active_n").alias("my"),
            F.avg(F.col("month_index") * F.col("active_n")).alias("mxy"),
            F.avg(F.col("month_index") * F.col("month_index")).alias("mx2"),
            F.avg("active_n").alias("mean_active_n"),
            F.avg(F.when(F.col("active_n") >= polypharmacy_threshold, 1.0).otherwise(0.0)).alias("poly_month_prop"),
            F.avg("turnover").alias("mean_turnover")
        )
        .withColumn(
            "burden_slope",
            F.when(
                (F.col("mx2") - F.col("mx") * F.col("mx")) != 0,
                (F.col("mxy") - F.col("mx") * F.col("my")) / (F.col("mx2") - F.col("mx") * F.col("mx"))
            ).otherwise(F.lit(0.0))
        )
        .select("person_id", "mean_active_n", "poly_month_prop", "mean_turnover", "burden_slope")
    )

    state_levels = [
        "NoRx", "Initiation", "StableMono", "StableLowPoly",
        "StablePolypharmacy", "Intensifying", "Deintensifying", "HighTurnover"
    ]

    state_counts = (
        person_month_summary
        .groupBy("person_id")
        .pivot("state", state_levels)
        .count()
        .fillna(0)
    )

    n_months = person_month_summary.groupBy("person_id").agg(F.count("*").alias("n_months"))
    state_props = state_counts.join(n_months, on="person_id", how="left")

    for s in state_levels:
        state_props = state_props.withColumn(f"prop_{s}", F.col(s) / F.col("n_months"))

    state_props = state_props.select("person_id", *[f"prop_{s}" for s in state_levels])

    era_counts = (
        eras
        .groupBy("person_id")
        .agg(
            F.count("*").alias("n_ingredient_eras"),
            F.countDistinct("ingredient_concept_id").alias("n_distinct_ingredients")
        )
    )

    disc_summary = (
        era_events
        .filter(F.col("maintenance_eligible") == 1)
        .groupBy("person_id")
        .agg(
            F.count("*").alias("n_maintenance_eras"),
            F.avg(F.col("early_discontinuation_90d").cast("double")).alias("early_disc_90_rate"),
            F.avg(F.when(F.col("observed_for_restart_window") == 1, F.col("restarted_within_180d").cast("double"))).alias("restart_180_rate"),
            F.avg(F.when(F.col("observed_for_switch_window") == 1, F.col("switched_within_60d").cast("double"))).alias("switch_60_rate"),
            F.expr("percentile_approx(era_days, 0.5)").alias("median_era_days")
        )
    )

    features = (
        eligible.select("person_id")
        .join(burden, on="person_id", how="left")
        .join(state_props, on="person_id", how="left")
        .join(era_counts, on="person_id", how="left")
        .join(disc_summary, on="person_id", how="left")
        .fillna(0)
    )

    features = (
        features
        .withColumn(
            "discontinuation_phenotype",
            F.when(
                (F.col("mean_active_n") >= polypharmacy_threshold) &
                (F.col("mean_turnover") < 0.20) &
                (F.col("early_disc_90_rate") < 0.25),
                "Stable polypharmacy"
            )
            .when(
                (F.col("mean_turnover") < 0.20) &
                (F.col("early_disc_90_rate") < 0.25),
                "Persistent stable use"
            )
            .when(
                F.col("restart_180_rate") >= 0.50,
                "Intermittent stop-start"
            )
            .when(
                F.col("switch_60_rate") >= 0.50,
                "High-turnover switching"
            )
            .when(
                (F.col("early_disc_90_rate") >= 0.50) |
                (F.col("burden_slope") < -0.10),
                "Early drop-off / de-intensification"
            )
            .otherwise("Mixed transition pattern")
        )
    )

    feature_cols = [
        "mean_active_n", "poly_month_prop", "mean_turnover", "burden_slope",
        "n_ingredient_eras", "n_distinct_ingredients", "n_maintenance_eras",
        "early_disc_90_rate", "restart_180_rate", "switch_60_rate", "median_era_days",
        "prop_NoRx", "prop_Initiation", "prop_StableMono", "prop_StableLowPoly",
        "prop_StablePolypharmacy", "prop_Intensifying", "prop_Deintensifying", "prop_HighTurnover"
    ]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
    assembled = assembler.transform(features)

    scaler = StandardScaler(inputCol="features_raw", outputCol="features_scaled", withMean=True, withStd=True)
    scaled = scaler.fit(assembled).transform(assembled)

    evaluator = ClusteringEvaluator(featuresCol="features_scaled", predictionCol="trajectory_cluster", metricName="silhouette")

    best_k = None
    best_model = None
    best_score = -1

    for k in k_grid:
        km = KMeans(k=k, seed=seed, featuresCol="features_scaled", predictionCol="trajectory_cluster")
        model = km.fit(scaled)
        pred = model.transform(scaled)
        score = evaluator.evaluate(pred)
        print(f"[{label}] k={k}, silhouette={score:.4f}")
        if score > best_score:
            best_score = score
            best_k = k
            best_model = model

    final_person = (
        best_model.transform(scaled)
        .select("person_id", "trajectory_cluster", "discontinuation_phenotype", *feature_cols)
    )

    cluster_summary = (
        final_person
        .groupBy("trajectory_cluster")
        .agg(
            F.count("*").alias("n_people"),
            F.avg("mean_active_n").alias("mean_active_n"),
            F.avg("poly_month_prop").alias("poly_month_prop"),
            F.avg("mean_turnover").alias("mean_turnover"),
            F.avg("early_disc_90_rate").alias("early_disc_90_rate"),
            F.avg("restart_180_rate").alias("restart_180_rate"),
            F.avg("switch_60_rate").alias("switch_60_rate")
        )
        .orderBy("trajectory_cluster")
    )

    cluster_months = (
        final_person.select("person_id", "trajectory_cluster")
        .join(person_month_summary.select("person_id", "month_index", "active_set"), on="person_id", how="inner")
    )

    cluster_ingredients = (
        cluster_months
        .withColumn("ingredient_concept_id", F.explode_outer("active_set"))
        .filter(F.col("ingredient_concept_id").isNotNull())
        .groupBy("trajectory_cluster", "ingredient_concept_id")
        .agg(F.count("*").alias("cluster_month_count"))
        .join(ingredient_concepts, on="ingredient_concept_id", how="left")
    )

    w_top = Window.partitionBy("trajectory_cluster").orderBy(F.col("cluster_month_count").desc())

    top_ingredients = (
        cluster_ingredients
        .withColumn("rank", F.row_number().over(w_top))
        .filter(F.col("rank") <= top_n)
        .orderBy("trajectory_cluster", "rank")
    )

    prefix = f"{outdir}/{label}"
    eligible.write.mode("overwrite").parquet(f"{prefix}_eligible.parquet")
    eras.write.mode("overwrite").parquet(f"{prefix}_eras.parquet")
    person_month_summary.write.mode("overwrite").parquet(f"{prefix}_person_months.parquet")
    era_events.write.mode("overwrite").parquet(f"{prefix}_era_events.parquet")
    final_person.write.mode("overwrite").parquet(f"{prefix}_person_level_phenotypes.parquet")
    cluster_summary.write.mode("overwrite").parquet(f"{prefix}_cluster_summary.parquet")
    top_ingredients.write.mode("overwrite").parquet(f"{prefix}_top_ingredients.parquet")

    print(f"[{label}] selected k = {best_k}, silhouette = {best_score:.4f}")

    return {
        "eligible": eligible,
        "eras": eras,
        "person_month_summary": person_month_summary,
        "era_events": era_events,
        "final_person": final_person,
        "cluster_summary": cluster_summary,
        "top_ingredients": top_ingredients
    }
