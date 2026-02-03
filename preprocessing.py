# Note - 1 [ creating dedicated spark session is Not required in the notebook , but to run job in dataproc, this is needed ]

from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .appName("HealthcareDataProcessing") \
                    .getOrCreate()


#Importing important functions

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
from pyspark.sql.functions import *
import google.cloud.logging
import logging

# Initialize Google Cloud Logging
logging_client = google.cloud.logging.Client()
logging_client.setup_logging()
logger = logging.getLogger('healthcare-data-pipeline')

def log_pipeline_step(step, message, level='INFO'):
    
    if level == 'INFO':
        logger.info(f"Step: {step}, Message: {message}")
        print(f"[INFO] {step}: {message}")
    elif level == 'ERROR':
        logger.error(f"Step: {step}, Error: {message}")
        print(f"[ERROR] {step}: {message}")
    elif level == 'WARNING':
        logger.warning(f"Step: {step}, Warning: {message}")
        print(f" [WARNING] {step}: {message}")

# Defining important variables

gcs_source_file=''
gcs_invalid_path=''
BQ_table_id = ""
gcs_temp_bucket=''



# ========== CODE CORRIGÉ ==========

def validate_data(df):    
    log_pipeline_step("Data Validation", "Starting data validation.")
    
    # CORRECTION: Convertir les types avant validation
    df = df.withColumn("heart_rate", col("heart_rate").cast("integer"))
    df = df.withColumn("blood_pressure", col("blood_pressure").cast("integer"))
    df = df.withColumn("temperature", col("temperature").cast("double"))
    
    df1 = df.withColumn("is_valid", 
                       when((col("heart_rate") > 40) & (col("heart_rate") < 200) & 
                            (col("blood_pressure") > 50) & (col("blood_pressure") < 200) & 
                            (col("temperature") > 35.0) & (col("temperature") < 42.0), 
                            True).otherwise(False))
    
    valid_records = df1.filter(col("is_valid") == True)
    invalid_records = df1.filter(col("is_valid") == False)
    
    log_pipeline_step("Data Validation", f"Valid records: {valid_records.count()}, Invalid records: {invalid_records.count()}")
    
    # DEBUG: Afficher quelques données
    if valid_records.count() > 0:
        log_pipeline_step("DEBUG", "Sample valid data:")
        valid_records.select("patient_id", "heart_rate", "blood_pressure", "temperature").show(3)
    
    return valid_records, invalid_records

def process_data():
    
    try:
        log_pipeline_step("Pipeline Start", "Healthcare data processing pipeline initiated.")
        
        # ========== ÉTAPE 1: LECTURE ==========
        log_pipeline_step("Data Ingestion", f"Reading from: {gcs_source_file}")
        
        # Lire SANS schéma d'abord pour être sûr
        df_raw = spark.read \
            .format("json") \
            .option("multiLine", "true")  \
            .load(gcs_source_file)
        
        log_pipeline_step("Data Ingestion", f"Raw data count: {df_raw.count()}")
        
        if df_raw.count() == 0:
            log_pipeline_step("Data Ingestion", "ERROR: No data found!", level='ERROR')
            return
        
        # DEBUG: Afficher ce qu'on a lu
        log_pipeline_step("DEBUG", "Raw schema:")
        df_raw.printSchema()
        
        log_pipeline_step("DEBUG", "Raw data sample:")
        df_raw.show(3, truncate=False)
        
        # ========== ÉTAPE 2: TRANSFORMATION ==========
        log_pipeline_step("Data Transformation", "Processing data structure...")
        
        # Vérifier si on a la colonne 'patients'
        if 'patients' not in df_raw.columns:
            log_pipeline_step("Data Transformation", "ERROR: 'patients' column not found!", level='ERROR')
            log_pipeline_step("DEBUG", f"Available columns: {df_raw.columns}")
            return
        
        # Éclater le tableau 'patients'
        df_exploded = df_raw.select(explode("patients").alias("patient"))
        log_pipeline_step("Data Transformation", f"After explode: {df_exploded.count()} records")
        
        # Extraire les colonnes
        df_flat = df_exploded.select(
            col("patient.patient_id").alias("patient_id"),
            col("patient.heart_rate").alias("heart_rate"),
            col("patient.blood_pressure").alias("blood_pressure"),
            col("patient.temperature").alias("temperature"),
            col("patient.timestamp").alias("timestamp")
        )
        
        log_pipeline_step("Data Transformation", "Flattened data sample:")
        df_flat.show(5, truncate=False)
        
        # ========== ÉTAPE 3: VALIDATION ==========
        log_pipeline_step("Data Validation", "Validating data...")
        valid_df, invalid_df = validate_data(df_flat)
        
        # ========== ÉTAPE 4: SAUVEGARDE INVALIDES ==========
        if invalid_df.count() > 0:
            log_pipeline_step("Invalid Data", f"Saving {invalid_df.count()} invalid records", level='WARNING')
            
            # Sélectionner seulement les colonnes nécessaires
            invalid_to_save = invalid_df.select(
                "patient_id", "heart_rate", "blood_pressure", 
                "temperature", "timestamp", "is_valid"
            )
            
            invalid_to_save.write \
                .mode("overwrite") \
                .format("json") \
                .save(gcs_invalid_path)
        
        # ========== ÉTAPE 5: TRANSFORMATION ==========
        if valid_df.count() == 0:
            log_pipeline_step("Data Transformation", "WARNING: No valid data to process!", level='WARNING')
            return
        
        log_pipeline_step("Data Aggregation", f"Aggregating {valid_df.count()} valid records")
        
        # Agréger les moyennes
        df_agg = valid_df.groupBy("patient_id").agg(
            round(avg("heart_rate"), 2).alias("avg_heart_rate"),
            round(avg("blood_pressure"), 2).alias("avg_blood_pressure"),
            round(avg("temperature"), 2).alias("avg_temperature")
        )
        
        log_pipeline_step("DEBUG", "Aggregated data (averages):")
        df_agg.show(5)
        
        # Calculer les écarts-types
        df_stddev = valid_df.groupBy("patient_id").agg(
            round(stddev("heart_rate"), 2).alias("stddev_heart_rate"),
            round(stddev("blood_pressure"), 2).alias("stddev_blood_pressure"),
            round(stddev("temperature"), 2).alias("stddev_temperature")
        )
        
        log_pipeline_step("DEBUG", "Standard deviations:")
        df_stddev.show(5)
        
        # Joindre
        df_joined = df_agg.join(df_stddev, on="patient_id", how="left")
        
        # Catégoriser les risques
        df_joined = df_joined.withColumn("risk_category", 
            when(col("avg_heart_rate") > 100, "High Risk")
            .when(col("stddev_heart_rate") > 15, "Moderate Risk")
            .otherwise("Low Risk")
        )
        
        log_pipeline_step("Data Transformation", "Final results:")
        df_joined.show()
        
        # ========== ÉTAPE 6: BIGQUERY ==========
        log_pipeline_step("Data Write", f"Writing to BigQuery: {BQ_table_id}")
        
        df_joined.write \
            .format("bigquery") \
            .option("table", BQ_table_id) \
            .option("temporaryGcsBucket", gcs_temp_bucket) \
            .mode("overwrite") \
            .save()
        
        log_pipeline_step("Data Write", "Successfully written to BigQuery!")
        
        # ========== RAPPORT FINAL ==========
        print("\n" + "=" * 60)
        print("📊 RAPPORT FINAL")
        print("=" * 60)
        print(f"Total patients: {df_flat.count()}")
        print(f"Valid patients: {valid_df.count()}")
        print(f"Invalid patients: {invalid_df.count()}")
        print(f"Analyzed patients: {df_joined.count()}")
        
        if df_joined.count() > 0:
            print("\nRisk distribution:")
            for row in df_joined.groupBy("risk_category").count().collect():
                print(f"  {row['risk_category']}: {row['count']}")
        
        print("=" * 60)
        
    except Exception as e:
        log_pipeline_step("Pipeline Error", f"Error: {str(e)}", level='ERROR')
        import traceback
        traceback.print_exc()

# ========== EXÉCUTION ==========
if __name__ == "__main__":
    # Version SIMPLE pour tester d'abord
    print("=" * 60)
    print("🧪 TEST SIMPLE DES DONNÉES")
    print("=" * 60)
    
    # Test 1: Vérifier la lecture
    print("\n1. Testing file reading...")
    try:
        df_test = spark.read.json(gcs_source_file)
        print(f"✅ Success! Found {df_test.count()} records")
        print("Schema:")
        df_test.printSchema()
        
        if df_test.count() > 0:
            print("First row:")
            df_test.show(1, truncate=False, vertical=True)
            
            # Vérifier la structure
            if 'patients' in df_test.columns:
                print("\n✅ Structure OK: 'patients' column found")
                
                # Éclater pour voir
                df_exploded_test = df_test.select(explode("patients").alias("p"))
                print(f"Patients count: {df_exploded_test.count()}")
                df_exploded_test.select("p.*").show(3)
            else:
                print("\n❌ Problem: 'patients' column NOT found")
                print(f"Columns found: {df_test.columns}")
                
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print("\n" + "=" * 60)
    print("🚀 LANCEMENT DU PIPELINE COMPLET")
    print("=" * 60)
    
    # Lancer le pipeline complet
    process_data()
    
    print("\n✅ Pipeline completed!")
    
    # Arrêter Spark
    spark.stop()