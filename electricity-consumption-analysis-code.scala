```scala
// Databricks notebook source

// DBTITLE 1,Libraries
// Importing the required libraries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// Initializing Spark session
val spark = SparkSession.builder.appName("PracticalCase").getOrCreate()

// DBTITLE 1,Reading Files
// Reading the SIPS CSV file
val sipsDF = spark.read
  .option("header", "true")
  .csv("/FileStore/PracticalCaseFiles/CNMC/201712_SIPS2_PS_ELECTRICIDAD_nacional.csv")

// Reading the SIPSCO CSV file
val sipscoDF = spark.read
  .option("header", "true")
  .csv("/FileStore/PracticalCaseFiles/CNMC/201712_SIPS2_CONSUMOS_ELECTRICIDAD_nacional.csv")

// Reading the contracts JSON file
val contractsDF = spark.read
  .json("/FileStore/PracticalCaseFiles/Contracts/contracts.json")

// // Displaying DFs
// sipsDF.show(5)
// sipscoDF.show(5)
// contractsDF.show(5)

// // Displaying DFs
// sipsDF.show(2, 100, true)
// sipscoDF.show(2, 100, true)
// contractsDF.show(5, 100, true)

// DBTITLE 1,Saving to Bronzer Layer
// Saving DFs to the Bronze layer
sipsDF.write
  .mode("overwrite")
  .parquet("/user/luna/raw/201712_SIPS2_PS_ELECTRICIDAD_nacional")

sipscoDF.write
  .mode("overwrite")
  .parquet("/user/luna/raw/201712_SIPS2_CONSUMOS_ELECTRICIDAD_nacional")

contractsDF.write
  .mode("overwrite")
  .parquet("/user/luna/raw/contracts")

// DBTITLE 1,Exporting from Bronzer Layer
// Reading the Parquet files from the Bronze layer
val sipsDF = spark.read.parquet("/user/luna/raw/201712_SIPS2_PS_ELECTRICIDAD_nacional")
val sipscoDF = spark.read.parquet("/user/luna/raw/201712_SIPS2_CONSUMOS_ELECTRICIDAD_nacional")
val contractsDF = spark.read.parquet("/user/luna/raw/contracts")

// DBTITLE 1,Renaming Columns
// Renaming columns for SIPS DF
val renamedSipsDF = sipsDF
  .withColumnRenamed("cups", "cups")
  .withColumnRenamed("nombreEmpresaDistribuidora", "distribuidora")
  .withColumnRenamed("codigoEmpresaDistribuidora", "coddist")
  .withColumnRenamed("fechaAltaSuministro", "falta")
  .withColumnRenamed("codigoTarifaATREnVigor", "tarifa_sips")
  .withColumnRenamed("codigoTensionV", "tensionps")
  .withColumnRenamed("codigoFasesEquipoMedida", "fases")  
  .withColumnRenamed("potenciaMaximaBIEW", "potmaxbie")
  .withColumnRenamed("valorDerechosExtensionW", "derext")
  .withColumnRenamed("valorDerechosAccesoW", "deraccll")
  .withColumnRenamed("tipoPerfilConsumo", "perfilree")
  .withColumnRenamed("potenciasContratadasEnWP1", "potcontrp1")
  .withColumnRenamed("potenciasContratadasEnWP2", "potcontrp2")
  .withColumnRenamed("potenciasContratadasEnWP3", "potcontrp3")
  .withColumnRenamed("potenciasContratadasEnWP4", "potcontrp4")
  .withColumnRenamed("potenciasContratadasEnWP5", "potcontrp5")
  .withColumnRenamed("potenciasContratadasEnWP6", "potcontrp6")
  .withColumnRenamed("codigoAutoconsumo", "autoconsumo")

// Renaming columns for SIPSCO DF
val renamedSipscoDF = sipscoDF
  .withColumnRenamed("cups", "cups")
  .withColumnRenamed("fechainicioMesConsumo", "finicio")
  .withColumnRenamed("fechaFinMesConsumo", "ffin")
  .withColumnRenamed("codigoTarifaATR", "tarifa_sipsco")
  .withColumnRenamed("consumoEnergiaActivaEnWhP1", "consumoactivap1")
  .withColumnRenamed("consumoEnergiaActivaEnWhP2", "consumoactivap2")
  .withColumnRenamed("consumoEnergiaActivaEnWhP3", "consumoactivap3")
  .withColumnRenamed("consumoEnergiaActivaEnWhP4", "consumoactivap4")
  .withColumnRenamed("consumoEnergiaActivaEnWhP5", "consumoactivap5")
  .withColumnRenamed("consumoEnergiaActivaEnWhP6", "consumoactivap6")
  .withColumnRenamed("consumoEnergiaReactivaEnVArhP1", "consumoreactivap1")
  .withColumnRenamed("consumoEnergiaReactivaEnVArhP2", "consumoreactivap2")
  .withColumnRenamed("consumoEnergiaReactivaEnVArhP3", "consumoreactivap3")
  .withColumnRenamed("consumoEnergiaReactivaEnVArhP4", "consumoreactivap4")
  .withColumnRenamed("consumoEnergiaReactivaEnVArhP5", "consumoreactivap5")
  .withColumnRenamed("consumoEnergiaReactivaEnVArhP6", "consumoreactivap6")
  .withColumnRenamed("potenciaDemandadaEnWP1", "potmaxp1")
  .withColumnRenamed("potenciaDemandadaEnWP2", "potmaxp2")
  .withColumnRenamed("potenciaDemandadaEnWP3", "potmaxp3")
  .withColumnRenamed("potenciaDemandadaEnWP4", "potmaxp4")
  .withColumnRenamed("potenciaDemandadaEnWP5", "potmaxp5")
  .withColumnRenamed("potenciaDemandadaEnWP6", "potmaxp6")
  .withColumn("numdias", datediff(col("ffin"), col("finicio"))) // Calculating numdias
  .withColumnRenamed("codigoTipoLectura", "cmd")

// Displaying DFs
renamedSipsDF.show(2, 100, true)
renamedSipscoDF.show(2, 100, true)

// DBTITLE 1,Cleaning Data
// Cleaning and processing the SIPS data
val cleanedSipsDF = renamedSipsDF
  .withColumn("falta", to_date(col("falta"), "yyyyMMdd")) // Converting date format
  .withColumn("tensionps", col("tensionps").cast("int")) // Casting to int
  .withColumn("fases", col("fases").cast("int")) 
  .withColumn("derext", col("derext").cast("double")) // Casting to double
  .withColumn("deraccll", col("deraccll").cast("double")) 
  .withColumn("potcontrp1", col("potcontrp1").cast("double")) 
  .withColumn("potcontrp2", col("potcontrp2").cast("double"))
  .withColumn("potcontrp3", col("potcontrp3").cast("double"))
  .withColumn("potcontrp4", col("potcontrp4").cast("double"))
  .withColumn("potcontrp5", col("potcontrp5").cast("double"))
  .withColumn("potcontrp6", col("potcontrp6").cast("double"))
  .withColumn("solicitudcorte", lit(false).cast("boolean")) // Normalizing to boolean and setting "false" as default
  .withColumn("autoconsumo", when(col("autoconsumo") === "YES", "true").otherwise("false").cast("boolean")) // Normalizing to boolean
  .na.fill("Unknown", Seq("distribuidora", "tarifa_sips", "perfilree")) // Handling missing values
  .na.fill(0.0, Seq("derext", "deraccll", "potcontrp1", "potcontrp2", "potcontrp3", "potcontrp4", "potcontrp5", "potcontrp6")) // Filling missing numeric values with 0

// Cleaning and processing the SIPSCO data
val cleanedSipscoDF = renamedSipscoDF
  .withColumn("finicio", to_date(col("finicio"), "yyyyMMdd")) // Converting date format
  .withColumn("ffin", to_date(col("ffin"), "yyyyMMdd")) // Converting date format
  .withColumn("consumoactivap1", col("consumoactivap1").cast("float")) // Casting to float
  .withColumn("consumoactivap2", col("consumoactivap2").cast("float"))
  .withColumn("consumoactivap3", col("consumoactivap3").cast("float"))
  .withColumn("consumoactivap4", col("consumoactivap4").cast("float"))
  .withColumn("consumoactivap5", col("consumoactivap5").cast("float"))
  .withColumn("consumoactivap6", col("consumoactivap6").cast("float"))
  .withColumn("consumoreactivap1", col("consumoreactivap1").cast("float"))
  .withColumn("consumoreactivap2", col("consumoreactivap2").cast("float"))
  .withColumn("consumoreactivap3", col("consumoreactivap3").cast("float"))
  .withColumn("consumoreactivap4", col("consumoreactivap4").cast("float"))
  .withColumn("consumoreactivap5", col("consumoreactivap5").cast("float"))
  .withColumn("consumoreactivap6", col("consumoreactivap6").cast("float"))
  .withColumn("potmaxp1", col("potmaxp1").cast("float"))
  .withColumn("potmaxp2", col("potmaxp2").cast("float"))
  .withColumn("potmaxp3", col("potmaxp3").cast("float"))
  .withColumn("potmaxp4", col("potmaxp4").cast("float"))
  .withColumn("potmaxp5", col("potmaxp5").cast("float"))
  .withColumn("potmaxp6", col("potmaxp6").cast("float"))
  .withColumn("numdias", datediff(col("ffin"), col("finicio"))) // Calculating numdias
  .na.fill("Unknown", Seq("tarifa_sipsco")) // Handling missing values
  .na.fill(0.0, Seq("consumoactivap1", "consumoactivap2", "consumoactivap3", "consumoactivap4", "consumoactivap5", "consumoactivap6", "consumoreactivap1", "consumoreactivap2", "consumoreactivap3", "consumoreactivap4", "consumoreactivap5", "consumoreactivap6", "potmaxp1", "potmaxp2", "potmaxp3", "potmaxp4", "potmaxp5", "potmaxp6")) // Filling missing numeric values with 0

// DBTITLE 1,Saving to Silver Layer
// Saving the transformed SIPSCO DF as an intermediate result
cleanedSipscoDF.write.mode("overwrite").parquet("/user/luna/intermediate/cleanedSipsco")

// Reading the intermediate SIPSCO DF back
val intermediateSipscoDF = spark.read.parquet("/user/luna/intermediate/cleanedSipsco")

// Joining the SIPS DF with the SIPSCO DF to get the missing fields
val finalSipsDF = cleanedSipsDF.join(intermediateSipscoDF.select("cups", "potmaxp1", "potmaxp2", "potmaxp3", "potmaxp4", "potmaxp5", "potmaxp6"), Seq("cups"), "left_outer")

// Saving the final SIPS DF to the silver layer
finalSipsDF.write.mode("overwrite").parquet("/user/luna/refined/201712_SIPS2_PS_ELECTRICIDAD_nacional")

// Saving the cleaned and transformed SIPSCO DF to the silver layer
intermediateSipscoDF.write.mode("overwrite").parquet("/user/luna/refined/201712_SIPS2_CONSUMOS_ELECTRICIDAD_nacional")

// DBTITLE 1,Exporting from Silver Layer
// Reading the transformed SIPS and SIPSCO DFs from the silver layer
val finalSipsDF = spark.read.parquet("/user/luna/refined/201712_SIPS2_PS_ELECTRICIDAD_nacional")
// val intermediateSipscoDF = spark.read.parquet("/user/luna/refined/201712_SIPS2_CONSUMOS_ELECTRICIDAD_nacional")

// Extracting unique distributors
val distributors = finalSipsDF.select("distribuidora").distinct().collect().map(_.getString(0))

// Function to create lookup tables for each distributor
def createLookupTables(distributor: String): Unit = {
  // Filtering data for the specific distributor
  val sipsDF = finalSipsDF.filter(col("distribuidora") === distributor)
  // val sipscoDF = intermediateSipscoDF.filter(col("distribuidora") === distributor)

  // Saving the filtered data as Parquet files
  sipsDF.write.mode("overwrite").parquet(s"/user/luna/refined/lookup/${distributor}_sips")
  // sipscoDF.write.mode("overwrite").parquet(s"/user/luna/refined/lookup/${distributor}_sipsco")
}

// Creating lookup tables for each distributor
distributors.foreach(createLookupTables)

// DBTITLE 1,Schemas
// Reading the transformed SIPS and SIPSCO DF from the silver layer
val finalSipsDF = spark.read.parquet("/user/luna/refined/201712_SIPS2_PS_ELECTRICIDAD_nacional")
val intermediateSipscoDF = spark.read.parquet("/user/luna/refined/201712_SIPS2_CONSUMOS_ELECTRICIDAD_nacional")

// Defining the schema for the SIPS table
val sipsSchema = finalSipsDF
  .select(
    col("cups").cast("string"),
    col("distribuidora").cast("string"),
    col("coddist").cast("string"),
    col("falta").cast("date"),
    col("tarifa_sips").cast("string"),
    col("tensionps").cast("int"),
    col("fases").cast("int"),
    col("potmaxbie").cast("string"),
    col("derext").cast("double"),
    col("deraccll").cast("double"),
    col("perfilree").cast("string"),
    col("potcontrp1").cast("double"),
    col("potcontrp2").cast("double"),
    col("potcontrp3").cast("double"),
    col("potcontrp4").cast("double"),
    col("potcontrp5").cast("double"),
    col("potcontrp6").cast("double"),
    col("potmaxp1").cast("double"),
    col("potmaxp2").cast("double"),
    col("potmaxp3").cast("double"),
    col("potmaxp4").cast("double"),
    col("potmaxp5").cast("double"),
    col("potmaxp6").cast("double"),
    col("solicitudcorte").cast("boolean"),
    col("autoconsumo").cast("boolean")
  )

// Defining the schema for the SIPSCO table
val sipscoSchema = intermediateSipscoDF
  .select(
    col("cups").cast("string"),
    col("finicio").cast("date"),
    col("ffin").cast("date"),
    col("tarifa_sipsco").cast("string"),
    col("consumoactivap1").cast("float"),
    col("consumoactivap2").cast("float"),
    col("consumoactivap3").cast("float"),
    col("consumoactivap4").cast("float"),
    col("consumoactivap5").cast("float"),
    col("consumoactivap6").cast("float"),
    col("consumoreactivap1").cast("float"),
    col("consumoreactivap2").cast("float"),
    col("consumoreactivap3").cast("float"),
    col("consumoreactivap4").cast("float"),
    col("consumoreactivap5").cast("float"),
    col("consumoreactivap6").cast("float"),
    col("potmaxp1").cast("float"),
    col("potmaxp2").cast("float"),
    col("potmaxp3").cast("float"),
    col("potmaxp4").cast("float"),
    col("potmaxp5").cast("float"),
    col("potmaxp6").cast("float"),
    col("numdias").cast("int"),
    col("cmd").cast("float")
  )

println("SIPS Table Schema:")
sipsSchema.printSchema()
println("SIPS Table Data:")
sipsSchema.show(2, 100, true)

println("SIPSCO Table Schema:")
sipscoSchema.printSchema()
println("SIPSCO Table Data:")
sipscoSchema.show(2, 100, true)

// DBTITLE 1,Saving to Gold Layer
// Saving the final SIPS DF to the gold layer
sipsSchema.write.mode("overwrite").parquet("/user/luna/trusted/SIPS")

// Saving the cleaned and transformed SIPSCO DF to the gold layer
sipscoSchema.write.mode("overwrite").parquet("/user/luna/trusted/SIPSCO")

// Reading back the saved data to verify
val finalSipsGoldDF = spark.read.parquet("/user/luna/trusted/SIPS")
val finalSipscoGoldDF = spark.read.parquet("/user/luna/trusted/SIPSCO")

// Printing rows of each table to verify
println("SIPS Table:")
finalSipsGoldDF.show(2, 100, true)
println("SIPSCO Table:")
finalSipscoGoldDF.show(2, 100, true)

// DBTITLE 1,Joining SIPS with SIPSCO
// Identifying the latest consumption entry for each CUPS
val sipscoLatestDF = finalSipscoGoldDF
  .withColumn("row_num", row_number().over(Window.partitionBy("cups").orderBy(desc("ffin")))) // Creating a row number for each entry within each 'cups'
  .filter(col("row_num") === 1) // Filtering to keep only the latest entry (row number 1) for each 'cups'
  .drop("row_num") // Column no longer needed

// Joining SIPS data with the latest SIPSCO data
val sipsWithSipscoDF = finalSipsGoldDF.join(sipscoLatestDF, Seq("cups", "potmaxp1", "potmaxp2", "potmaxp3", "potmaxp4", "potmaxp5", "potmaxp6"))

// Printing the schema and rows of the joined DF
println("Joined SIPS and SIPSCO Table Schema:")
sipsWithSipscoDF.printSchema()
println("Joined SIPS and SIPSCO Table Data:")
sipsWithSipscoDF.show(2, 100, true)

// DBTITLE 1,Flattening Contract Data
// Flattening the contract data
val contractsFlattenDF = contractsDF
  .select(
    col("contract_number").as("numcontrato"),
    col("cups"),
    col("contract_date").as("fcontrato"),
    col("product.code").as("codproducto"),
    col("personal_data.email"),
    col("personal_data.address.zip_code").as("codpostal")
  )

// Joining with contract data 
val currentConsumptionDF = sipsWithSipscoDF.join(contractsFlattenDF, Seq("cups"))

// DBTITLE 1,Reordering Columns
// Selecting and reordering columns according to the specified schema
val currentConsumptionFinalDF = currentConsumptionDF
  .select(
    col("cups").cast("string"),
    col("numcontrato").cast("string"),
    col("fcontrato").cast("string"),
    col("codproducto").cast("string"),
    col("email").cast("string"),
    col("codpostal").cast("string"),
    col("distribuidora").cast("string"),
    col("coddist").cast("string"),
    col("falta").cast("date"),
    col("tarifa_sips").cast("string"),
    col("tensionps").cast("int"),
    col("fases").cast("int"),
    col("potmaxbie").cast("string"),
    col("perfilree").cast("string"),
    col("potcontrp1").cast("double"),
    col("potcontrp2").cast("double"),
    col("potcontrp3").cast("double"),
    col("potcontrp4").cast("double"),
    col("potcontrp5").cast("double"),
    col("potcontrp6").cast("double"),
    col("potmaxp1").cast("double"),
    col("potmaxp2").cast("double"),
    col("potmaxp3").cast("double"),
    col("potmaxp4").cast("double"),
    col("potmaxp5").cast("double"),
    col("potmaxp6").cast("double"),
    col("finicio").cast("date"),
    col("ffin").cast("date"),
    col("tarifa_sipsco").cast("string"),
    col("consumoactivap1").cast("float"),
    col("consumoactivap2").cast("float"),
    col("consumoactivap3").cast("float"),
    col("consumoactivap4").cast("float"),
    col("consumoactivap5").cast("float"),
    col("consumoactivap6").cast("float"),
    col("consumoreactivap1").cast("float"),
    col("consumoreactivap2").cast("float"),
    col("consumoreactivap3").cast("float"),
    col("consumoreactivap4").cast("float"),
    col("consumoreactivap5").cast("float"),
    col("consumoreactivap6").cast("float"),
    col("numdias").cast("int"),
    col("cmd").cast("float")
  )

// Printing the schema and first rows
println("CURRENT_CONSUMPTION Table Schema:")
currentConsumptionFinalDF.printSchema()
println("CURRENT_CONSUMPTION Table Data:")
currentConsumptionFinalDF.show(2, 100, true)

// DBTITLE 1,Saving FinalDF to Gold Layer
// Saving the final CURRENT_CONSUMPTION DF to the gold layer
currentConsumptionFinalDF.write.mode("overwrite").parquet("/user/luna/trusted/CURRENT_CONSUMPTION")

// Reading back the saved data 
val finalCurrentConsumptionGoldDF = spark.read.parquet("/user/luna/trusted/CURRENT_CONSUMPTION")

// Printing rows of the table to verify
println("CURRENT_CONSUMPTION Table:")
finalCurrentConsumptionGoldDF.show(2, 100, true)
