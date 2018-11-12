package com.sparkProject

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}


object Trainer {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAll(Map(
      "spark.scheduler.mode" -> "FIFO",
      "spark.speculation" -> "false",
      "spark.reducer.maxSizeInFlight" -> "48m",
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.kryoserializer.buffer.max" -> "1g",
      "spark.shuffle.file.buffer" -> "32k",
      "spark.default.parallelism" -> "12",
      "spark.sql.shuffle.partitions" -> "12",
      "spark.driver.maxResultSize" -> "2g"
    ))

    val spark = SparkSession
      .builder
      .config(conf)
      .appName("TP_spark")
      .getOrCreate()


    /*******************************************************************************
      *
      *       TP 3
      *
      *       - lire le fichier sauvegarder précédemment
      *       - construire les Stages du pipeline, puis les assembler
      *       - trouver les meilleurs hyperparamètres pour l'entraînement du pipeline avec une grid-search
      *       - Sauvegarder le pipeline entraîné
      *
      *       if problems with unimported modules => sbt plugins update
      *
      ********************************************************************************/

    /** Question 1 - Charger le dataframe **/
    /** ********************************* **/

    val df = spark.read.parquet("/Users/osans/Documents/Telecom/Cours/INF729_Hadoop_Spark/Spark/TP/kickstarter/prepared_trainingset")
    df.printSchema()

    /** Question 2 - Utiliser les données textuelles **/
    /** ******************************************** **/

    /** a) Stage 1 : Séparer les textes en mots (ou tokens) avec un tokenizer **/

    val tokenizer = new RegexTokenizer()
      .setPattern("\\W+")
      .setGaps(true)
      .setInputCol("text")
      .setOutputCol("tokens")

    /** b) Stage 2 : Retirer les stop words pour ne pas encombrer le modèle. **/

    val remover = new StopWordsRemover()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("filtered")

    /** c) Stage 3 : Partie TF avec CountVectorizer **/

    val cv_model = new CountVectorizer()
      .setInputCol(remover.getOutputCol)
      .setOutputCol("words_count")

    /** d) Stage 4 : Trouvez la partie IDF **/

    val idf = new IDF()
      .setInputCol("words_count")
      .setOutputCol("tfidf")

    /** Question 3 - Convertir les catégories en données numériques **/
    /** *********************************************************** **/

    /** e) Stage 5 : Convertir la variable catégorielle “country2” en quantités numériques **/

    val country_indexer = new StringIndexer()
      .setInputCol("country2")
      .setOutputCol("country_indexed")
      .setHandleInvalid("skip")

    /** f) Stage 6 : Convertir la variable catégorielle “currency2” en quantités numériques **/

    val currency_indexer = new StringIndexer()
      .setInputCol("currency2")
      .setOutputCol("currency_indexed")
      .setHandleInvalid("skip")

    /** g) Stage 7 : Transformer country_indexed avec un “one-hot encoder” **/

    val country_encoder = new OneHotEncoder()
      .setInputCol("country_indexed")
      .setOutputCol("country_onehot")

    /** g) Stage 8 : Transformer currency_indexed avec un “one-hot encoder” **/

    val currency_encoder = new OneHotEncoder()
      .setInputCol("currency_indexed")
      .setOutputCol("currency_onehot")

    /** Question 4 : Mettre les données sous une forme utilisable par Spark.ML. **/
    /** *********************************************************************** **/

    /** h) Stage 9 : Assembler les features "tfidf", "days_campaign", "hours_prepa",
     "goal", "country_onehot", "currency_onehot"  dans une seule colonne “features” **/

    val assembler = new VectorAssembler()
      .setInputCols(Array("tfidf", "days_campaign", "hours_prepa", "goal", "country_onehot", "currency_onehot"))
      .setOutputCol("features")

    /** i) Stage 10 : Le modèle de classification : régression logistique **/

    val lr = new LogisticRegression()
      .setElasticNetParam(0.0)
      .setFitIntercept(true)
      .setFeaturesCol("features")
      .setLabelCol("final_status")
      .setStandardization(true)
      .setPredictionCol("predictions")
      .setRawPredictionCol("raw_predictions")
      .setThresholds(Array(0.7, 0.3))
      .setTol(1.0e-6)
      .setMaxIter(300)

    /** j) Créer le pipeline en assemblant les 10 stages **/

    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, remover, cv_model, idf, country_indexer, currency_indexer, country_encoder, currency_encoder, assembler, lr))


    /** Question 5 - Entrainement et tuning du modèle **/
    /** ********************************************* **/

    /** k) Créer un dataFrame nommé “training” 90% et un autre nommé “test” 10% à partir du dataFrame. **/

    val Array(training, test) = df.randomSplit(Array(0.9, 0.1), seed = 1)

    /** l) Préparer la grid-search pour satisfaire les conditions explicitées
           puis lancer la grid-search sur le dataset “training” **/

    val grid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(10e-8, 10e-6, 10e-4, 10e-2))
      .addGrid(cv_model.minDF, Array(55.0, 75.0, 95.0))
      .build()

    val f1 = new MulticlassClassificationEvaluator()
      .setMetricName("f1")
      .setLabelCol("final_status")
      .setPredictionCol("predictions")

    val model_tuning = new TrainValidationSplit()
      .setEstimator(pipeline)
      .setEvaluator(f1)
      .setEstimatorParamMaps(grid)
      .setTrainRatio(0.7)

    val model_tune = model_tuning.fit(training)

    /** m) Appliquer le meilleur modèle trouvé avec la grid-search aux données test. **/

    val df_WithPredictions = model_tune.transform(test)
    val f1_score = f1.evaluate(df_WithPredictions)

    /** n) Afficher df_WithPredictions.groupBy("final_status", "predictions").count.show() **/

    df_WithPredictions.groupBy("final_status", "predictions").count.show()
    println(s"Le f1-score est de : $f1_score")
  }
}