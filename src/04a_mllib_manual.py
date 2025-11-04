from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import DataFrame
from pyspark.ml.feature import StringIndexerModel # Model tipini belirtmek için

def preprocess(df: DataFrame, indexer_model: StringIndexerModel, assembler: VectorAssembler) -> DataFrame:
    """
    Eğitilmiş bir 'indexer_model' ve bir 'assembler' kullanarak
    ham DataFrame'i (eğitim veya test) işleyen yardımcı fonksiyon.
    Bu, 'Pipeline'ın 'transform' adımının manuel simülasyonudur.
    """
    # Adım 1: String etiketleri, öğrenilmiş kurallarla sayısal 'label'a çevir
    df_indexed = indexer_model.transform(df)

    # Adım 2: Özellik sütunlarını tek bir 'features' vektöründe birleştir
    df_assembled = assembler.transform(df_indexed)

    return df_assembled

def main():
    """
    Spark MLlib (pyspark.ml) kullanarak basit bir makine öğrenimi sınıflandırma modeli demosu.
    Amaç: Spark'ın makine öğrenimi iş akışının temel adımlarını göstermektir:
     - veri yükleme
     - training ve test verisini bölme
     - özellik mühendisliği (feature engineering)
     - model eğitimi (training)
     - değerlendirme (evaluation)
    """

    # 0. VERİ SETİ SEÇİMİ ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    ### ------- Ders geçme veri seti -------
    ### Demo'nun Amacı       : "Öğrencinin devamsızlık, çalışma saati ve vize notuna bakarak dersten
    ###                         geçip geçmeyeceğini tahmin edebilir miyiz?" sorusunu cevaplamaktır.
    #file_path = "/spark-demo/data/mllib/dersler.csv"
    #feature_columns = ['devamsizlik', 'calisma_saati', 'vize_notu']
    #label_column="sonuc"
    ### ------- Ders geçme veri seti -------

    ### ------- Iris veri seti -------
    ### Demo'nun Amacı       : "Bir çiçeğin sadece Taç Yaprak ve Çanak Yaprak boyutlarına (Length/Width) bakarak,
    ###                         onun hangi Tür (Species) olduğunu tahmin edebilir miyiz?" sorusunu cevaplamak.
    ### Iris (Süsen)         : Çok bilinen bir çiçek türü.
    ### Species (Tür)        : Veri setindeki hedeftir. 3 farklı tür Iris çiçeği vardır: Iris-setosa, Iris-versicolor, Iris-virginica
    ### Petal (Taç Yaprak)   : Çiçeğin renkli, büyük yapraklarıdır.
    ### Sepal (Çanak Yaprak) : Çiçek tomurcuğunu koruyan, genellikle yeşil olan daha küçük yapraklardır.

    file_path = "/spark-demo/data/mllib/iris.csv"
    feature_columns = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width']
    label_column="species"
    ### ------- Iris veri seti -------

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


    spark = SparkSession.builder.appName("MLlibManualDemo").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark oturumu başladı")

    # Makine öğrenmesi algoritmaları metinden anlamaz, sayı isterler.
    # Sürecin tamamı insanların anladığı veriyi matematik diline, yani sayılara dönüştürmeye dayalıdır.

    # 1. VERİ YÜKLEME (EXTRACT)
    # Veri setini CSV formatında okuyoruz.
    # header=True, ilk satırın sütun adları olduğunu belirtir.
    # inferSchema=True, Spark'ın sütun türlerini (int, double, vb.) otomatik olarak algılamasını sağlar.
    input_df = spark.read.csv(file_path, header=True, inferSchema=True)

    print("✅ Veri seti yüklendi:")
    input_df.show(10)
    print("📊 Veri Şeması (Schema):")
    input_df.printSchema()

    # 2. VERİYİ BÖL
    # ÖNEMLİ: Tüm 'fit' (eğitim) işlemlerinden önce veriyi eğitim (%80) ve test (%20) olarak ikiye bölmeliyiz.
    # Bu, 'veri sızıntısını' (data leakage) engeller ve modelin sadece eğitim verisini görmesini sağlar.
    (training_data_raw, test_data_raw) = input_df.randomSplit([0.8, 0.2], seed=42)
    print(f"🚀 Veri bölündü: Eğitim verisi={training_data_raw.count()}, Test verisi={test_data_raw.count()}")

    # 3. ÖZELLİK MÜHENDİSLİĞİ (Feature Engineering) (TRANSFORM)
    print("🔧 Özellik Mühendisliği (Feature Engineering) başlıyor...")

    # Adım 3a: Feature Engineering - 1. Aşama (String -> Sayısal Etiket)
    # Metin formatındaki hedef sütunu (örn: "sonuc" veya "species")
    # modellerin anlayabileceği "label" adlı sayısal bir sütuna çevirir.
    # handleInvalid="keep": eğitimde görülmemiş sınıf testte çıkarsa "diğer" sınıfını oluştur
    # Örnek:
    #   0=Iris-setosa 1=Iris-versicolor ...
    #   0=Geçti 1=Kaldı
    #   .. gibi
    label_indexer = StringIndexer(inputCol=label_column, outputCol="label", handleInvalid="keep")

    # Adım 3b: Feature Engineering - 2. Aşama (Sütunlar -> Vektör)
    # MLlib modelleri, tüm özelliklerin tek bir vektör sütununda toplanmasını beklediği için
    # girdi sütunlarını (örn: "devamsızlık", "vize_notu") "features" adlı tek bir vektör sütununda birleştirir.
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

    # Adım 3c: Decision Tree
    # Algoritmanın kendisi. "features" sütununa bakarak "label" sütununu tahmin etmeyi öğrenir.
    # Bir Karar Ağacı (Decision Tree) sınıflandırma modeli oluşturuyoruz.
    # labelCol: Hedef değişkenin olduğu sütun.
    # featuresCol: Özellik vektörünün olduğu sütun.
    dt = DecisionTreeClassifier(
        labelCol="label", featuresCol="features",
        # "regularization" (düzenlileştirme) ile ilgili bazı parametreler:
        maxDepth=3, minInstancesPerNode=2, seed=42
    )

    # Adım 3d: 'fit' Adımı (Sadece Eğitim Verisiyle)
    # 'StringIndexer'ı (bir 'Estimator') SADECE eğitim verisi üzerinde 'fit' ediyoruz.
    # Bu, bize 'StringIndexerModel' adında eğitilmiş bir 'Transformer' verir.
    print("🔧 StringIndexer 'fit' ediliyor (SADECE eğitim verisiyle)...")
    label_indexer_model = label_indexer.fit(training_data_raw)

    # Adım 3e: 'transform' Adımı
    # 'preprocess' fonksiyonumuzu kullanarak HEM eğitim HEM test verisini dönüştürüyoruz.
    print("🔧 'preprocess' fonksiyonu ile veriler dönüştürülüyor...")
    training_data = preprocess(training_data_raw, label_indexer_model, assembler)
    test_data = preprocess(test_data_raw, label_indexer_model, assembler)

    print("✅ Özellik mühendisliği tamamlandı. Eğitim verisinin son hali:")
    training_data.select(label_column, "label", "features").show(10, truncate=False)


    # 4. MODEL EĞİTİMİ (TRAIN)
    print("🧠 Model (DecisionTree) eğitiliyor...")
    model = dt.fit(training_data)
    print("✅ Model (DecisionTree) eğitildi.")

    # 5. TAHMİN VE DEĞERLENDİRME (EVALUATE)
    # Eğitilmiş 'model'i (desicion tree'yi) test verisi üzerinde uyguluyoruz.
    print("🧪 Test verisi üzerinde tahmin yapılıyor... (Bu bir 'Dönüşüm'dür)")
    predictions = model.transform(test_data)

    print("📊 Test verisi üzerindeki tahminler:")
    # 'prediction' (modelin tahmini) ve 'label' (gerçek cevap) sütunlarını göster
    predictions.select(label_column, "label", "prediction", "features").show(10, truncate=False)

    # Modelin performansını değerlendir
    # MulticlassClassificationEvaluator, çok sınıflı sınıflandırma metriklerini hesaplar.
    # 'accuracy' (doğruluk) metriğini kullanıyoruz.
    print("📈 Modelin performansı değerlendiriliyor... (Bu bir 'Eylem'dir)")
    evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy"
    )
    accuracy = evaluator.evaluate(predictions)
    print(f"🎉 Modelin test verisi üzerindeki doğruluğu (Accuracy): {accuracy:.2f} (yani %{accuracy*100:.0f})")

    spark.stop()
    print("🏁 Spark oturumu durduruldu.")

if __name__ == "__main__":
    main()
