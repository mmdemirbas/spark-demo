import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

# Spark tarafından izlenecek girdi dizini
STREAMING_DIR = "/spark-demo/data/streaming/generated"
CHECKPOINT_DIR = "/spark-demo/data/streaming/checkpoint"

def main():
    """
    Spark Structured Streaming kullanarak basit bir dosya tabanlı akış uygulaması.
    Bu uygulama, belirtilen bir dizini yeni metin dosyaları için sürekli olarak izler.
    Yeni bir dosya eklendiğinde, içeriğini okur, kelimeleri sayar ve güncel
    kelime sayılarını konsola yazar.
    """
    spark = SparkSession.builder.appName("StructuredStreamingWordCount").getOrCreate()

    # Log seviyesini düşürerek konsol çıktısını daha okunabilir hale getiriyoruz.
    spark.sparkContext.setLogLevel("ERROR")
    print("✅ SparkSession oluşturuldu. Akış işlemi başlıyor...")

    # 1. AKIŞ KAYNAĞINI TANIMLAMA (READ STREAM)
    # spark.readStream kullanarak bir akış DataFrame'i oluşturuyoruz.
    #.format("text"): Kaynağın metin dosyaları olduğunu belirtir.
    #.load(): İzlenecek dizinin yolunu belirtir.
    # Spark, bu dizine eklenen her yeni dosyayı otomatik olarak işleyecektir.
    lines_df = spark.readStream.format("text").load(STREAMING_DIR)

    print(f"👀 '{STREAMING_DIR}' dizini yeni metin dosyaları için izleniyor.")
    print("⚠️ Ayrı bir terminalde '05_structured_streaming_generator.py' betiğini çalıştırın.")

    # 2. DÖNÜŞÜMLERİ TANIMLAMA (TRANSFORM)
    # Bu dönüşümler, batch (toplu) işlemdeki Word Count ile neredeyse aynıdır.
    # Spark'ın birleşik (unified) API'sinin gücü budur.
    words_df = lines_df.select(
        explode(
            split(col("value"), "\\s+")
        ).alias("word")
    )
    words_df = words_df.filter(col("word")!= "")
    word_counts_df = words_df.groupBy("word").count()

    # 3. AKIŞ HEDEFİNİ TANIMLAMA (WRITE STREAM)
    #.writeStream kullanarak sonuçların nasıl ve nereye yazılacağını tanımlıyoruz.
    #.format("console"): Sonuçları standart çıktıya (konsola) yazdırır.
    #.outputMode("complete"): Her tetiklemede (yeni dosya geldiğinde) tüm kelime
    #   sayımlarının tam tablosunu güncelleyip yazdırır. Diğer modlar "append" ve "update"dir.
    #.start(): Akış sorgusunu başlatır. Bu, arka planda sürekli çalışacak bir sorgu nesnesi döndürür.
    query = (
        word_counts_df.writeStream
           .queryName("streaming-word-count")    # UI'da adıyla gör
           .outputMode("update")                 # update: sadece değişenleri yaz, complete: hepsini yaz
           .format("console")
           .option("checkpointLocation", CHECKPOINT_DIR)
           .option("pathGlobFilter", "*.txt")    # sadece txt dosyalarını dinle
           .trigger(processingTime="1 seconds")  # düzenli aralıklarla işle
           .start()
    )
    print("🚀 Sorgu başlatıldı. Konsolda kelime sayıları güncellenecek.")

    #.awaitTermination(): Sorgu durdurulana veya bir hata alana kadar ana programın
    #   beklemesini sağlar. Bu olmadan, betik hemen biter ve akış işlemi durur.
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n⏳Kullanıcı tarafından durduruldu.")
    finally:
        print("🛑 Akış sorgusu durduruluyor...")
        query.stop()
        spark.stop()
        print("🏁 Akış sorgusu ve SparkSession durduruldu.")

if __name__ == "__main__":
    main()
