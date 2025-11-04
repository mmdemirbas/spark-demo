from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, lower, regexp_replace


def main():
    """
    Apache Spark'ın "Merhaba, Dünya!"'sı olan Word Count (Kelime Sayma) uygulaması.
    Bu uygulama, bir metin dosyasını okur, içerisindeki kelimeleri sayar ve
    her kelimenin kaç kez geçtiğini konsola yazdırır.
    """

    # SparkSession, Spark ile etkileşim kurmak için giriş noktasıdır.
    # .appName() ile uygulamamızın ismini veriyoruz (sadece Spark UI'da kolay ayırt edebilmek için)
    spark = SparkSession.builder.appName("WordCount").getOrCreate()

    # Çıktıda gereksiz kalabalık etmemesi için INFO loglarını kapatalım
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark oturumu başladı")

    # 0. GİRDİ olarak kullanılacak dosyayı belirtiyoruz.
    # Dilerseniz yolu değiştirerek diğer girdi dosyalarını kullanabilirsiniz.
    input_file_path = "/spark-demo/data/word_count/istiklal_marşı.txt"
    #input_file_path = "/spark-demo/data/word_count/atasözleri.txt"
    #input_file_path = "/spark-demo/data/word_count/yunus_emre.txt"

    # 1. EXTRACT: Veriyi Kaynaktan Okuma
    # Metin dosyasını bir DataFrame olarak okuyoruz.
    # Her satır, "value" adında tek bir sütuna sahip bir satır haline gelir.

    lines_df = spark.read.text(input_file_path)
    print(f"✅ Girdi dosyası okundu: {input_file_path}")

    print("🚀 İlk 10 satır:")
    lines_df.show(10, truncate=False)

    # 2. TRANSFORM: Veriyi Dönüştürme

    # Adım 2a: Hepsini küçük harfe çevir. (lower fonksiyonu)
    # Bu, "Milletimin" ve "milletimin" kelimelerinin aynı sayılmasını sağlar.
    # Ancak Türkçe'ye özgü I->ı ve İ->i dönüşümünden ötürü önce bu iki durumu
    # regexp_replace fonksiyonunu kullanarak manuel olarak ele aldık.
    lower_df = lines_df.select(
            lower(
                regexp_replace(regexp_replace(col("value"), "İ", "i"), "I", "ı")
            ).alias("line")
        )

    # Adım 2b: Noktalama işaretlerini ve "kelime-dışı" her şeyi temizle.
    # [^\p{L}\p{N}\s’'] -> Harf, Rakam, Boşluk, Kıvrık Kesme İşareti (’) veya
    #                   Düz Kesme İşareti (') DIŞINDA kalan her şeyi bul
    #                   ve bunları tek bir boşlukla değiştir.
    # \p{L} -> Unicode Harfler (ç, ğ, ı, ö, ş, ü dahil)
    # \p{N} -> Rakamlar
    # \s    -> Boşluklar
    # ’'    -> Kesme işaretleri
    clean_df = lower_df.select(
        regexp_replace(col("line"), r"[^\p{L}\p{N}\s’']", " ").alias("clean_line")
    )

    # Adım 2c: Temizlenmiş satırları boşluklara göre kelimelere ayır (split).
    # \s+ -> Bir veya daha fazla boşluk karakteri (temizlikten kalan fazla boşlukları yakalar)
    # explode -> Kelime dizisini tek tek satırlara patlat.
    words_df = clean_df.select(
        explode(
            split(col("clean_line"), r"\s+")
        ).alias("word")
    )

    # Adım 2d: Temizlik ve bölme sonrası oluşabilecek boş satırları filtrele.
    words_df = words_df.filter(col("word") != "")
    print("🚀 Temizlenmiş ve kelimelere ayrılmış DataFrame (ilk 100):")
    words_df.show(100)

    # Adım 2e: Kelimeleri gruplayıp say.
    word_counts_df = words_df.groupBy("word").count()

    # Adım 2f: Sonuçları frekansa (kaç kez geçtiğine) göre azalan sırada sırala.
    # Aynı sayıda geçen kelimeleri alfabetik sıraya göre sırala.
    sorted_word_counts_df = word_counts_df.orderBy(col("count").desc(), col("word").asc())

    # 3. LOAD: Sonucu Gösterme (Eylem)
    # .show() bir eylemdir (action) ve şu ana kadar tanımlanan tüm dönüşümlerin
    # (okuma, bölme, patlatma, gruplama, sayma) çalıştırılmasını tetikler.
    print("🚀 En çok geçen 100 kelime:")
    sorted_word_counts_df.show(100)

    # SparkSession'ı durdurmak, kümedeki kaynakları serbest bırakır.
    spark.stop()
    print("🏁 Spark oturumu durduruldu.")


if __name__ == "__main__":
    main()
