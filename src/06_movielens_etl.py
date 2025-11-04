from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc


def main():
    """
    Gerçek dünya ETL (Extract, Transform, Load) senaryosu.
    MovieLens veri setini kullanarak şunu yapar:
    "En çok oy alan ilk 10 filmi bul ve bu filmlerin ortalama puanlarını göster."

    Bu demo, birden fazla veri kaynağını birleştirme (join), gruplama (groupBy),
    toplama (aggregation) ve sıralama (orderBy) gibi temel ETL işlemlerini gösterir.
    """
    spark = SparkSession.builder.appName("MovieLensETL").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark oturumu başladı")

    # Veri setlerinin yolları
    movies_path = "/spark-demo/data/movielens/movies.csv"
    ratings_path = "/spark-demo/data/movielens/ratings.csv"

    # 1. EXTRACT: Verileri Yükleme
    # movies.csv: movieId, title, genres
    # ratings.csv: userId, movieId, rating, timestamp
    print("🚀 Veri setleri yükleniyor...")
    movies_df = spark.read.csv(movies_path, header=True, inferSchema=True)
    ratings_df = spark.read.csv(ratings_path, header=True, inferSchema=True)

    print("📊 Movies DataFrame şeması:")
    movies_df.printSchema()
    print("📊 Ratings DataFrame şeması:")
    ratings_df.printSchema()

    # 2. TRANSFORM: Verileri Dönüştürme ve Analiz Etme
    # İki DataFrame'i 'movieId' sütunu üzerinden birleştiriyoruz.
    # Bu, her bir oy'a karşılık gelen film başlığını eklememizi sağlar.
    # 'join' bir WIDE dönüşümdür ve bir shuffle'a neden olur.
    print("🔧 'movies' ve 'ratings' DataFrame'leri birleştiriliyor...")
    movie_ratings_df = ratings_df.join(movies_df, "movieId")

    print("🔧 Birleştirilmiş DataFrame'den ilk 10 satır:")
    movie_ratings_df.show(10, truncate=False)

    # Filmleri başlıklarına göre gruplayıp her film için iki metrik hesaplıyoruz:
    # 1. Oy sayısı (rating_count)
    # 2. Ortalama puan (avg_rating)
    # 'groupBy' da bir WIDE dönüşümdür ve ikinci bir shuffle'a neden olur.
    print("🔧 Filmler gruplanıyor ve metrikler hesaplanıyor...")
    movie_stats_df = movie_ratings_df.groupBy("title") \
       .agg(
            count("rating").alias("rating_count"),
            avg("rating").alias("avg_rating")
        )

    print("🔧 Hesaplanan metriklerden ilk 10 satır:")
    movie_stats_df.show(10, truncate=False)

    # Sonuçları oy sayısına göre azalan sırada sıralıyoruz.
    print("🔧 Sonuçlar oy sayısına göre sıralanıyor...")
    top_movies_df = movie_stats_df.orderBy(desc("rating_count"))

    # 3. LOAD: Sonucu Gösterme
    # Sadece en çok oy alan ilk 10 filmi gösteriyoruz.
    print("\n" + "="*50)
    print("🔧 En Çok Oy Alan İlk 10 Film ve Ortalama Puanları")
    print("="*50)
    top_movies_df.show(10, truncate=False)

    # Bu işin Spark UI'daki DAG'ını incelemeniz tavsiye edilir.

    spark.stop()
    print("🏁 Spark oturumu durduruldu.")

if __name__ == "__main__":
    main()
