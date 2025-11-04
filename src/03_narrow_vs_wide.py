from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

# Geniş dönüşümler (wide transformations) çalıştırılmadan önce Spark UI'ı incelemek için bekleme süresi
WAIT_BEFORE_RUN_SECONDS = 60

# Geniş dönüşümler (wide transformations) tamamlandıktan sonra Spark UI'ı incelemek için bekleme süresi
WAIT_BEFORE_STOP_SECONDS = 120


def main():
    """
    Narrow (Dar) ve Wide (Geniş) dönüşümler arasındaki farkı gösteren demo.
    - Narrow dönüşümler, verinin executor'lar arasında taşınmasını (shuffle) gerektirmez.
      Her girdi bölümü (partition) en fazla bir çıktı bölümünü etkiler.
    - Wide dönüşümler, verinin yeniden dağıtılmasını (shuffle) gerektirir.
      Bu, ağ ve disk G/Ç'si nedeniyle maliyetli bir operasyondur.
    """
    spark = SparkSession.builder.appName("NarrowVsWideTransformations").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    print("✅ Spark oturumu başladı")

    # Basit bir RDD oluşturalım.
    data_rdd = sc.parallelize(range(0, 1000), 4)  # 4 bölüm (partition)

    # --- BÖLÜM 1: NARROW DÖNÜŞÜMLER ---
    print("\n" + "=" * 50)
    print("JOB 0: NARROW DÖNÜŞÜMLER")
    print("=" * 50)

    # map: Her elemana bir fonksiyon uygular. 1-1 eşleme.
    # Her bölümdeki veriler, diğer bölümlerden bağımsız olarak işlenebilir.
    mapped_rdd = data_rdd.map(lambda x: x * 2)
    print("🔧 Narrow Dönüşüm 1 tanımlandı: map(lambda x: x * 2)")

    # filter: Bir koşulu sağlayan elemanları tutar.
    # Bu da her bölümde bağımsız olarak yapılabilir.
    filtered_rdd = mapped_rdd.filter(lambda x: x % 3 == 0)
    print("🔧 Narrow Dönüşüm 2 tanımlandı: filter(lambda x: x % 3 == 0)")

    # EYLEM: collect()
    # Bu eylem, sadece narrow dönüşümler içeren bir işi (job) tetikler.
    print("\n🚀 Narrow dönüşümleri içeren işi tetiklemek için eylem çağrılıyor...")
    result_narrow = filtered_rdd.collect()
    print(f"🚀 Narrow dönüşümlerin sonucu (ilk 10 eleman): {result_narrow[:10]}")

    print()
    print("💤 Adrese git: http://localhost:4040/jobs/job/?id=0")
    print("💤 Gözlemle  : (DAG Visualization) DAG tek aşamalıdır (Stage 0)")
    print("💤 Gözlemle  : (tablo) 'Shuffle Read/Write' metrikleri sıfırdır")
    print(
        "💤 Neden     : Henüz hiç bir geniş dönüşüm (wide transformation) kullanmadık"
    )

    wait(WAIT_BEFORE_RUN_SECONDS, "Wide (geniş) dönüşümler başlayacak...")

    # --- BÖLÜM 2: WIDE DÖNÜŞÜM ---
    print("\n" + "=" * 50)
    print("JOB 1: WIDE DÖNÜŞÜM (SHUFFLE)")
    print("=" * 50)

    # groupByKey: Aynı anahtara sahip değerleri bir araya getirir.
    # Bu işlem bir WIDE dönüşümdür. 'x % 10' anahtarı aynı olan değerler
    # farklı bölümlerde (ve dolayısıyla farklı executor'larda) olabilir.
    # Spark, bu değerleri aynı bölümde toplamak için veriyi ağ üzerinden
    # taşımak (shuffle) zorundadır.
    # (anahtar, değer) çiftleri oluşturalım.
    kv_rdd = data_rdd.map(lambda x: (x % 10, x))
    print("🔧 Wide dönüşüm için hazırlık yapıldı: map(lambda x: (x % 10, x))")

    grouped_rdd = kv_rdd.groupByKey()
    print("🔧 Wide Dönüşüm tanımlandı: groupByKey()")

    # Sadece her gruptaki eleman sayısını alalım.
    group_counts_rdd = grouped_rdd.map(lambda kv: (kv[0], len(kv[1])))

    # EYLEM: collect()
    # Bu eylem, bir wide dönüşüm (groupByKey) içeren bir işi tetikler.
    print()
    print("🚀 Wide dönüşüm içeren işi tetiklemek için eylem çağrılıyor...")
    result_wide = group_counts_rdd.collect()
    print(f"🚀 Wide dönüşümün sonucu: {sorted(result_wide)}")

    print()
    print("🚀 Adrese git: http://localhost:4040/jobs/job/?id=1")
    print("🚀 Gözlemle  : (DAG Visualization) DAG çok aşamalıdır (Stage 1 & 2)")
    print("🚀 Gözlemle  : (tablo) 'Shuffle Read/Write' metrikleri sıfırdan büyüktür")
    print(
        "🚀 Neden     : groupByKey geniş dönüşüm (wide transformation) olduğu için shuffle (veri dağıtımı) yapılmasına sebep oldu"
    )

    wait(
        WAIT_BEFORE_STOP_SECONDS,
        "Spark kümesi (cluster) durdurulacak ve Spark UI'a erişilemeyecek. History Server (localhost:18080) üzerinden erişmeye devam edebilirsiniz.",
    )
    spark.stop()
    print("🏁 Spark oturumu durduruldu.")


def wait(seconds: int, message: str = "") -> None:
    if seconds <= 0:
        return

    for remaining in range(seconds, 0, -1):
        print(
            f"\r⏳ {remaining:2d} saniye içinde: {message}                 ",
            end="",
            flush=True,
        )
        time.sleep(1)

    print() # Sonraki satıra düzgün geçtiğimizden emin olmak için bir boş satır bırak

if __name__ == "__main__":
    main()
