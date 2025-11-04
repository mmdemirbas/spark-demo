from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws
import time

# Job devam etmeden önce Spark UI'yı incelemek için bekleme süresi
WAIT_BEFORE_RUN_SECONDS = 60

# Job tamamlandıktan sonra Spark UI'yı incelemek için bekleme süresi
WAIT_BEFORE_STOP_SECONDS = 120


def main():
    """
    Spark'ın Lazy Evaluation (Tembel Değerlendirme) özelliğini gösteren demo.
    Spark, bir eylem (action) çağrılana kadar dönüşümleri (transformations) çalıştırmaz.
    Bu betik, dönüşümler tanımlandıktan sonra, ancak eylem çağrılmadan hemen önce duraklar
    ve kullanıcıya Spark UI'ı kontrol etme fırsatı verir.
    """
    spark = SparkSession.builder.appName("LazyEvaluationDemo").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Bir DataFrame oluşturuyoruz. Bu işlem hızlıdır çünkü henüz veri işlenmiyor.
    data = [
        ("John", "Doe", "USA", "CA"),
        ("Jane", "Doe", "USA", "NY"),
        ("Mehmet", "Yilmaz", "TR", "Ankara"),
    ]
    columns = ["firstname", "lastname", "country", "state"]
    df = spark.createDataFrame(data=data, schema=columns)

    print("🚀 Başlangıç DataFrame'i tanımlandı:")
    # df.show() # Bu bir eylemdir, bu yüzden tembelliği göstermek için yorum satırı yapıldı.

    # 1. DÖNÜŞÜM: Sütun Seçimi (select)
    # Sadece 'firstname' ve 'state' sütunlarını seçen bir dönüşüm tanımlıyoruz.
    # Bu noktada HİÇBİR İŞLEM ÇALIŞTIRILMAZ. Spark sadece yapılması gerekeni not alır.
    df_selected = df.select("firstname", "state")
    print("🔧 Dönüşüm 1 tanımlandı: Sütun seçimi (select)")

    # 2. DÖNÜŞÜM: Filtreleme (filter)
    # Sadece 'CA' eyaletindeki kayıtları tutan bir dönüşüm daha tanımlıyoruz.
    # Bu da hemen ÇALIŞTIRILMAZ. Spark, bu adımı bir önceki adımın üzerine ekler.
    df_filtered = df_selected.filter(col("state") == "CA")
    print("🔧 Dönüşüm 2 tanımlandı: Filtreleme (filter)")

    # 3. DÖNÜŞÜM: Yeni Sütun Ekleme (withColumn)
    # Yeni bir 'full_name' sütunu ekleyen bir dönüşüm tanımlıyoruz.
    # Bu da ÇALIŞTIRILMAZ.
    df_final = df_filtered.withColumn(
        "full_name", concat_ws(" ", col("firstname"), col("state"))
    )
    print("🔧 Dönüşüm 3 tanımlandı: Yeni sütun ekleme (withColumn)")

    # --- KRİTİK NOKTA ---
    # Şu ana kadar sadece bir dizi dönüşüm tanımladık. Spark, bu adımlardan oluşan bir
    # "mantıksal plan" veya DAG (Directed Acyclic Graph) oluşturdu, ancak henüz
    # küme üzerinde hiçbir iş (job) başlatmadı.
    # Şimdi programı duraklatıp Spark UI'ı kontrol edeceğiz.
    print()
    print("💤 GECİKMELİ ÇALIŞTIRMA (LAZY EVALUATION) NOKTASI")
    print("💤 Adrese git: http://localhost:4040/jobs")
    print("💤 Gözlemle  : Spark UI'da 'Jobs' sekmesi TAMAMEN BOŞ")
    print("💤 Neden     : Henüz .show(), .count(), .collect() gibi bir 'Eylem' çağırmadık")

    wait(WAIT_BEFORE_RUN_SECONDS, "show() eylemi (action) çağrılacak...")

    # 4. EYLEM: Sonucu Gösterme (show)
    # .show() bir eylemdir. Bu komut, Spark'a şimdiye kadar tanımlanan tüm
    # dönüşümleri optimize edip küme üzerinde çalıştırmasını söyler.
    print()
    print("🚀 EYLEM ÇAĞRILDI: show()")
    print("🚀 Adrese git: http://localhost:4040/jobs")
    print("🚀 Gözlemle  : 'Completed Jobs' bölümünde tamamlanan işimiz listelendi")
    print("🚀 Neden     : .show() eylemini çağırdık")
    df_final.show()

    wait(
        WAIT_BEFORE_STOP_SECONDS,
        "Spark kümesi (cluster) durdurulacak ve Spark UI'a erişilemeyecek. History Server (localhost:18080) üzerinden erişmeye devam edebilirsiniz.",
    )
    spark.stop()


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
