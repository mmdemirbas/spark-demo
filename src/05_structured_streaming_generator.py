import os
import time
import random
import uuid
from datetime import datetime

# Spark tarafından izlenecek girdi dizini (structured streaming demosu)
STREAMING_DIR = "/spark-demo/data/streaming/generated"

# Örnek kelime listesi
WORDS = [
    "spark", "hadoop", "kafka", "flink", "data", "streaming",
    "realtime", "analytics", "pyspark", "sql", "dataframe", "rdd",
    "batch", "processing", "cluster", "executor", "driver"
]

def generate_random_text():
    """Rastgele kelimelerden oluşan bir metin satırı oluşturur."""
    num_words = random.randint(5, 15)
    line = ' '.join(random.choices(WORDS, k=num_words))
    return line

def main():
    """
    Belirtilen dizine periyodik olarak yeni metin dosyaları oluşturan bir betik.
    Bu, Spark Structured Streaming'in dosya tabanlı bir kaynağı nasıl işlediğini
    göstermek için bir veri akışını simüle eder.
    """
    print(f"Veri üretici başlatıldı. Dosyalar '{STREAMING_DIR}' dizinine yazılacak.")
    print("Durdurmak için Ctrl+C'ye basın.")

    # Hedef dizinin var olduğundan emin olun.
    os.makedirs(STREAMING_DIR, exist_ok=True)

    try:
        while True:
            # Benzersiz bir dosya adı oluştur.
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            unique_id = str(uuid.uuid4())[:8]
            file_name = f"log_{timestamp}_{unique_id}.txt"
            file_path = os.path.join(STREAMING_DIR, file_name)

            # Dosyaya rastgele metin yaz.
            num_lines = random.randint(1, 5)

            tmp_path = file_path + ".tmp"
            with open(tmp_path, 'w', encoding="utf-8") as f:
                for _ in range(num_lines):
                    f.write(generate_random_text() + '\n')
            os.replace(tmp_path, file_path)  # atomic

            print(f"Oluşturuldu: {file_name} ({num_lines} satır)")

            # Bir sonraki dosyayı oluşturmadan önce rastgele bir süre bekle.
            sleep_time = random.uniform(2, 5)
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\nVeri üretici durduruldu.")

if __name__ == "__main__":
    main()
