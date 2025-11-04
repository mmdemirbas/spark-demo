# Apache Spark'a Giriş Demosu

## 0. Amaç

Bu proje, Apache Spark ile yeni tanışanlara hızlıca ayağa kaldırıp kurcalayabilecekleri bir ortam sunmayı ve temel
kavramları tanıtmayı amaçlamaktadır.

## 1. Ön Gereksinimler

- Docker Compose (Docker Desktop kurulu olması yeterlidir)
- İnternet bağlantısı (docker'ın image'ları indirebilmesi için)

Proje, olası kurulum sorunlarını ortadan kaldırmak için tamamen Docker container'ları içinde çalışacak şekilde
tasarlanmıştır. Bu yüzden Python, Scala, Java, Spark gibi başka herhangi bir araç setinin kurulu olması gerekmez.

## 2. Öğrenim Hedefleri

- Yerel, çok düğümlü bir Spark kümesini başlatma ve yönetme.
- PySpark uygulamalarını küme üzerinde çalıştırma (`spark-submit`) ve yürütülmesini izleme.
- Uygulama performansını anlamak için Spark UI'ı (Web Arayüzü) yorumlama.
- "Lazy Evaluation" (Tembel Değerlendirme) kavramını açıklama ve canlı olarak gösterme.
- "Narrow" (Dar) ve "Wide" (Geniş) dönüşümler arasındaki farkı anlama ve performansı etkileyen kritik "shuffle"
  operasyonlarını tespit etme.
- Birden çok veri kaynağını içeren temel bir uçtan uca ETL (Extract, Transform, Load) hattı uygulama.
- Spark'ın Makine Öğrenimi (MLlib) ve Akış (Streaming) yetenekleri hakkında kavramsal bir anlayış geliştirme.

## 3. Proje Yapısı

```
spark-starter-kit/
├── conf/
│   ├── spark-defaults.conf                   # Tüm düğümlerde ortak olan önemli Spark ayarları
├── data/
│   ├── core/                                  # Demo 1 için girdiler
│   ├── mllib/                                 # Demo 4a ve 4b için girdiler
│   └── streaming/                             # Demo 5 için girdiler
├── src/
│   ├── 01_word_count.py                       # Demo 1: Word count
│   ├── 02_lazy_evaluation.py                  # Demo 2: Lazy evaluation
│   ├── 03_narrow_vs_wide.py                   # Demo 3: Narrow-wide transformations
│   ├── 04a_mllib_manual.py                    # Demo 4a: MLlib ile machine learning classification (manual)
│   ├── 04b_mllib_pipeline.py                  # Demo 4b: MLlib ile machine learning classification (pipeline)
│   ├── 05_structured_streaming.py             # Demo 5: Structured streaming demosu
│   ├── 05_structured_streaming_generator.py   # Demo 5 için girdi üreten script
│   └── 06_movielens_etl.py
├── docker-compose.yml                         # docker compose proje dosyası
├── Dockerfile                                 # temel docker image'ını tanımlayan Dockerfile
└── README.md
```

## 4. Çalıştırma

1. Bir terminal açıp proje dizinine gidin (`cd`).

2. Servisleri başlatın:

   ```bash
   docker compose -up -d
   ```

3. Master node'a bağlanın:

   ```bash
   docker compose exec -it master /bin/bash
   ```

   Kısaca:
   ```bash
   docker compose down --remove-orphans &&
      docker compose -up -d &&
      docker compose exec master /bin/bash
   ```

4. Master node içinde istediğiniz demo betiğini (script) çalıştırın:

   ```bash
   # Batch (Spark Core & Spark SQL)
   /opt/spark/bin/spark-submit /spark-demo/src/01_word_count.py
   /opt/spark/bin/spark-submit /spark-demo/src/02_lazy_evaluation.py
   /opt/spark/bin/spark-submit /spark-demo/src/03_narrow_vs_wide.py

   # MLlib
   /opt/spark/bin/spark-submit /spark-demo/src/04a_mllib_manual.py
   /opt/spark/bin/spark-submit /spark-demo/src/04b_mllib_pipeline.py

   # Streaming (generator script ayrı bir terminalde çalıştırılıp sürekli veri üretmesi sağlanmalı)
   python3                     /spark-demo/src/05_structured_streaming_generator.py
   /opt/spark/bin/spark-submit /spark-demo/src/05_structured_streaming.py

   # Birleşik
   /opt/spark/bin/spark-submit /spark-demo/src/06_movielens_etl.py

   ```

   Her betik ile ilgili detaylı açıklamaları kendi dosyası içinde yorum satırı olarak bulabilirsiniz.

5. Spark arayüzünü inceleyin:
    - Spark History UI: `http://localhost:18080` (bitmiş ve devam eden tüm işler)
    - Spark UI: `http://localhost:4040` (sadece iş devam ederken erişilebilir)
    - Node'lar:
        - Spark Master: `http://localhost:8080`
        - Spark Worker: `http://localhost:8081`
        - Spark Worker: `http://localhost:8082`

6. Servisleri durdurabilirsiniz (proje dizininde çalıştırın):
   ```bash
   docker compose down --remove-orphans
   ```

## 5. Güncelleme

Daha yeni bir Spark versiyonuna geçmek isterseniz Dockerfile'daki versiyonu güncellemelisiniz.

Uyumlu Python paket versiyonlarını bulmak ve sabitlemek için şu yöntemi kullanabilirsiniz:

```bash
# Geçici bir container başlat (kendi Spark versiyonunu kullan)
docker run --rm -it --user root apache/spark:3.5.7 /bin/bash

# Bir seferlik Python paketlerini indirip versiyonları gör
apt-get update && apt-get install -y python3-pip
mkdir /tmp/deps && cd /tmp/deps
pip download "pyspark[ml,sql]==3.5.7"
ls -1

# Dockerfile'ı doğru versiyonları yazarak güncelle ve versiyonları sabitle
```
