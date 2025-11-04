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
├── data/                              # verilerin tutulduğu dizin
│   ├── movielens/
│   ├── word_count_input/
│   └── streaming_input/
├── scripts/                           # yardımcı betikler (script)
│   └── generate_text_files.py
├── src/                               # demo script'leri
│   ├── 01_word_count.py
│   ├── 02_lazy_evaluation.py
│   ├── 03_narrow_vs_wide.py
│   ├── 04a_mllib_classification.py
│   ├── 04b_structured_streaming.py
│   └── 05_movielens_etl.py
├── docker-compose.yml                 # docker compose projesi
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

4. Master node içinde istediğiniz demo betiğini (script) çalıştırın:
    ```bash
    /opt/spark/bin/spark-submit /spark-demo/src/01_word_count.py
    /opt/spark/bin/spark-submit /spark-demo/src/02_lazy_evaluation.py
    /opt/spark/bin/spark-submit /spark-demo/src/03_narrow_vs_wide.py
    /opt/spark/bin/spark-submit /spark-demo/src/04a_mllib_classification.py
    /opt/spark/bin/spark-submit /spark-demo/src/04b_structured_streaming.py
    /opt/spark/bin/spark-submit /spark-demo/src/05_movielens_etl.py
    ```
   Her betik ile ilgili ek açıklamaları kendi dosyası içinde yorum satırı olarak bulabilirsiniz.

5. Spark arayüzünü inceleyin:
    - Spark Master UI: `http://localhost:8080`
    - Spark History UI: `http://localhost:18080`

6. Servisleri durdurabilirsiniz (proje dizininde çalıştırın):
    ```bash
    docker compose down
    ```
