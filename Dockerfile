# Spark core & Spark SQL demoları için base image (apache/spark:3.5.7) yeterlidir,
# özel Dockerfile kullanmadan doğrudan apache/spark:3.5.7 kullanılabilirdik.

# Ancak MLlib demosu için numpy gibi bağımlılıklar yüklü gelmediğinden ötürü
# kendi Dockerfile'ımız içinde bunları yüklememiz gerekiyor.

# 1. Temel image olarak Spark açık-kaynak topluluğu
#    tarafından bakımı yapılan resmi image ile başla
FROM apache/spark:3.5.7

# 2. Kurulum yapabilmek için geçici olarak 'root' kullanıcısına geç
USER root

# 3. MLlib için gerekli Python paketlerini kur (numpy zorunlu)
RUN apt-get update && \
  # 1. pip'i kur \
  apt-get install -y --no-install-recommends python3-pip && \
  # 2. pip'in kendisini güncelle \
  python3 -m pip install --upgrade pip && \
  # 3. Paketleri --no-cache-dir ve sabit versiyonlarla kur \
  python3 -m pip install --no-cache-dir \
    "numpy==1.24.4" \
    "pandas==2.0.3" \
    "pyarrow==17.0.0" && \
  # 4. Kurulum bittikten sonra pip'i temizle \
  apt-get purge -y --auto-remove python3-pip && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* /root/.cache/pip

# 4. Spark'ın güvenli öntanımlı kullanıcısına ('spark' kullanıcısı) geri dön
USER spark
