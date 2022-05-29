https://prometheus.io/download/

## 프로메테우스 
```shell
# 프로메테우스 실행
./prometheus --config.file=prometheus.yml
```

> chrome 창에서 localhost:9090으로 접속
![img_0.png](src/main/resources/img/readme/img_0.png) 
> 현재 서버와 매핑되어 있지 않음

```yaml
# prometheus.yml 파일 수정
scrape_configs:
  # The job name is added as a label `job=<job_name>` to any timeseries scraped from this config.
  - job_name: "prometheus"

    # metrics_path defaults to '/metrics'
    # scheme defaults to 'http'.

    static_configs:
      - targets: ["localhost:9090"]

  - job_name: "actuator_prometheus"
    metrics_path: "/actuator/prometheus"
    scrape_interval: 5s
    static_configs:
      - targets: ["localhost:8180"]

```
> 아래와 같이 kafka_producer와 같은 목록이 추가됨 
>![img_1.png](src/main/resources/img/readme/img_1.png)
>![img_2.png](src/main/resources/img/readme/img_2.png)

# Grafana
- prometheus 보다 시각화된 자료를 활용하기 위해서 사용함
- url : https://grafana.com/grafana/download       
```shell
# grafana 실행
cd conf
cp defaults.ini grafana.ini

bin/grafana-server --config conf/grafana.ini
```
> ![img_3.png](src/main/resources/img/readme/img_3.png)
> ![img_4.png](src/main/resources/img/readme/img_4.png)
> ![img_5.png](src/main/resources/img/readme/img_5.png)
> ![img_6.png](src/main/resources/img/readme/img_6.png)
> 최초 계정  ID:admin / PW:admin
 

> ## Grafana Dashboard
> ### consumer
> ![img_7.png](src/main/resources/img/readme/img_7.png)
> 
> ### producer
> ![img_8.png](src/main/resources/img/readme/img_8.png)