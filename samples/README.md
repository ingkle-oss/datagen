
# Kamp samples

| Filename | Name | Interval | Unique Alt | Incremental | Link |
| --- | --- | --- | --- | --- | --- |
| ingkle_an2_demo_battery_tester01.csv | 전자부품(배터리팩) 품질보증 AI 데이터셋 | 1 seconds | SerialNumber | - | <https://www.kamp-ai.kr/aidataDetail?AI_SEARCH=전자부품%28배터리팩%29+품질보증+AI+데이터셋&page=1&DATASET_SEQ=58&EQUIP_SEL=&GUBUN_SEL=&FILE_TYPE_SEL=&WDATE_SEL=> |
| ingkle_an2_demo_injection_moulder01.csv | 사출성형 예지보전 AI 데이터셋 | Machine_Cycle_Time (sec) | - | No_Shot (+1) | <https://www.kamp-ai.kr/aidataDetail?AI_SEARCH=사출성형+예지보전+AI+데이터셋&page=1&DATASET_SEQ=56&EQUIP_SEL=&GUBUN_SEL=&FILE_TYPE_SEL=&WDATE_SEL=> |
| ingkle_an2_demo_molding01.csv | 사출성형기 AI 데이터셋 | Cycle_Time (sec) | - | - | <https://www.kamp-ai.kr/aidataDetail?AI_SEARCH=성형&page=1&DATASET_SEQ=4&EQUIP_SEL=&GUBUN_SEL=&FILE_TYPE_SEL=&WDATE_SEL=> |
| ingkle_an2_demo_press01.csv | 프레스기 AI 데이터셋 | 1 seconds | - | - | <https://www.kamp-ai.kr/aidataDetail?AI_SEARCH=압력&page=1&DATASET_SEQ=7&EQUIP_SEL=&GUBUN_SEL=&FILE_TYPE_SEL=&WDATE_SEL>  |
| ingkle_an2_demo_qt_heater01.csv | 열처리 공정최적화 AI 데이터셋 | 1 seconds | process_num | - | <https://www.kamp-ai.kr/aidataDetail?AI_SEARCH=열처리+공정최적화+AI+데이터셋&page=1&DATASET_SEQ=61&EQUIP_SEL=&GUBUN_SEL=&FILE_TYPE_SEL=&WDATE_SEL=> |

# Other samples

| Filename | Name | Interval |
| --- | --- | --- |
| ingkle_an2_demo_mct01.csv | MCT 가공기 1 | 200 ms |
| ingkle_an2_demo_mct02.csv | MCT 가공기 2 | 100 ms |
| ingkle_an2_demo_press_cushion01.csv | NC Cushion | 20 ms |

# How to generate samples

```bash
# ingkle_an2_demo_battery_tester01
python3 src/produce_file.py \
--kafka-bootstrap-servers KAFKA_BOOTSTRAP_SERVERS \
--kafka-security-protocol SASL_SSL \
--kafka-sasl-username KAFKA_USERNAME \
--kafka-sasl-password KAFKA_PASSWORD \
--kafka-topic test-topic \
--report-interval 1 \
--input-filepath samples/kamp/ingkle_an2_demo_battery_tester01.csv \
--input-type csv \
--unique-alt-field SerialNumber \
--rate 1


# ingkle_an2_demo_injection_moulder01
python3 src/produce_file.py \
--kafka-bootstrap-servers KAFKA_BOOTSTRAP_SERVERS \
--kafka-security-protocol SASL_SSL \
--kafka-sasl-username KAFKA_USERNAME \
--kafka-sasl-password KAFKA_PASSWORD \
--kafka-topic test-topic \
--report-interval 1 \
--input-filepath samples/kamp/ingkle_an2_demo_injection_moulder01.csv \
--input-type csv \
--interval-field Machine_Cycle_Time \
--interval-field-unit second \
--incremental-field No_Shot \

python3 src/publish_file.py \
--mqtt-host emqx.aot.nazare.dev \
--mqtt-port 21883 \
--mqtt-username MQTT_USERNAME \
--mqtt-password MQTT_PASSWORD \
--mqtt-client-id test-publisher \
--mqtt-topic test-topic/data/_all \
--input-filepath samples/kamp/ingkle_an2_demo_injection_moulder01.csv \
--input-type csv \
--interval-field Machine_Cycle_Time \
--interval-field-unit second \
--incremental-field No_Shot


python3 src/redpanda_http_file.py \
--redpanda-host REDPANDA_HTTP_PROXY_HOST \
--redpanda-port REDPANDA_HTTP_PROXY_PORT \
--redpanda-ssl \
--kafka-sasl-username KAFKA_USERNAME \
--kafka-sasl-password KAFKA_PASSWORD \
--kafka-topic test-topic \
--input-filepath samples/kamp/ingkle_an2_demo_injection_moulder01.csv \
--input-type csv \
--interval-field Machine_Cycle_Time \
--interval-field-unit second \
--incremental-field No_Shot


# ingkle_an2_demo_molding01
python3 src/produce_file.py \
--kafka-bootstrap-servers KAFKA_BOOTSTRAP_SERVERS \
--kafka-security-protocol SASL_SSL \
--kafka-sasl-username KAFKA_USERNAME \
--kafka-sasl-password KAFKA_PASSWORD \
--kafka-topic test-topic \
--report-interval 1 \
--input-filepath samples/kamp/ingkle_an2_demo_molding01.csv \
--input-type csv \
--interval-field Cycle_Time \
--interval-field-unit second



# ingkle_an2_demo_press01.csv
python3 src/produce_file.py \
--kafka-bootstrap-servers KAFKA_BOOTSTRAP_SERVERS \
--kafka-security-protocol SASL_SSL \
--kafka-sasl-username KAFKA_USERNAME \
--kafka-sasl-password KAFKA_PASSWORD \
--kafka-topic test-topic \
--report-interval 1 \
--input-filepath samples/kamp/ingkle_an2_demo_press01.csv \
--input-type csv \
--rate 1


# ingkle_an2_demo_qt_heater01.csv
python3 src/produce_file.py \
--kafka-bootstrap-servers KAFKA_BOOTSTRAP_SERVERS \
--kafka-security-protocol SASL_SSL \
--kafka-sasl-username KAFKA_USERNAME \
--kafka-sasl-password KAFKA_PASSWORD \
--kafka-topic test-topic \
--report-interval 1 \
--input-filepath samples/kamp/ingkle_an2_demo_qt_heater01.csv \
--input-type csv \
--unique-alt-field process_num \
--rate 1



# ingkle_an2_demo_mct01.csv
python3 src/produce_file.py \
--kafka-bootstrap-servers KAFKA_BOOTSTRAP_SERVERS \
--kafka-security-protocol SASL_SSL \
--kafka-sasl-username KAFKA_USERNAME \
--kafka-sasl-password KAFKA_PASSWORD \
--kafka-topic test-topic \
--report-interval 1 \
--input-filepath samples/other/ingkle_an2_demo_mct01.csv \
--input-type csv \
--rate 5


# ingkle_an2_demo_mct02.csv
python3 src/produce_file.py \
--kafka-bootstrap-servers KAFKA_BOOTSTRAP_SERVERS \
--kafka-security-protocol SASL_SSL \
--kafka-sasl-username KAFKA_USERNAME \
--kafka-sasl-password KAFKA_PASSWORD \
--kafka-topic test-topic \
--report-interval 1 \
--input-filepath samples/kamp/ingkle_an2_demo_mct02.csv \
--input-type csv \
--rate 10

# ingkle_an2_demo_press_cushion01.csv
python3 src/produce_file.py \
--kafka-bootstrap-servers KAFKA_BOOTSTRAP_SERVERS \
--kafka-security-protocol SASL_SSL \
--kafka-sasl-username KAFKA_USERNAME \
--kafka-sasl-password KAFKA_PASSWORD \
--kafka-topic test-topic \
--report-interval 1 \
--input-filepath samples/kamp/ingkle_an2_demo_press_cushion01.csv \
--input-type csv \
--rate 50
```

# 데이터 셋 출처

중소벤처기업부, Korea AI Manufacturing Platform(KAMP), 사출성형기 AI 데이터셋, KAIST(울산과학기술원, (주)이피엠솔루션즈), 2020.12.14., <www.kamp-ai.kr>
