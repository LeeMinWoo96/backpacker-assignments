# 백패커 과제

## 사용 데이터
https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store

## 요구 사항
- KST 기준 일일 파티셔닝
- Parquet 형식 및 Snappy 압축
- Hive External Table 지원
- 장애 복구 메커니즘
- 추가 기간 데이터 처리


## 사용 기술
> AWS Glue   
> Spark   
> Java 11


## 참고 사항
1. 관련 데이터들은 S3에 업로드 후 진행
2. metastore_db 는 default (derby 사용)

