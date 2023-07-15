## Dockerfile 생성

> 아래와 같이 Dockerfile을 생성합니다.<br>
>
> - USER root가 없으면 permission denied가 발생합니다!<br>
> - gcc가 설치되지 않은 경우에도 에러가 발생하기 때문에 꼭 추가해줍니다.

```docker
FROM apache/airflow:2.5.1

# permission denied 방지
USER root

# gcc 설치
RUN apt-get update && apt-get install -y build-essential

# mysqlclient를 설치하기 위해 필요한 패키지 설치
RUN apt-get install -y default-libmysqlclient-dev

# airflow-webserver 실행 시
CMD ["webserver"]

```

## docker-compose 수정

> airflow-webserver에 추가하였습니다.
>
> - 아래와 같이 \_PIP_ADDITIONAL_REQUIREMENTS에 mysqlclient를 추가하고, build 설정을 추가합니다.

```yaml
...

# mysqlclient 추가
    _PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:- mysqlclient}

...

airflow-webserver:
    <<: *airflow-common
    command: webserver
    ####### build 추가 ########
    build:
      context: .
      dockerfile: Dockerfile
    #########################
    user: "${AIRFLOW_UID:-50000}:0"
    ports:
      - 8080:8080
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 10s
      timeout: 10s
      retries: 5
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully
...
```
