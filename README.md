# Local Airflow Practice
Practice of running airflow in docker.

## Quick Start
1. open termial in the same folder as docker-compose.yaml
2. run the below command

```bash
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

3. Run docker-compose up -d