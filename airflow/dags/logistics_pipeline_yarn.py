from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

SPARK_CLIENT = "kraft-spark-client"
NAMENODE = "kraft-namenode"
HIVE_SERVER = "kraft-hive-server"

SPARK_SUBMIT_BASE = (
    f"docker exec -i {SPARK_CLIENT} /opt/spark/bin/spark-submit "
    "--master yarn --deploy-mode client "

    # --- PERSISTENCIA PARA CLASE: Usamos los JARs que subimos a HDFS ---
    "--conf spark.yarn.jars=hdfs:///spark-jars/*.jar "

    # ---- LÍMITES AJUSTADOS (Mínimo 512m para evitar SparkIllegalArgumentException) ----
    "--conf spark.dynamicAllocation.enabled=false "
    "--conf spark.executor.instances=1 "
    "--conf spark.executor.cores=1 "

    # Subimos a 512m para cumplir con el requisito de Spark 3.5.1
    "--conf spark.executor.memory=512m "
    "--conf spark.executor.memoryOverhead=128m "

    # AM y Driver con memoria suficiente para ser estables
    "--conf spark.yarn.am.memory=512m "
    "--conf spark.yarn.am.memoryOverhead=128m "
    "--conf spark.driver.memory=512m "

    # Evitar sobrecarga en el clúster
    "--conf spark.sql.shuffle.partitions=2 "
    "--conf spark.default.parallelism=2 "
    "--conf spark.yarn.maxAppAttempts=1 "

    # Hadoop conf
    "--conf spark.yarn.appMasterEnv.HADOOP_CONF_DIR=/opt/hadoop-conf "
    "--conf spark.executorEnv.HADOOP_CONF_DIR=/opt/hadoop-conf "
)


with DAG(
    dag_id="logistics_pipeline_yarn",
    start_date=datetime(2026, 2, 11),
    schedule="@monthly",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["kraft", "yarn"],
    max_active_runs=1,      
    max_active_tasks=1,
) as dag:

    # 1) Verificar raw
   
    check_raw = BashOperator(
        task_id="check_raw_data",
        bash_command="docker exec -i " + NAMENODE + " hdfs dfs -test -d /data/transport/raw/dt={{ ds }}",
    )
   
   
    # 2) Clean transport
    run_clean_transport = BashOperator(
        task_id="spark_clean_transport",
        bash_command=SPARK_SUBMIT_BASE + "local:///opt/spark-apps/clean_logistics.py --dt {{ ds }}",
    )

    # 3) Clean weather
    run_clean_weather = BashOperator(
        task_id="spark_clean_weather",
        bash_command=SPARK_SUBMIT_BASE + "local:///opt/spark-apps/clean_weather.py --dt {{ ds }}",
    )

    # 4) Graph analytics (GraphFrames + Cassandra connector)
    run_graphs = BashOperator(
        task_id="spark_graph_analytics",
        bash_command=(
            SPARK_SUBMIT_BASE
            + "--packages "
              "graphframes:graphframes:0.8.3-spark3.5-s_2.12,"
              "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 "
            + "local:///opt/spark-apps/batch_analytics.py --dt {{ ds }}"
        ),
    )

    # 5) Curated stats
    run_curated = BashOperator(
        task_id="spark_curated_stats",
        bash_command=SPARK_SUBMIT_BASE + "local:///opt/spark-apps/curated_logistics.py --dt {{ ds }}",
    )

    # 6) Permisos HDFS antes de Hive
    fix_permissions = BashOperator(
        task_id="fix_hdfs_permissions",
        bash_command=f"docker exec -i {NAMENODE} hdfs dfs -chmod -R 777 /data/transport /data/weather || true",
    )

    # 7) Reporte en Hive (beeline)
    hive_report = BashOperator(
        task_id="hive_report",
        bash_command=(
            f"docker exec -i {HIVE_SERVER} /opt/hive/bin/beeline "
            "-u jdbc:hive2://localhost:10000 -n hive -p hivepassword "
            "-f /opt/hive_init/hive_init.hql"
        ),
    )

    # 8) Limpiar checkpoints (opcional)
    hdfs_cleanup = BashOperator(
        task_id="hdfs_cleanup",
        bash_command=f"docker exec -i {NAMENODE} hdfs dfs -rm -r -f /tmp/spark-checkpoints/* || true",
    )

    # --- AJUSTE DE DEPENDENCIAS PARA EL JOIN (SIN PARALELO PARA NO SATURAR EL PC) ---

    # 1) El chequeo inicial desbloquea la primera limpieza
    check_raw >> run_clean_transport

    # 2) Cuando termina transporte, lanza clima (ya NO van en paralelo)
    run_clean_transport >> run_clean_weather

    # 3) La analítica curada necesita AMBOS (transporte y clima) para el JOIN
    #    (como weather depende de transport, con esto garantizas que ya están los dos)
    run_clean_weather >> run_curated

    # 4) La analítica de grafos solo depende del transporte limpio (puede ir después de transport)
    run_clean_transport >> run_graphs

    # 5) Flujo final hacia Hive (se mantiene igual)
    run_curated >> fix_permissions >> hive_report >> hdfs_cleanup
    run_graphs >> hdfs_cleanup

