import datetime
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Final, List, Optional, Tuple

import pymongo
from celery.app.task import Task
from celery.utils.log import get_task_logger
from clp_py_utils.clp_config import (
    Database,
    S3Config,
    StorageEngine,
    StorageType,
    WorkerConfig,
)
from clp_py_utils.clp_logging import set_logging_level
from clp_py_utils.s3_utils import (
    generate_s3_virtual_hosted_style_url,
    get_credential_env_vars,
    s3_put,
)
from clp_py_utils.sql_adapter import SQL_Adapter
from job_orchestration.executor.query.celery import app
from job_orchestration.executor.query.utils import (
    report_task_failure,
    run_query_task,
)
from job_orchestration.executor.utils import load_worker_config
from job_orchestration.scheduler.job_config import ExtractIrJobConfig, ExtractJsonJobConfig
from job_orchestration.scheduler.scheduler_data import QueryTaskStatus

# Setup logging
logger = get_task_logger(__name__)

# Constant
STREAM_STAT_BEGIN_MSG_IX: Final[str] = "begin_msg_ix"
STREAM_STAT_END_MSG_IX: Final[str] = "end_msg_ix"
STREAM_STAT_IS_LAST_CHUNK: Final[str] = "is_last_chunk"
STREAM_STAT_LAST_ACCESS_TS: Final[str] = "last_access_ts"
STREAM_STAT_PATH: Final[str] = "path"
STREAM_STAT_STREAM_ID: Final[str] = "stream_id"


def _make_clp_command_and_env_vars(
    clp_home: Path,
    worker_config: WorkerConfig,
    archive_id: str,
    job_config: dict,
) -> Tuple[Optional[List[str]], Optional[Dict[str, str]]]:
    storage_type = worker_config.archive_output.storage.type
    archives_dir = worker_config.archive_output.get_directory()
    stream_output_dir = worker_config.stream_output.get_directory()

    if StorageType.S3 == storage_type:
        logger.error(
            f"IR extraction is not supported for storage type '{storage_type}' while using the"
            f" '{worker_config.package.storage_engine}' storage engine."
        )
        return None, None

    logger.info("Starting IR extraction")
    extract_ir_config = ExtractIrJobConfig.parse_obj(job_config)
    if not extract_ir_config.file_split_id:
        logger.error("file_split_id not supplied")
        return None, None
    command = [
        str(clp_home / "bin" / "clo"),
        "i",
        str(archives_dir / archive_id),
        extract_ir_config.file_split_id,
        str(stream_output_dir),
        "--print-ir-stats",
    ]
    if extract_ir_config.target_uncompressed_size is not None:
        command.append("--target-size")
        command.append(str(extract_ir_config.target_uncompressed_size))
    return command, None


def _make_clp_s_command_and_env_vars(
    clp_home: Path,
    worker_config: WorkerConfig,
    archive_id: str,
    job_config: dict,
) -> Tuple[Optional[List[str]], Optional[Dict[str, str]]]:
    storage_type = worker_config.archive_output.storage.type
    stream_output_dir = worker_config.stream_output.get_directory()
    stream_collection_name = worker_config.stream_collection_name

    logger.info("Starting JSON extraction")
    extract_json_config = ExtractJsonJobConfig.parse_obj(job_config)
    command = [
        str(clp_home / "bin" / "clp-s"),
        "x",
    ]

    dataset = extract_json_config.dataset
    if StorageType.S3 == storage_type:
        s3_config = worker_config.archive_output.storage.s3_config
        s3_object_key = f"{s3_config.key_prefix}{dataset}/{archive_id}"
        try:
            s3_url = generate_s3_virtual_hosted_style_url(
                s3_config.region_code, s3_config.bucket, s3_object_key
            )
        except ValueError as ex:
            logger.error(f"Encountered error while generating S3 url: {ex}")
            return None, None
        # fmt: off
        command.extend((
            s3_url,
            str(stream_output_dir),
            "--auth",
            "s3",
        ))
        # fmt: on
        env_vars = dict(os.environ)
        env_vars.update(get_credential_env_vars(s3_config.aws_authentication))
    else:
        archives_dir = worker_config.archive_output.get_directory() / dataset
        # fmt: off
        command.extend((
            str(archives_dir),
            str(stream_output_dir),
            "--archive-id",
            archive_id,
        ))
        # fmt: on
        env_vars = None

    # fmt: off
    command.extend((
        "--ordered",
        "--print-ordered-chunk-stats"
    ))
    # fmt: on

    if extract_json_config.target_chunk_size is not None:
        command.append("--target-ordered-chunk-size")
        command.append(str(extract_json_config.target_chunk_size))
    return command, env_vars


def _make_command_and_env_vars(
    clp_home: Path,
    worker_config: WorkerConfig,
    archive_id: str,
    job_config: dict,
) -> Tuple[Optional[List[str]], Optional[Dict[str, str]]]:
    storage_engine = worker_config.package.storage_engine
    if StorageEngine.CLP == storage_engine:
        command, env_vars = _make_clp_command_and_env_vars(
            clp_home,
            worker_config,
            archive_id,
            job_config,
        )
    elif StorageEngine.CLP_S == storage_engine:
        command, env_vars = _make_clp_s_command_and_env_vars(
            clp_home,
            worker_config,
            archive_id,
            job_config,
        )
    else:
        logger.error(f"Unsupported storage engine {storage_engine}")
        return None, None
    return command, env_vars


def _validate_stream_stats(stream_stats: Dict[str, Any]) -> bool:
    required_stream_stat_names = [
        STREAM_STAT_BEGIN_MSG_IX,
        STREAM_STAT_END_MSG_IX,
        STREAM_STAT_IS_LAST_CHUNK,
        STREAM_STAT_PATH,
        STREAM_STAT_STREAM_ID,
    ]
    for stat_name in required_stream_stat_names:
        if stat_name not in stream_stats:
            logger.error(f"Key `{stat_name}` doesn't exist in stream stats")
            return False

    return True


def _write_stream_metadata(
    stream_stats: Dict[str, Any], stream_relative_path: str, mongodb_uri: str, collection_name: str
) -> bool:
    stream_stats[STREAM_STAT_PATH] = stream_relative_path
    stream_stats[STREAM_STAT_LAST_ACCESS_TS] = int(time.time())
    try:
        with pymongo.MongoClient(mongodb_uri) as results_cache_client:
            results_cache_db = results_cache_client.get_default_database()
            stream_collection = results_cache_db.get_collection(collection_name)
            stream_collection.insert_one(stream_stats)

    except Exception as e:
        logger.exception(f"Unexpected exception when writing stream metadata: {e}")
        return False

    return True


@app.task(bind=True)
def extract_stream(
    self: Task,
    job_id: str,
    task_id: int,
    job_config: dict,
    archive_id: str,
    clp_metadata_db_conn_params: dict,
    results_cache_uri: str,
) -> Dict[str, Any]:
    task_name = "Stream Extraction"

    # Setup logging to file
    clp_logs_dir = Path(os.getenv("CLP_LOGS_DIR"))
    clp_logging_level = os.getenv("CLP_LOGGING_LEVEL")
    set_logging_level(logger, clp_logging_level)

    logger.info(f"Started {task_name} task for job {job_id}")

    start_time = datetime.datetime.now()
    task_status: QueryTaskStatus
    sql_adapter = SQL_Adapter(Database.parse_obj(clp_metadata_db_conn_params))

    # Load configuration
    clp_config_path = Path(os.getenv("CLP_CONFIG_PATH"))
    worker_config = load_worker_config(clp_config_path, logger)
    if worker_config is None:
        return report_task_failure(
            sql_adapter=sql_adapter,
            task_id=task_id,
            start_time=start_time,
        )

    # Make task_command
    clp_home = Path(os.getenv("CLP_HOME"))

    # Get S3 config
    storage_config = worker_config.stream_output.storage
    s3_config: Optional[S3Config] = (
        storage_config.s3_config if StorageType.S3 == storage_config.type else None
    )
    enable_s3_upload = s3_config is not None

    task_command, core_clp_env_vars = _make_command_and_env_vars(
        clp_home=clp_home,
        worker_config=worker_config,
        archive_id=archive_id,
        job_config=job_config,
    )
    if not task_command:
        logger.error(f"Error creating {task_name} command")
        return report_task_failure(
            sql_adapter=sql_adapter,
            task_id=task_id,
            start_time=start_time,
        )

    task_results, task_stdout_str = run_query_task(
        sql_adapter=sql_adapter,
        logger=logger,
        clp_logs_dir=clp_logs_dir,
        task_command=task_command,
        env_vars=core_clp_env_vars,
        task_name=task_name,
        job_id=job_id,
        task_id=task_id,
        start_time=start_time,
    )

    if QueryTaskStatus.SUCCEEDED == task_results.status:
        logger.info(f"Handling extracted streams...")

        error_encountered = False
        for line in task_stdout_str.splitlines():
            try:
                stream_stats = json.loads(line)
            except json.decoder.JSONDecodeError:
                logger.exception(f"`{line}` cannot be decoded as JSON")
                error_encountered = True
                continue

            if not _validate_stream_stats(stream_stats):
                error_encountered = True

            # If we've had a single error, we don't want to write stream metadata or try uploading
            # any other streams since that may unnecessarily slow down the task and generate a lot
            # of extraneous output.
            if error_encountered:
                continue

            stream_path = Path(stream_stats[STREAM_STAT_PATH])
            stream_name = stream_path.name
            if enable_s3_upload:
                logger.info(f"Uploading stream {stream_name} to S3...")
                try:
                    s3_put(s3_config, stream_path, stream_name)
                    logger.info(f"Finished uploading stream {stream_name} to S3.")
                except Exception as err:
                    logger.error(f"Failed to upload stream {stream_name}: {err}")
                    error_encountered = True
                    continue

                stream_path.unlink()

            if not _write_stream_metadata(
                stream_stats,
                stream_name,
                results_cache_uri,
                worker_config.stream_collection_name,
            ):
                error_encountered = True

        if error_encountered:
            task_results.status = QueryTaskStatus.FAILED
            task_results.error_log_path = str(os.getenv("CLP_WORKER_LOG_PATH"))
        else:
            logger.info(f"Finished handling streams.")

    return task_results.dict()
