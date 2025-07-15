from prefect import flow, task, get_run_logger
import hashlib
import datetime
import pathlib
import httpx
import json
import os
import random
from prefect.futures import wait
from prefect.task_runners import ThreadPoolTaskRunner
from prefect.cache_policies import INPUTS, TASK_SOURCE

CACHE_POLICY = (INPUTS + TASK_SOURCE).configure(key_storage="big-dataset-cache-keys")

WIKIMEDIA_MIRRORS = [
    "https://dumps.wikimedia.org",
    "https://wikimedia.bringyour.com",
    "https://wikipedia.c3sl.ufpr.br",
    "https://mirror.clarkson.edu",
    "https://wikimedia.mirror.clarkson.edu",
    "https://wikipedia.mirror.pdapps.org",
]


def md5(file_path: str):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(40960000), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


@task(cache_policy=CACHE_POLICY)
def download_file(file_url, save_path: pathlib.Path):
    logger = get_run_logger()

    mirror_list = WIKIMEDIA_MIRRORS.copy()
    random.shuffle(mirror_list)

    for mirror_url in mirror_list:
        try:
            url = mirror_url + file_url
            with httpx.stream("GET", url) as response:
                response.raise_for_status()
                with open(save_path, "wb") as f:
                    for chunk in response.iter_bytes():
                        f.write(chunk)
            return
        except Exception as exc:
            logger.info(f"Error response {exc} while requesting {url}.")

    raise Exception("All mirrors failed")


@task(log_prints=True)
def download_and_verify(file_info):
    # {'size': 2315628993,
    # 'url': '/enwiki/20250601/enwiki-20250601-page.sql.gz',
    # 'md5': 'f72c990b51c964dfd321b78f2193670c',
    # 'sha1': '316c2bf3794f356119990e778e3dc035ee37ea65',
    # 'title': 'enwiki-20250601-page.sql.gz',
    # 'output_folder_path': PosixPath('/tmp/20250601')}
    logger = get_run_logger()
    if 'url' not in file_info:
        logger.error(f"File info does not contain 'url' key. file_info: {file_info}")
        return
    logger.info(
        f"start download file {file_info['title']} from {file_info['url']}",
    )
    output_folder = file_info["output_folder_path"]
    dowmload_file = output_folder.joinpath(file_info["title"])
    dowmload_tmp_file = output_folder.joinpath(file_info["title"] + "_tmp")
    if dowmload_file.exists():
        print(
            f"file {dowmload_file} exists, skip download",
        )
        return

    download_file(file_info["url"], dowmload_tmp_file)

    file_md5 = md5(dowmload_tmp_file)
    if file_md5 == file_info["md5"]:
        dowmload_tmp_file.rename(dowmload_file)


@task
def dwonload_dumpstatus(dump_status_file: str, version_flag: str):

    url = f"https://dumps.wikimedia.org/enwiki/{version_flag}/dumpstatus.json"
    with httpx.Client() as client:
        response = client.get(url)
        response.raise_for_status()  # 如果不是 200，会抛异常
        with open(dump_status_file, "wb") as f:
            f.write(response.content)


@flow(task_runner=ThreadPoolTaskRunner(max_workers=3))
def wikimedia_dumper(output_folder: str, proxy: str = ""):
    logger = get_run_logger()
    logger.info("start wikimedia dumper")
    day_before_15 = datetime.datetime.now() - datetime.timedelta(days=8)
    version_flag = day_before_15.strftime("%Y%m01")

    output_folder_path = pathlib.Path(output_folder).joinpath(version_flag)
    output_folder_path.mkdir(exist_ok=True)

    # space_stats_flag = output_folder_path.joinpath("COMPLETE.txt")
    # if space_stats_flag.exists():
    #     with open(space_stats_flag, "rt") as f:
    #         data = f.read()
    #     logger.info("download complete as: %s", data)
    #     return

    dump_status_file = output_folder_path.joinpath("dumpstatus.json")
    dwonload_dumpstatus(dump_status_file, version_flag)

    with open(dump_status_file, "rt") as f:
        dumpstatus_data = json.load(f)

    dump_data_map = {}
    for key in [
        "pagetable",
        "categorytable",
        "categorylinkstable",
        "redirecttable",
        "geotagstable",
        "articlesdumprecombine",
        "langlinkstable",
        "externallinkstable",
        "pagelinkstable",
        "categorylinkstable",
        "metahistory7zdump",
    ]:
        if value:=dumpstatus_data["jobs"].get(key):
            dump_data_map[key] = value

    # todo
    # maybe anaylize the processing of the dumpstatus.json file

    files_to_download = []
    for metadump in dump_data_map.values():
        for title, elem in metadump.get("files", {}).items():
            elem["title"] = title
            elem["output_folder_path"] = output_folder_path
            files_to_download.append(elem)
    ret = download_and_verify.map(files_to_download)
    wait(ret)
    logger.info("download complete")


from prefect.client.schemas.objects import (
    ConcurrencyLimitConfig,
    ConcurrencyLimitStrategy,
)

if __name__ == "__main__":
    # wikimedia_dumper.serve(
    #     name="wikimedia-downloader",
    #     parameters={"output_folder": "/mnt/st01/wikipeida_download"},
    #     # set env
    #     # job_variables={"env": {"NAME": "Marvin"}},
    #     tags=["wikimedia", "crawler"],
    #     cron="39 17 * * *",
    # )
    wikimedia_dumper("/mnt/st01/wikipeida_download")
