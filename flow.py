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

# CACHE_POLICY = (INPUTS + TASK_SOURCE).configure(key_storage="big-dataset-cache-keys")

WIKIMEDIA_MIRRORS = [
    "https://dumps.wikimedia.org",
    "https://wikimedia.bringyour.com",
    "https://wikipedia.c3sl.ufpr.br",
    "https://mirror.clarkson.edu",
    "https://wikimedia.mirror.clarkson.edu",
    "https://wikipedia.mirror.pdapps.org",
    "https://mirror.accum.se/mirror/wikimedia.org/dumps",
    "https://wikidata.aerotechnet.com",
]


def md5(file_path: str):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(40960000), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


# @task(cache_policy=CACHE_POLICY)
@task()
def download_file(file_url, save_path: pathlib.Path, proxy: str = ""):
    logger = get_run_logger()

    mirror_list = WIKIMEDIA_MIRRORS.copy()
    random.shuffle(mirror_list)

    for mirror_url in mirror_list:
        try:
            path_obj = pathlib.Path(save_path)
            headers = {}
            file_mode = "wb"  # 默认为二进制写入模式
            resumed_size = 0

            # 1. 检查文件是否存在，如果存在，则设置 Range 请求头和文件追加模式
            if path_obj.exists():
                resumed_size = path_obj.stat().st_size
                headers["Range"] = f"bytes={resumed_size}-"
                file_mode = "ab"  # 切换为二进制追加模式
                logger.info(f"发现已下载文件，将从 {resumed_size} 字节处继续下载...")
        
            url = mirror_url + file_url
            logger.info(f"start download '{save_path}' from {url}")

            with httpx.stream("GET", url, headers=headers, follow_redirects=True, timeout=30,  proxy=proxy) as response:
                # 如果服务器返回 200 OK 且我们请求了续传，说明服务器不支持，需从头开始
                if response.status_code == 200 and resumed_size > 0:
                    logger.info("服务器不支持断点续传，将重新从头开始下载。")
                    resumed_size = 0
                    file_mode = "wb"

                response.raise_for_status()

                # 获取文件总大小用于进度条
                # 对于断点续传 (206)，Content-Length 是剩余部分的大小
                remaining_size = int(response.headers.get("Content-Length", 0))
                total_size = resumed_size + remaining_size

                logger.info(f"文件总大小: {total_size / 1024 / 1024:.2f} MB")

                # 2. 以正确的模式打开文件并写入数据
                with open(path_obj, file_mode) as f:
                    for chunk in response.iter_bytes(chunk_size=8192):
                        f.write(chunk)
            
            logger.info(f"文件 '{save_path}' 下载完成。 ✨")

            return
        except Exception as exc:
            logger.info(f"proxy download fail:{url}, proxy:{proxy}, try next mirror")          


    raise Exception(f"All mirrors failed: {file_url}")


@task(log_prints=True)
def download_and_verify(file_info, proxy: str = ""):
    # {'size': 2315628993,
    # 'url': '/enwiki/20250601/enwiki-20250601-page.sql.gz',
    # 'md5': 'f72c990b51c964dfd321b78f2193670c',
    # 'sha1': '316c2bf3794f356119990e778e3dc035ee37ea65',
    # 'title': 'enwiki-20250601-page.sql.gz',
    # 'output_folder_path': PosixPath('/tmp/20250601')}
    logger = get_run_logger()
    if 'url' not in file_info:
        logger.warning(f"File info does not contain 'url' key. file_info: {file_info}")
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
    try:
        download_file(file_info["url"], dowmload_tmp_file)
    except Exception as exc:
        download_file(file_info["url"], dowmload_tmp_file, proxy)

    file_md5 = md5(dowmload_tmp_file)
    if file_md5 == file_info["md5"]:
        dowmload_tmp_file.rename(dowmload_file)


@task
def dwonload_dumpstatus(dump_status_file: str, version_flag: str, lang:str="en", proxy: str = ""):
    logger = get_run_logger()
    url = f"https://dumps.wikimedia.org/{lang}wiki/{version_flag}/dumpstatus.json"
    try:
        with httpx.Client() as client:
            response = client.get(url)
            response.raise_for_status()  # 如果不是 200，会抛异常
            with open(dump_status_file, "wb") as f:
                f.write(response.content)
                return
    except Exception as exc:
        logger.error(f"download dumpstatus.json fail, error: {exc}")
        
    if proxy:
        with httpx.Client(proxy=proxy) as client:
            response = client.get(url)
            response.raise_for_status()  # 如果不是 200，会抛异常
            with open(dump_status_file, "wb") as f:
                f.write(response.content)
                return


def wikimedia_dumper_task(output_folder: str, proxy: str = "",lang:str="en"):
    logger = get_run_logger()
    logger.info("start wikimedia dumper")
    day_before_15 = datetime.datetime.now() - datetime.timedelta(days=8)
    version_flag = day_before_15.strftime("%Y%m01")

    output_folder_path = pathlib.Path(output_folder).joinpath(lang).joinpath(version_flag)
    output_folder_path.mkdir(exist_ok=True)

    # space_stats_flag = output_folder_path.joinpath("COMPLETE.txt")
    # if space_stats_flag.exists():
    #     with open(space_stats_flag, "rt") as f:
    #         data = f.read()
    #     logger.info("download complete as: %s", data)
    #     return

    logger.info(
        f"output folder: {output_folder_path}, version_flag: {version_flag}",
    )
    dump_status_file = output_folder_path.joinpath("dumpstatus.json")
    dwonload_dumpstatus(dump_status_file, version_flag, lang, proxy)

    with open(dump_status_file, "rt") as f:
        dumpstatus_data = json.load(f)

    dump_data_map = {}
    for key in [
        "categorylinkstable",
        "pagelinkstable",
        "pagetable",
        "categorytable",
        "redirecttable",
        "geotagstable",
        "articlesdumprecombine",
        "langlinkstable",
        "externallinkstable",
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
    logger.info(f"files to download: {len(files_to_download)}")
    ret = download_and_verify.map(files_to_download, proxy)
    wait(ret)
    logger.info("download complete")


@flow(task_runner=ThreadPoolTaskRunner(max_workers=3))
def wikimedia_dumper(output_folder: str, proxy: str = "",):
    wikimedia_dumper_task(output_folder, proxy,"zh")
    wikimedia_dumper_task(output_folder, proxy,"en")


if __name__ == "__main__":
    # wikimedia_dumper.serve(
    #     name="wikimedia-downloader",
    #     parameters={"output_folder": "/mnt/st01/wikipeida_download"},
    #     # set env
    #     # job_variables={"env": {"NAME": "Marvin"}},
    #     tags=["wikimedia", "crawler"],
    #     cron="39 17 * * *",
    # )
    wikimedia_dumper("/mnt/st01/wikipeida_download",proxy="http://192.168.1.230:10808")
