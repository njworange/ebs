import datetime
import pathlib
import re
import threading

import flask
import requests
from sqlalchemy import inspect, text

from plugin.create_plugin import PluginBase
from plugin.logic_module_base import PluginModuleBase
from support.expand.ffmpeg import SupportFfmpeg
from tool import ToolUtil

from .clients import EbsTvClient
from .models import ModelEbsEpisode
from .queue_service import QueueService
from .setup import F, P

name = "auto"


def normalize_text(text: str) -> str:
    text = (text or "").lower()
    text = re.sub(r"\s+", "", text)
    return text


def parse_keywords(value: str) -> list[str]:
    if not value:
        return []
    tokens = re.split(r"[\n,]+", value)
    result = []
    for token in tokens:
        parsed = normalize_text(token)
        if parsed:
            result.append(parsed)
    return result


def parse_release_date(value: str) -> datetime.date | None:
    value = (value or "").strip()
    match = re.search(r"(\d{4})\.(\d{2})\.(\d{2})", value)
    if not match:
        return None
    try:
        return datetime.date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except ValueError:
        return None


def parse_collect_since(value: str) -> datetime.date | None:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return datetime.date.fromisoformat(value[:10])
    except ValueError:
        return None


def parse_int_arg(value: str) -> int | None:
    try:
        return int(str(value).strip())
    except Exception:
        return None


def title_needs_upgrade(title: str) -> bool:
    norm = normalize_text(title)
    return (norm in {"", "ebs", "ebs애니키즈", "애니키즈", "ebsenglish", "ebsenglishtv"}) or norm.isdigit()


class ModuleAuto(PluginModuleBase):
    download_thread = None

    def __init__(self, P: PluginBase) -> None:
        super(ModuleAuto, self).__init__(P, "list", scheduler_desc="EBS TV 자동 다운로드")
        self.name = name
        self.db_default = {
            f"{P.package_name}_{self.name}_last_list_option": "",
            f"{self.name}_interval": "30",
            f"{self.name}_auto_start": "False",
            f"{self.name}_collect_since": "",
            f"{self.name}_download_mode": "blacklist",
            f"{self.name}_blacklist_program": "",
            f"{self.name}_blacklist_episode": "",
            f"{self.name}_whitelist_program": "",
            f"{self.name}_whitelist_episode": "",
            f"{self.name}_scan_page_limit": "5",
            f"{self.name}_allow_preview": "False",
            f"{self.name}_download_subtitle": "False",
            f"{self.name}_max_retry": "5",
            f"{self.name}_retry_failed": "True",
        }
        self.web_list_model = ModelEbsEpisode

    @property
    def filter_settings(self) -> dict:
        return {
            "mode": P.ModelSetting.get(f"{self.name}_download_mode") or "blacklist",
            "whitelist_program": parse_keywords(P.ModelSetting.get(f"{self.name}_whitelist_program") or ""),
            "whitelist_episode": parse_keywords(P.ModelSetting.get(f"{self.name}_whitelist_episode") or ""),
            "blacklist_program": parse_keywords(P.ModelSetting.get(f"{self.name}_blacklist_program") or ""),
            "blacklist_episode": parse_keywords(P.ModelSetting.get(f"{self.name}_blacklist_episode") or ""),
        }

    def process_menu(self, page_name: str, req: flask.Request) -> flask.Response:
        arg = P.ModelSetting.to_dict()
        if page_name == "setting":
            arg["is_include"] = F.scheduler.is_include(self.get_scheduler_id())
            arg["is_running"] = F.scheduler.is_running(self.get_scheduler_id())
        return flask.render_template(f"{P.package_name}_{name}_{page_name}.html", arg=arg)

    def process_command(
        self, command: str, arg1: str, arg2: str, arg3: str, req: flask.Request
    ) -> flask.Response:
        ret: dict[str, object] = {"ret": "success"}
        match command:
            case "collect_now":
                collected = self.collect_episodes()
                queued = self.enqueue_candidates(include_failed=True)
                ret["msg"] = f"신규 {collected}개 수집, {queued}개를 큐에 추가했습니다."
            case "queue_status":
                items = QueueService.current_items()
                ret["msg"] = f"큐에 {len(items)}개 항목이 있습니다."
                ret["data"] = items
            case "retry_failed":
                reset_count = self.retry_failed()
                queued = self.enqueue_candidates(include_failed=True)
                ret["msg"] = f"실패 항목 {reset_count}개를 재시도 상태로 변경, {queued}개를 큐에 추가했습니다."
            case "queue_reset":
                ret["msg"] = f"큐 초기화 완료 ({self.reset_queue()}개)."
            case "reset_status":
                item_id = parse_int_arg(arg1)
                item = ModelEbsEpisode.get_by_id(item_id) if item_id is not None else None
                if not item:
                    ret["ret"] = "warning"
                    ret["msg"] = "항목을 찾을 수 없습니다."
                else:
                    item.completed = False
                    item.retry = 0
                    item.status = "PENDING"
                    item.message = ""
                    item.save()
                    ret["msg"] = "상태를 초기화했습니다."
            case "delete":
                item_id = parse_int_arg(arg1)
                if item_id is not None and ModelEbsEpisode.delete_by_id(item_id):
                    ret["msg"] = "삭제했습니다."
                else:
                    ret["ret"] = "warning"
                    ret["msg"] = "삭제에 실패했습니다."
            case "download_item":
                item_id = parse_int_arg(arg1)
                item = ModelEbsEpisode.get_by_id(item_id) if item_id is not None else None
                if not item:
                    ret["ret"] = "warning"
                    ret["msg"] = "항목을 찾을 수 없습니다."
                else:
                    item.completed = False
                    item.retry = 0
                    item.status = "WAITING"
                    item.message = ""
                    item.save()
                    if QueueService.enqueue_item(item.id):
                        ret["msg"] = f"다운로드 큐에 추가했습니다. (ID: {item.id})"
                    else:
                        ret["msg"] = f"이미 큐에 있습니다. (ID: {item.id})"
            case "add_condition":
                target = (arg1 or "").strip()
                value = (arg2 or "").strip()
                if not target or not value:
                    ret["ret"] = "warning"
                    ret["msg"] = "추가할 대상이 없습니다."
                else:
                    old_list = P.ModelSetting.get_list(target, ",")
                    old_str = P.ModelSetting.get(target) or ""
                    if value in old_list:
                        ret["ret"] = "warning"
                        ret["msg"] = "이미 설정되어 있습니다."
                    else:
                        P.ModelSetting.set(target, f"{old_str}, {value}" if old_str else value)
                        ret["msg"] = "추가했습니다."
            case _:
                ret["ret"] = "warning"
                ret["msg"] = f"지원하지 않는 명령: {command}"
        return flask.jsonify(ret)

    def plugin_load(self) -> None:
        self.ensure_schema_columns()
        QueueService.ensure_queue()
        if ModuleAuto.download_thread is None:
            ModuleAuto.download_thread = threading.Thread(target=self.download_thread_function, args=())
            ModuleAuto.download_thread.daemon = True
            ModuleAuto.download_thread.start()
        collect_since = (P.ModelSetting.get(f"{self.name}_collect_since") or "").strip()
        if not collect_since:
            collect_since = datetime.date.today().isoformat()
            P.ModelSetting.set(f"{self.name}_collect_since", collect_since)
            P.logger.info("[ebs] 자동 수집 기준일 초기화: %s", collect_since)
        for item in ModelEbsEpisode.get_queue_states():
            item.status = "PENDING"
            item.save()
        self.enqueue_candidates()

    def scheduler_function(self) -> None:
        P.logger.debug("[ebs] Scheduler start")
        collected = self.collect_episodes()
        queued = self.enqueue_candidates()
        P.logger.debug("[ebs] Scheduler end - collected=%s queued=%s", collected, queued)

    def ensure_schema_columns(self) -> None:
        table_name = ModelEbsEpisode.__tablename__
        with F.app.app_context():
            engine = F.db.get_engine(bind=getattr(ModelEbsEpisode, "__bind_key__", None))
            inspector = inspect(engine)
            if not inspector.has_table(table_name):
                return
            columns = {col.get("name") for col in inspector.get_columns(table_name)}
            migrations = [
                ("thumbnail", "VARCHAR(512)"),
                ("display_title", "VARCHAR(255)"),
                ("source_type", "VARCHAR(32)"),
                ("remote_program_id", "VARCHAR(64)"),
                ("remote_episode_id", "VARCHAR(64)"),
                ("remote_media_id", "VARCHAR(64)"),
                ("is_preview", "BOOLEAN"),
                ("is_login", "VARCHAR(1)"),
                ("buy_state", "VARCHAR(16)"),
            ]
            for column_name, column_type in migrations:
                if column_name in columns:
                    continue
                with engine.begin() as conn:
                    conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"))

    def make_client(self) -> EbsTvClient:
        return self.make_public_client()

    def make_public_client(self) -> EbsTvClient:
        return EbsTvClient(
            cookie="",
            user_agent=P.ModelSetting.get("basic_user_agent") or "Mozilla/5.0",
        )

    def make_auth_client(self) -> EbsTvClient:
        return EbsTvClient(
            cookie=(P.ModelSetting.get("basic_cookie") or "").strip(),
            user_agent=P.ModelSetting.get("basic_user_agent") or "Mozilla/5.0",
        )

    def refresh_cookie_with_saved_account(self, force: bool = False) -> tuple[bool, str]:
        if (not force) and (not P.ModelSetting.get_bool("basic_cookie_refresh")):
            return False, "자동 쿠키 갱신이 꺼져 있습니다."
        user_id = (P.ModelSetting.get("basic_account_id") or "").strip()
        password = P.ModelSetting.get("basic_account_pw") or ""
        if (not user_id) or (not password):
            return False, "자동 갱신용 계정(ID/PW)이 저장되어 있지 않습니다."
        user_agent = P.ModelSetting.get("basic_user_agent") or "Mozilla/5.0"
        result = EbsTvClient.login_and_get_cookie(user_id=user_id, password=password, user_agent=user_agent)
        if result.get("success") and result.get("cookie"):
            P.ModelSetting.set("basic_cookie", result.get("cookie"))
            return True, "저장된 계정으로 쿠키를 갱신했습니다."
        return False, result.get("message") or "쿠키 갱신 실패"

    def collect_episodes(self) -> int:
        client = self.make_public_client()
        page_limit = max(P.ModelSetting.get_int(f"{self.name}_scan_page_limit"), 1)
        collect_since = parse_collect_since(P.ModelSetting.get(f"{self.name}_collect_since"))
        created = 0
        skipped_unsupported = 0
        skipped_old = 0
        for page in range(1, page_limit + 1):
            try:
                rows = client.collect_daily_vods(page=page)
            except requests.exceptions.Timeout as e:
                if page == 1:
                    retry_timeout = max(client.timeout * 3, 45)
                    P.logger.warning(
                        "[ebs] collect_daily_vods timeout: page=%s, retrying once with timeout=%s (%s)",
                        page,
                        retry_timeout,
                        e,
                    )
                    try:
                        rows = client.collect_daily_vods(page=page, timeout=retry_timeout)
                    except requests.exceptions.Timeout as retry_error:
                        P.logger.warning("[ebs] collect_daily_vods retry timeout: page=%s (%s)", page, retry_error)
                        break
                else:
                    P.logger.warning("[ebs] collect_daily_vods timeout: page=%s (%s)", page, e)
                    break
            if not rows:
                break
            P.logger.info("[ebs] collect_daily_vods page=%s rows=%s", page, len(rows))
            for row in rows:
                if not isinstance(row, dict):
                    continue
                if row.get("source_type") != "tv_show":
                    skipped_unsupported += 1
                    continue
                release_date = parse_release_date(row.get("release_date") or "")
                if collect_since and release_date and release_date < collect_since:
                    skipped_old += 1
                    continue
                remote_program_id = (row.get("remote_program_id") or row.get("course_id") or "").strip()
                remote_episode_id = (row.get("remote_episode_id") or row.get("lect_id") or "").strip()
                remote_media_id = (row.get("remote_media_id") or row.get("step_id") or "").strip()
                if (not remote_program_id) or (not remote_episode_id) or (not remote_media_id):
                    skipped_unsupported += 1
                    continue
                if row.get("source_type") == "tv_show":
                    needs_metadata = (
                        (not row.get("thumbnail"))
                        or (not row.get("episode_no"))
                        or title_needs_upgrade(row.get("program_title") or "")
                        or (row.get("display_title") or "") == (row.get("episode_title") or "")
                    )
                    if needs_metadata:
                        try:
                            metadata = client.fetch_show_metadata(remote_program_id, remote_episode_id, remote_media_id)
                            if metadata.get("thumbnail") and not row.get("thumbnail"):
                                row["thumbnail"] = metadata.get("thumbnail")
                            if metadata.get("episode_no") and not row.get("episode_no"):
                                row["episode_no"] = metadata.get("episode_no")
                            if metadata.get("program_title") and title_needs_upgrade(row.get("program_title") or ""):
                                row["program_title"] = metadata.get("program_title")
                            if metadata.get("display_title"):
                                row["display_title"] = metadata.get("display_title")
                        except Exception as e:
                            P.logger.debug(
                                "[ebs] show 메타데이터 보강 실패: %s / %s / %s (%s)",
                                remote_program_id,
                                remote_episode_id,
                                remote_media_id,
                                e,
                            )
                item = ModelEbsEpisode.get_by_keys(remote_program_id, remote_episode_id, remote_media_id)
                if item:
                    updated = False
                    if row.get("thumbnail") and not item.thumbnail:
                        item.thumbnail = row.get("thumbnail") or ""
                        updated = True
                    if row.get("episode_no") and not item.episode_no:
                        item.episode_no = row.get("episode_no") or ""
                        updated = True
                    if row.get("program_title") and title_needs_upgrade(item.program_title or ""):
                        item.program_title = row.get("program_title") or item.program_title
                        updated = True
                    if row.get("display_title") and (
                        (not item.display_title)
                        or (item.display_title == item.episode_title)
                        or title_needs_upgrade(item.display_title or "")
                        or normalize_text(item.display_title or "") == normalize_text(item.remote_program_id or "")
                    ):
                        item.display_title = row.get("display_title") or item.display_title
                        updated = True
                    if updated:
                        item.save()
                    continue
                item = ModelEbsEpisode(remote_program_id, remote_episode_id, remote_media_id)
                item.set_info(
                    program_title=(row.get("program_title") or remote_program_id),
                    display_title=(row.get("display_title") or row.get("program_title") or remote_program_id),
                    episode_no=(row.get("episode_no") or ""),
                    episode_title=(row.get("episode_title") or ""),
                    release_date=(row.get("release_date") or ""),
                    show_url=(row.get("show_url") or client.build_show_url(remote_program_id, remote_episode_id, remote_media_id)),
                    thumbnail=(row.get("thumbnail") or ""),
                    source_type=(row.get("source_type") or "tv_show"),
                )
                item.save()
                created += 1
        P.logger.info(
            "[ebs] 수집 요약 - created=%s skipped_unsupported=%s skipped_old=%s since=%s",
            created,
            skipped_unsupported,
            skipped_old,
            collect_since.isoformat() if collect_since else "",
        )
        return created

    def _is_allowed(self, item: ModelEbsEpisode, settings: dict) -> tuple[bool, str]:
        if item.completed:
            return False, "이미 완료됨"
        program = normalize_text(item.program_title)
        episode = normalize_text(item.episode_title)
        mode = settings["mode"]
        if mode == "whitelist":
            in_whitelist = False
            for keyword in settings["whitelist_program"]:
                if keyword in program:
                    in_whitelist = True
                    break
            if not in_whitelist:
                for keyword in settings["whitelist_episode"]:
                    if keyword in episode:
                        in_whitelist = True
                        break
            if not in_whitelist:
                return False, "화이트리스트 미일치"
            return True, ""
        for keyword in settings["blacklist_program"]:
            if keyword in program:
                return False, "블랙리스트 프로그램 키워드 일치"
        for keyword in settings["blacklist_episode"]:
            if keyword in episode:
                return False, "블랙리스트 에피소드 키워드 일치"
        return True, ""

    def enqueue_candidates(self, include_failed: bool | None = None) -> int:
        settings = self.filter_settings
        if include_failed is None:
            include_failed = P.ModelSetting.get_bool(f"{self.name}_retry_failed")
        max_retry = max(P.ModelSetting.get_int(f"{self.name}_max_retry"), 1)
        add_count = 0
        for item in ModelEbsEpisode.get_candidates(max_retry=max_retry):
            if item.status == "WAITING":
                if QueueService.enqueue_item(item.id):
                    add_count += 1
                continue
            if item.status == "FILTERED":
                continue
            if (item.status in ("FAILED", "PREVIEW_BLOCKED", "GIVEUP")) and (not include_failed):
                continue
            allowed, reason = self._is_allowed(item, settings)
            if not allowed:
                if not item.completed:
                    item.status = "FILTERED"
                    item.message = reason
                    item.save()
                continue
            if item.status not in ("WAITING", "DOWNLOADING"):
                item.status = "WAITING"
                item.message = ""
                item.save()
            if QueueService.enqueue_item(item.id):
                add_count += 1
        return add_count

    def download_thread_function(self) -> None:
        while True:
            item_id = QueueService.download_queue.get() if QueueService.download_queue is not None else None
            if item_id is None:
                continue
            try:
                self.download_one(item_id)
            except Exception:
                P.logger.exception("다운로드 스레드 오류: id=%s", item_id)
            finally:
                QueueService.finish_item(item_id)

    def pick_quality(self, qualities: dict[str, str], preferred: str) -> tuple[str | None, str | None]:
        quality_orders = {
            "M50": ["M50", "M20", "M10", "M05"],
            "M20": ["M20", "M10", "M05"],
            "M10": ["M10", "M05"],
            "M05": ["M05"],
        }
        for code in quality_orders.get((preferred or "").upper(), ["M50", "M20", "M10", "M05"]):
            if code in qualities:
                return code, qualities[code]
        if qualities:
            code = sorted(qualities.keys(), reverse=True)[0]
            return code, qualities[code]
        return None, None

    def download_one(self, item_id: int) -> None:
        item = ModelEbsEpisode.get_by_id(item_id)
        if item is None or item.completed:
            return

        max_retry = max(P.ModelSetting.get_int(f"{self.name}_max_retry"), 1)
        if item.retry >= max_retry:
            item.status = "GIVEUP"
            item.message = "재시도 횟수 초과"
            item.save()
            return

        client = self.make_auth_client()
        item.status = "DOWNLOADING"
        item.message = ""
        item.save()

        try:
            info = None
            refresh_msg = ""
            for attempt in range(2):
                try:
                    info = client.resolve_play_info(
                        item.remote_program_id,
                        item.remote_episode_id,
                        item.remote_media_id,
                    )
                except requests.exceptions.Timeout:
                    if attempt == 0:
                        P.logger.warning(
                            "[ebs] resolve_play_info timeout: retry once without cookie refresh: id=%s remote=%s/%s/%s",
                            item.id,
                            item.remote_program_id,
                            item.remote_episode_id,
                            item.remote_media_id,
                        )
                        continue
                    raise
                except Exception as e:
                    if attempt == 0:
                        refreshed, refresh_msg = self.refresh_cookie_with_saved_account(force=False)
                        if refreshed:
                            client = self.make_auth_client()
                            continue
                    raise

                preferred_quality = P.ModelSetting.get("basic_quality") or "M50"
                quality_code, play_url = self.pick_quality(info.get("qualities") or {}, preferred_quality)
                needs_refresh = False
                if attempt == 0:
                    if (not quality_code) or (not play_url):
                        needs_refresh = True
                    elif (info.get("is_login") is False) and P.ModelSetting.get_bool("basic_cookie_refresh"):
                        needs_refresh = True
                if needs_refresh:
                    refreshed, refresh_msg = self.refresh_cookie_with_saved_account(force=False)
                    if refreshed:
                        client = self.make_auth_client()
                        continue
                break

            if info is None:
                raise Exception("재생 정보 확인에 실패했습니다.")
            item.is_login = "Y" if info.get("is_login") else "N"
            item.buy_state = info.get("buy_state") or ""

            preferred_quality = P.ModelSetting.get("basic_quality") or "M50"
            quality_code, play_url = self.pick_quality(info.get("qualities") or {}, preferred_quality)
            if not quality_code or not play_url:
                raise Exception("재생 가능한 화질 URL을 찾지 못했습니다.")

            item.quality_code = quality_code
            item.play_url = play_url
            item.is_preview = client.is_preview_url(play_url) or int(info.get("preview_end") or 0) > 0
            allow_preview = P.ModelSetting.get_bool(f"{self.name}_allow_preview")
            if item.is_preview and not allow_preview:
                P.logger.warning(
                    "[ebs] preview blocked: id=%s remote=%s/%s/%s is_login=%s buy_state=%s quality=%s quality_count=%s preview_end=%s show=%s",
                    item.id,
                    item.remote_program_id,
                    item.remote_episode_id,
                    item.remote_media_id,
                    item.is_login,
                    item.buy_state,
                    item.quality_code,
                    len(info.get("qualities") or {}),
                    info.get("preview_end") or 0,
                    info.get("show_url") or item.show_url,
                )
                item.status = "PREVIEW_BLOCKED"
                item.retry += 1
                item.message = "프리뷰 URL 감지. 로그인/구독 쿠키를 확인하세요."
                item.save()
                return

            save_path = ToolUtil.make_path(P.ModelSetting.get("basic_save_path"))
            pathlib.Path(save_path).mkdir(parents=True, exist_ok=True)
            final_quality_code = quality_code or (P.ModelSetting.get("basic_quality") or "M50")
            filename = item.make_filename(final_quality_code)
            output_path = pathlib.Path(save_path) / filename
            if output_path.exists() and output_path.stat().st_size > 0:
                item.filesize = output_path.stat().st_size
                item.filepath = output_path.as_posix()
                item.completed = True
                item.completed_time = datetime.datetime.now()
                item.status = "COMPLETED"
                item.message = "이미 파일이 존재합니다."
                item.save()
                return

            cookie = (P.ModelSetting.get("basic_cookie") or "").strip()
            if cookie.lower().startswith("cookie:"):
                cookie = cookie.split(":", 1)[1].strip()
            cookie = cookie.replace("\r", " ").replace("\n", " ").strip()
            headers = {
                "User-Agent": P.ModelSetting.get("basic_user_agent") or "Mozilla/5.0",
                "Referer": info.get("show_url") or item.show_url or "https://www.ebs.co.kr/tv/program?tab=vod",
            }
            if cookie:
                headers["Cookie"] = cookie

            downloader = SupportFfmpeg(
                play_url,
                filename,
                save_path=str(pathlib.Path(save_path)),
                headers=headers,
                callback_id=f"{P.package_name}_{self.name}_{item.id}",
                timeout_minute=180,
            )
            downloader.start()
            if downloader.thread is not None:
                downloader.thread.join()
            data = downloader.get_data()
            if downloader.status != SupportFfmpeg.Status.COMPLETED:
                raise Exception(
                    f"ffmpeg 다운로드 실패: {data.get('status_kor') or data.get('status_str') or downloader.status}"
                )
            if not output_path.exists() or output_path.stat().st_size <= 0:
                raise Exception("ffmpeg 다운로드가 완료되었지만 파일이 생성되지 않았습니다.")

            item.filesize = output_path.stat().st_size
            item.filepath = output_path.as_posix()
            item.completed = True
            item.completed_time = datetime.datetime.now()
            item.status = "COMPLETED"
            item.message = "다운로드 완료"
            item.save()
        except Exception as e:
            P.logger.exception("[ebs] download_one failed: id=%s, remote=%s/%s/%s", item.id, item.remote_program_id, item.remote_episode_id, item.remote_media_id)
            item.retry += 1
            item.status = "FAILED"
            item.message = str(e)
            item.save()

    def retry_failed(self) -> int:
        max_retry = max(P.ModelSetting.get_int(f"{self.name}_max_retry"), 1)
        count = 0
        for item in ModelEbsEpisode.get_failed(max_retry=max_retry):
            item.status = "PENDING"
            item.message = ""
            item.save()
            count += 1
        return count

    def reset_queue(self) -> int:
        queue_count = QueueService.reset()
        for item in ModelEbsEpisode.get_queue_states():
            item.status = "PENDING"
            item.message = "큐를 초기화했습니다."
            item.save()
        return queue_count
