import json
import os

import flask

from plugin.create_plugin import PluginBase
from plugin.logic_module_base import PluginModuleBase

from .clients import EbsTvClient
from .setup import P

name = "basic"


class ModuleBasic(PluginModuleBase):
    def __init__(self, P: PluginBase) -> None:
        super(ModuleBasic, self).__init__(P, "setting")
        self.name = name
        self.db_default = {
            f"{self.name}_save_path": "{PATH_DATA}" + os.sep + "download",
            f"{self.name}_quality": "M50",
            f"{self.name}_user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/132.0.0.0 Safari/537.36"
            ),
            f"{self.name}_cookie": "",
            f"{self.name}_account_id": "",
            f"{self.name}_account_pw": "",
            f"{self.name}_cookie_refresh": "True",
            f"{self.name}_cookie_browser": "auto",
            f"{self.name}_cookie_file": "",
            f"{self.name}_recent_url": "",
        }
        self.previous_analyze = None

    def process_menu(self, page_name: str, req: flask.Request) -> flask.Response:
        arg = P.ModelSetting.to_dict()
        if page_name == "download":
            arg["url_or_code"] = (req.args.get("code") or "").strip() or P.ModelSetting.get(
                f"{self.name}_recent_url"
            )
        return flask.render_template(f"{P.package_name}_{name}_{page_name}.html", arg=arg)

    def process_command(
        self, command: str, arg1: str, arg2: str, arg3: str, req: flask.Request
    ) -> flask.Response:
        ret: dict[str, object] = {"ret": "success"}
        match command:
            case "analyze_url":
                url_or_code = (arg1 or "").strip()
                if not url_or_code:
                    ret["ret"] = "warning"
                    ret["msg"] = "URL 또는 코드를 입력하세요."
                else:
                    client = EbsTvClient(
                        cookie="",
                        user_agent=P.ModelSetting.get(f"{self.name}_user_agent") or "Mozilla/5.0",
                    )
                    result = client.analyze_program_url(url_or_code, step_id=(arg2 or "").strip() or None)
                    ret["msg"] = result.get("message") or "분석 완료"
                    data = result.get("data") or {}
                    episodes = data.get("episodes") or []
                    if isinstance(episodes, list):
                        self.annotate_episodes(episodes)
                    ret["data"] = data
                    if not result.get("success"):
                        ret["ret"] = "warning"
                    else:
                        self.previous_analyze = data
                        P.ModelSetting.set(f"{self.name}_recent_url", url_or_code)
            case "download_manual":
                from .models import ModelEbsEpisode
                from .queue_service import QueueService

                try:
                    payload = json.loads(arg1 or "[]")
                except Exception:
                    return flask.jsonify({"ret": "warning", "msg": "요청 데이터(JSON) 파싱 실패"})

                episodes = payload if isinstance(payload, list) else [payload] if isinstance(payload, dict) else []
                if not episodes:
                    return flask.jsonify({"ret": "warning", "msg": "선택된 항목이 없습니다."})

                added = 0
                for ep in episodes:
                    if not isinstance(ep, dict):
                        continue
                    remote_program_id = (ep.get("course_id") or ep.get("remote_program_id") or "").strip()
                    remote_episode_id = (ep.get("lect_id") or ep.get("remote_episode_id") or "").strip()
                    remote_media_id = (ep.get("step_id") or ep.get("remote_media_id") or "").strip()
                    if (not remote_program_id) or (not remote_episode_id) or (not remote_media_id):
                        continue
                    item = ModelEbsEpisode.get_by_keys(remote_program_id, remote_episode_id, remote_media_id)
                    if item and item.completed:
                        continue
                    if not item:
                        item = ModelEbsEpisode(remote_program_id, remote_episode_id, remote_media_id)
                    item.set_info(
                        program_title=(ep.get("program_title") or remote_program_id),
                        display_title=(ep.get("display_title") or ep.get("program_title") or remote_program_id),
                        episode_no=(ep.get("episode_no") or ""),
                        episode_title=(ep.get("episode_title") or ""),
                        release_date=(ep.get("release_date") or ""),
                        show_url=(ep.get("show_url") or ""),
                        thumbnail=(ep.get("thumbnail") or ""),
                        source_type=(ep.get("source_type") or "tv_show"),
                    )
                    item.completed = False
                    item.status = "WAITING"
                    item.message = ""
                    item.save()
                    QueueService.enqueue_item(item.id)
                    added += 1
                ret["msg"] = f"{added}개를 다운로드 큐에 추가했습니다."
            case "login_with_account":
                user_id = (arg1 or "").strip()
                password = arg2 or ""
                user_agent = P.ModelSetting.get(f"{self.name}_user_agent") or "Mozilla/5.0"
                result = EbsTvClient.login_and_get_cookie(user_id=user_id, password=password, user_agent=user_agent)
                ret["msg"] = result.get("message") or "로그인 처리 완료"
                if result.get("success") and result.get("cookie"):
                    P.ModelSetting.set(f"{self.name}_cookie", result.get("cookie"))
                    P.ModelSetting.set(f"{self.name}_account_id", user_id)
                    P.ModelSetting.set(f"{self.name}_account_pw", password)
                    ret["cookie"] = result.get("cookie")
                else:
                    ret["ret"] = "warning"
            case "refresh_cookie_saved":
                ok, msg = self.refresh_cookie_with_saved_account(force=True)
                if ok:
                    ret["msg"] = msg
                    ret["cookie"] = P.ModelSetting.get(f"{self.name}_cookie") or ""
                else:
                    ret["ret"] = "warning"
                    ret["msg"] = msg
            case "get_cookie_browser":
                browser = (arg1 or P.ModelSetting.get(f"{self.name}_cookie_browser") or "auto").strip() or "auto"
                user_agent = P.ModelSetting.get(f"{self.name}_user_agent") or "Mozilla/5.0"
                result = EbsTvClient.get_cookie_from_browser(browser=browser, user_agent=user_agent)
                ret["msg"] = result.get("message") or "브라우저 쿠키 처리 완료"
                if result.get("success") and result.get("cookie"):
                    P.ModelSetting.set(f"{self.name}_cookie", result.get("cookie"))
                    P.ModelSetting.set(f"{self.name}_cookie_browser", browser)
                    ret["cookie"] = result.get("cookie")
                else:
                    ret["ret"] = "warning"
            case "get_cookie_file":
                path = (arg1 or P.ModelSetting.get(f"{self.name}_cookie_file") or "").strip()
                user_agent = P.ModelSetting.get(f"{self.name}_user_agent") or "Mozilla/5.0"
                result = EbsTvClient.get_cookie_from_file(path=path, user_agent=user_agent)
                ret["msg"] = result.get("message") or "쿠키 파일 처리 완료"
                if result.get("success") and result.get("cookie"):
                    P.ModelSetting.set(f"{self.name}_cookie", result.get("cookie"))
                    P.ModelSetting.set(f"{self.name}_cookie_file", path)
                    ret["cookie"] = result.get("cookie")
                else:
                    ret["ret"] = "warning"
            case _:
                ret["ret"] = "warning"
                ret["msg"] = f"지원하지 않는 명령: {command}"
        return flask.jsonify(ret)

    def refresh_cookie_with_saved_account(self, force: bool = False) -> tuple[bool, str]:
        if (not force) and (not P.ModelSetting.get_bool(f"{self.name}_cookie_refresh")):
            return False, "자동 쿠키 갱신이 꺼져 있습니다."
        user_id = (P.ModelSetting.get(f"{self.name}_account_id") or "").strip()
        password = P.ModelSetting.get(f"{self.name}_account_pw") or ""
        if (not user_id) or (not password):
            return False, "자동 갱신용 계정(ID/PW)이 저장되어 있지 않습니다."
        user_agent = P.ModelSetting.get(f"{self.name}_user_agent") or "Mozilla/5.0"
        result = EbsTvClient.login_and_get_cookie(user_id=user_id, password=password, user_agent=user_agent)
        if result.get("success") and result.get("cookie"):
            P.ModelSetting.set(f"{self.name}_cookie", result.get("cookie"))
            return True, "저장된 계정으로 쿠키를 갱신했습니다."
        return False, result.get("message") or "쿠키 갱신 실패"

    def annotate_episodes(self, episodes: list[dict]) -> None:
        from .models import ModelEbsEpisode

        for ep in episodes:
            if not isinstance(ep, dict):
                continue
            remote_program_id = (ep.get("course_id") or ep.get("remote_program_id") or "").strip()
            remote_episode_id = (ep.get("lect_id") or ep.get("remote_episode_id") or "").strip()
            remote_media_id = (ep.get("step_id") or ep.get("remote_media_id") or "").strip()
            if (not remote_program_id) or (not remote_episode_id) or (not remote_media_id):
                ep["local_exists"] = False
                ep["local_status"] = ""
                ep["local_message"] = ""
                ep["local_completed"] = False
                continue
            item = ModelEbsEpisode.get_by_keys(remote_program_id, remote_episode_id, remote_media_id)
            if not item:
                ep["local_exists"] = False
                ep["local_status"] = ""
                ep["local_message"] = ""
                ep["local_completed"] = False
                continue
            ep["local_exists"] = True
            ep["local_status"] = item.status or ("COMPLETED" if item.completed else "")
            ep["local_message"] = item.message or ""
            ep["local_completed"] = bool(item.completed)
