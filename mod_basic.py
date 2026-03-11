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
                        cookie=(P.ModelSetting.get(f"{self.name}_cookie") or "").strip(),
                        user_agent=P.ModelSetting.get(f"{self.name}_user_agent") or "Mozilla/5.0",
                    )
                    result = client.analyze_program_url(url_or_code)
                    ret["msg"] = result.get("message") or "분석 완료"
                    ret["data"] = result.get("data") or {}
                    if not result.get("success"):
                        ret["ret"] = "warning"
                    else:
                        self.previous_analyze = result.get("data") or {}
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
            case _:
                ret["ret"] = "warning"
                ret["msg"] = f"지원하지 않는 명령: {command}"
        return flask.jsonify(ret)
