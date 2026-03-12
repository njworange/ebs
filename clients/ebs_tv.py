import dataclasses
import html
import logging
import re
import time
from typing import Any
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

import requests


logger = logging.getLogger("ebs.client")

BASE_URL = "https://www.ebs.co.kr"
TV_PROGRAM_URL = f"{BASE_URL}/tv/program"
TV_PROGRAM_LIST_API = f"{BASE_URL}/tv/search/programListNew"
TV_SHOW_URL = f"{BASE_URL}/tv/show"
TV_SHOW_VOD_LIST_API = f"{BASE_URL}/tv/show/vodListNew"

LIST_ITEM_RE = re.compile(
    r"<li[^>]*>\s*<div class=\"tbl_th\">.*?<strong class=\"mainTit\">\s*"
    r"<a href=\"(?P<show_url>[^\"]+)\"[^>]*>(?P<episode_title>.*?)</a>.*?</strong>.*?"
    r"<span class=\"tbl_td col2\">(?P<release_date>[^<]+)</span>.*?"
    r"<span class=\"tbl_td col3\">\s*<a href=\"(?P<program_url>[^\"]+)\"[^>]*>(?P<program_title>.*?)</a>",
    re.S,
)
VOD_OPTION_RE = re.compile(r"var\s+vodOption\s*=\s*\{(?P<body>.*?)\};", re.S)
VOD_STATE_RE = re.compile(r"var\s+vodstate\s*=\s*\{(?P<body>.*?)\};", re.S)
VOD_LIST_LINK_RE = re.compile(
    r"selVodList\('(?P<lect_id>[^']*)','(?P<page>[^']*)','(?P<srch_type>[^']*)','(?P<srch_text>[^']*)','(?P<srch_year>[^']*)','(?P<srch_month>[^']*)',\s*'(?P<prod_id>[^']*)'\);"
)
VOD_LIST_ITEM_RE = re.compile(
    r"<li[^>]*>\s*<div class=\"pro_vod\">.*?<p class=\"tit\">\s*"
    r"<a href=\"javascript:(?P<script>[^\"]+)\"[^>]*title=\"(?P<title>.*?)\">(?P<label>.*?)<span class=\"date\">(?P<date>[^<]+)</span>",
    re.S,
)
VOD_PAGE_RE = re.compile(r"<strong>(?P<current>\d+)</strong>\s*/\s*(?P<total>\d+)")
QUALITY_RE = re.compile(
    r"\{code:\s*'(?P<code>M\d+)',\s*label:\s*'(?P<label>[^']*)',\s*src:\s*'(?P<src>[^']+)'",
    re.S,
)
PREVIEW_RANGE_RE = re.compile(r"preview:\s*\{\s*data:\s*\[\s*\{start:\s*(?P<start>\d+),\s*end:\s*(?P<end>\d+)\}", re.S)
JS_FIELD_RE = re.compile(r"(?P<key>[A-Za-z0-9_]+)\s*:\s*\"?(?P<value>[^\",\n]+)\"?")
OG_IMAGE_RE = re.compile(r'<meta\s+property="og:image"\s+content="(?P<url>[^"]+)"', re.I)
THUMBNAIL_URL_RE = re.compile(r'"thumbnailUrl"\s*:\s*"(?P<url>[^"]+)"', re.I)
KC_FORM_RE = re.compile(r'id=["\']kc-form-login["\']', re.I)
KC_FEEDBACK_RE = re.compile(r'kc-feedback-text[^>]*>\s*(?P<msg>[^<]+)\s*<', re.I)
KC_ALREADY_LOGGED_RE = re.compile(r"you are already logged in", re.I)
FORM_OPEN_RE = re.compile(r"<form\b[^>]*>", re.I)
FORM_BLOCK_RE = re.compile(r"<form\b[^>]*>.*?</form>", re.I | re.S)
INPUT_RE = re.compile(r"<input\b[^>]*>", re.I)
ATTR_RE = re.compile(r'([a-zA-Z_:][\w:.-]*)\s*=\s*("([^"]*)"|\'([^\']*)\')')


@dataclasses.dataclass
class EpisodeRow:
    remote_program_id: str
    remote_episode_id: str
    remote_media_id: str
    program_title: str
    display_title: str
    episode_no: str
    episode_title: str
    release_date: str
    show_url: str
    thumbnail: str = ""
    source_type: str = "tv_show"

    def as_dict(self) -> dict[str, Any]:
        return {
            "remote_program_id": self.remote_program_id,
            "remote_episode_id": self.remote_episode_id,
            "remote_media_id": self.remote_media_id,
            "course_id": self.remote_program_id,
            "lect_id": self.remote_episode_id,
            "step_id": self.remote_media_id,
            "program_title": self.program_title,
            "display_title": self.display_title,
            "episode_no": self.episode_no,
            "episode_title": self.episode_title,
            "release_date": self.release_date,
            "show_url": self.show_url,
            "thumbnail": self.thumbnail,
            "source_type": self.source_type,
        }


class EbsTvClient:
    def __init__(self, cookie: str = "", user_agent: str = "Mozilla/5.0", timeout: int = 15) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept-Language": "ko,en-US;q=0.9,en;q=0.8",
                "Referer": BASE_URL,
            }
        )
        self.set_cookie(cookie)

    def set_cookie(self, cookie: str) -> None:
        cookie = (cookie or "").strip()
        if cookie:
            self.session.headers["Cookie"] = cookie
        elif "Cookie" in self.session.headers:
            del self.session.headers["Cookie"]

    @staticmethod
    def get_cookie_from_browser(
        browser: str = "auto", user_agent: str = "Mozilla/5.0", timeout: int = 20
    ) -> dict[str, Any]:
        try:
            import browser_cookie3  # type: ignore
        except Exception as e:
            return {"success": False, "message": f"browser-cookie3 로드 실패: {e}", "cookie": ""}

        browser = (browser or "auto").strip().lower() or "auto"
        browser_order_all = ["chrome", "edge", "firefox", "chromium", "brave", "opera", "vivaldi"]
        browser_order = browser_order_all if browser == "auto" else [browser]
        errors: list[str] = []
        for browser_name in browser_order:
            getter = getattr(browser_cookie3, browser_name, None)
            if not callable(getter):
                errors.append(f"{browser_name}: 미지원 브라우저")
                continue
            try:
                try:
                    cookiejar = getter(domain_name="ebs.co.kr")
                except TypeError:
                    cookiejar = getter()
                cookie_header = _join_cookie_header(cookiejar)
                if not cookie_header:
                    errors.append(f"{browser_name}: ebs.co.kr 쿠키 없음")
                    continue
                client = EbsTvClient(cookie=cookie_header, user_agent=user_agent or "Mozilla/5.0", timeout=timeout)
                login_state = client.quick_login_state()
                return {
                    "success": login_state != "N",
                    "message": (
                        f"{browser_name} 브라우저에서 쿠키를 가져왔습니다. (isLogin: {login_state})"
                        if login_state != "N"
                        else f"{browser_name} 브라우저 쿠키를 읽었지만 로그인 상태가 아닙니다. (isLogin: {login_state})"
                    ),
                    "cookie": cookie_header if login_state != "N" else "",
                }
            except Exception as e:
                errors.append(f"{browser_name}: {type(e).__name__} - {e}")
        return {
            "success": False,
            "message": "브라우저에서 쿠키를 가져오지 못했습니다. " + (f"(상세: {' | '.join(errors[:4])})" if errors else ""),
            "cookie": "",
        }

    @staticmethod
    def get_cookie_from_file(path: str, user_agent: str = "Mozilla/5.0", timeout: int = 20) -> dict[str, Any]:
        path = (path or "").strip()
        if not path:
            return {"success": False, "message": "쿠키 파일 경로가 비어 있습니다.", "cookie": ""}
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                raw = f.read()
        except FileNotFoundError:
            return {"success": False, "message": f"쿠키 파일을 찾을 수 없습니다. (path: {path})", "cookie": ""}
        except Exception as e:
            return {"success": False, "message": f"쿠키 파일 읽기 실패: {type(e).__name__} - {e}", "cookie": ""}

        cookie_header = _extract_cookie_header_from_raw(raw)
        if not cookie_header:
            return {"success": False, "message": "쿠키 파일에서 ebs.co.kr 쿠키를 추출하지 못했습니다.", "cookie": ""}
        client = EbsTvClient(cookie=cookie_header, user_agent=user_agent or "Mozilla/5.0", timeout=timeout)
        login_state = client.quick_login_state()
        return {
            "success": login_state != "N",
            "message": (
                f"쿠키 파일에서 쿠키를 가져왔습니다. (isLogin: {login_state})"
                if login_state != "N"
                else f"쿠키 파일에서 쿠키를 읽었지만 로그인 상태가 아닙니다. (isLogin: {login_state})"
            ),
            "cookie": cookie_header if login_state != "N" else "",
        }

    def quick_login_state(self) -> str:
        probe_url = f"{BASE_URL}/tv/show?courseId=10207460&lectId=60696407&stepId=60058016"
        try:
            text = self.get_text(probe_url, referer=f"{TV_PROGRAM_URL}?tab=vod")
        except Exception:
            return "미검출"
        vod_state = self._parse_vod_state(text)
        return vod_state.get("isLogin", "미검출")

    @staticmethod
    def login_and_get_cookie(user_id: str, password: str, user_agent: str, timeout: int = 20) -> dict[str, Any]:
        user_id = (user_id or "").strip()
        password = password or ""
        if (not user_id) or (not password):
            return {"success": False, "message": "아이디/비밀번호를 모두 입력하세요.", "cookie": ""}

        try:
            session = requests.Session()
            session.headers.update(
                {
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "ko,en-US;q=0.9,en;q=0.8",
                    "User-Agent": user_agent or "Mozilla/5.0",
                }
            )

            login_page_url = f"{BASE_URL}/login"
            login_resp = session.get(login_page_url, timeout=timeout, allow_redirects=True, headers={"Referer": BASE_URL})
            login_page_text = login_resp.text or ""
            login_page_final = login_resp.url or login_page_url

            form_action = f"{BASE_URL}/sso/login"
            form_fields: dict[str, str] = {}
            frm_match = re.search(r'<form\b[^>]*\bid=["\']frm["\'][^>]*>(.*?)</form>', login_page_text, re.I | re.S)
            if frm_match:
                frm_html = frm_match.group(0)
                action_match = re.search(r'\baction=["\']([^"\']+)["\']', frm_html, re.I)
                if action_match:
                    form_action = urljoin(login_page_final, action_match.group(1))
                for inp_match in INPUT_RE.finditer(frm_html):
                    attrs = _parse_attrs(inp_match.group(0))
                    inp_type = (attrs.get("type") or "text").lower()
                    inp_name = (attrs.get("name") or "").strip()
                    inp_value = attrs.get("value", "")
                    if inp_type == "hidden" and inp_name:
                        form_fields[inp_name] = inp_value

            payload = dict(form_fields)
            payload["i"] = user_id
            payload["c"] = password
            payload.setdefault("r", "false")
            payload.setdefault("userId", "")
            payload.setdefault("snsSite", "")
            payload.setdefault("j_logintype", "")

            response = session.post(
                form_action,
                data=payload,
                timeout=timeout,
                allow_redirects=True,
                headers={"Referer": login_page_final, "Origin": _origin_for_url(login_page_final)},
            )

            auto_submit_tried = False
            final_url = response.url or ""
            for _ in range(15):
                current_url = response.url or ""
                current_url_lower = current_url.lower()
                if ("www.ebs.co.kr" in current_url_lower) and ("sso.ebs.co.kr" not in current_url_lower) and ("/login" not in current_url_lower):
                    break

                response_text = response.text or ""
                has_auto_submit = (
                    "document.forms[0].submit" in response_text.lower()
                    or "document.forms['form'].submit" in response_text.lower()
                    or 'onload="document.forms' in response_text.lower()
                )
                action, method, inputs, _action_raw = _extract_best_form(response_text, current_url)
                if not inputs and not has_auto_submit:
                    break

                names = {inp["name"] for inp in inputs} if inputs else set()
                has_login_fields = ("username" in names and "password" in names)
                relay_fields = {"scope", "response_type", "redirect_uri", "state", "client_id"}
                is_relay_form = any(key in names for key in relay_fields)
                action_lower = (action or "").lower()
                is_sso_action = (
                    ("sso.ebs.co.kr" in action_lower)
                    or ("openid-connect" in action_lower)
                    or ("login-actions" in action_lower)
                    or ("/sso/" in action_lower)
                )
                is_sso_page = "sso.ebs.co.kr" in current_url_lower

                if not (has_login_fields or is_relay_form or is_sso_action or is_sso_page or has_auto_submit):
                    break
                if not action and not has_auto_submit:
                    break

                post_data: dict[str, str] = {}
                if has_login_fields:
                    if has_auto_submit and (not auto_submit_tried):
                        for inp in inputs:
                            post_data[inp["name"]] = inp["value"]
                        post_data.setdefault("username", "")
                        post_data.setdefault("password", "")
                        auto_submit_tried = True
                    else:
                        for inp in inputs:
                            post_data[inp["name"]] = inp["value"]
                        post_data["username"] = user_id
                        post_data["password"] = password
                        if "credentialId" in names:
                            post_data["credentialId"] = ""
                        if "login" in names:
                            post_data["login"] = "Log In"
                else:
                    for inp in inputs:
                        post_data[inp["name"]] = inp["value"]

                submit_headers = {"Referer": current_url}
                if method == "post" or has_auto_submit:
                    origin = _origin_for_url(current_url)
                    if origin:
                        submit_headers["Origin"] = origin
                    submit_url = action or current_url
                    response = session.post(submit_url, data=post_data, timeout=timeout, allow_redirects=True, headers=submit_headers)
                else:
                    response = session.get(action or current_url, params=post_data, timeout=timeout, allow_redirects=True, headers=submit_headers)
                final_url = response.url or final_url

            response_text = response.text or ""
            already_logged_in_sso = KC_ALREADY_LOGGED_RE.search(response_text) is not None
            kc_form_present = KC_FORM_RE.search(response_text) is not None
            if kc_form_present and (not already_logged_in_sso):
                feedback = _parse_kc_feedback(response_text)
                return {
                    "success": False,
                    "message": f"SSO 로그인 단계에서 인증에 실패했습니다. ({feedback})" if feedback else "SSO 로그인 단계에서 인증에 실패했습니다.",
                    "cookie": "",
                }

            cookie_header = _join_cookie_header(session.cookies)
            has_sso_auth = any(
                c.name == "sso.authenticated" and c.value == "1"
                for c in session.cookies
                if (c.domain or "").endswith("ebs.co.kr")
            )
            has_kc_identity = any(
                c.name == "KEYCLOAK_IDENTITY"
                for c in session.cookies
                if (c.domain or "").endswith("ebs.co.kr")
            )
            if has_sso_auth and has_kc_identity and cookie_header:
                return {"success": True, "message": "로그인 성공. 쿠키를 생성했습니다.", "cookie": cookie_header}
            if has_sso_auth and cookie_header:
                return {"success": True, "message": "로그인 성공. 쿠키를 생성했습니다.", "cookie": cookie_header}

            probe_client = EbsTvClient(cookie=cookie_header, user_agent=user_agent or "Mozilla/5.0", timeout=timeout)
            login_state = probe_client.quick_login_state() if cookie_header else "N"
            if login_state == "Y" and cookie_header:
                return {"success": True, "message": "로그인 성공. 쿠키를 생성했습니다.", "cookie": cookie_header}

            if cookie_header and (not _is_sso_or_login_url(final_url)):
                return {
                    "success": True,
                    "message": "쿠키를 생성했습니다. 로그인 판별 신호가 불안정하여 쿠키 기반으로 계속 진행합니다.",
                    "cookie": cookie_header,
                }

            return {
                "success": False,
                "message": f"로그인에 실패했습니다. (최종 URL: {_safe_url_for_message(final_url)}, isLogin: {login_state})",
                "cookie": "",
            }
        except Exception as e:
            logger.exception("[LOGIN] 로그인 처리 중 예외 발생")
            return {"success": False, "message": f"로그인 처리 중 오류: {e}", "cookie": ""}

    def get_text(self, url: str, referer: str | None = None) -> str:
        headers = {}
        if referer:
            headers["Referer"] = referer
        response = self.session.get(url, timeout=self.timeout, headers=headers, allow_redirects=True)
        response.raise_for_status()
        return response.text or ""

    def collect_daily_vods(self, page: int = 1) -> list[dict[str, Any]]:
        response = self.session.post(
            TV_PROGRAM_LIST_API,
            data={
                "srchBrdc": "ING,END",
                "srchText": "",
                "srchClsfn": "",
                "pageNum": str(page),
                "mobHmpYn": "N",
                "pcMobileYn": "N",
                "srchSrt": "NEW",
                "listKind": "list",
                "tabKind": "tabVod",
                "frmWeek": "",
            },
            headers={"X-Requested-With": "XMLHttpRequest", "Referer": f"{TV_PROGRAM_URL}?tab=vod"},
            timeout=self.timeout,
        )
        response.raise_for_status()
        text = response.text or ""
        rows: list[dict[str, Any]] = []
        for match in LIST_ITEM_RE.finditer(text):
            show_url = _normalize_url(match.group("show_url") or "")
            program_url = _normalize_url(match.group("program_url") or "")
            episode_title = _strip_html_preserve_text(match.group("episode_title") or "")
            program_title = _strip_html_preserve_text(match.group("program_title") or "")
            release_date = _strip_html_preserve_text(match.group("release_date") or "")
            source_type = self._classify_source(show_url)
            remote_program_id, remote_episode_id, remote_media_id = self._extract_ids_from_url(show_url)
            episode_no = _extract_episode_no(episode_title)
            display_title = program_title
            rows.append(
                {
                    "remote_program_id": remote_program_id,
                    "remote_episode_id": remote_episode_id,
                    "remote_media_id": remote_media_id,
                    "course_id": remote_program_id,
                    "lect_id": remote_episode_id,
                    "step_id": remote_media_id,
                    "program_title": program_title,
                    "display_title": display_title,
                    "episode_no": episode_no,
                    "episode_title": episode_title,
                    "release_date": release_date,
                    "show_url": show_url,
                    "program_url": program_url,
                    "thumbnail": self._extract_inline_thumbnail(match.group(0) or ""),
                    "source_type": source_type,
                }
            )
        return rows

    def fetch_show_metadata(
        self, remote_program_id: str, remote_episode_id: str, remote_media_id: str
    ) -> dict[str, str]:
        show_url = self.build_show_url(remote_program_id, remote_episode_id, remote_media_id)
        text = self.get_text(show_url, referer=f"{TV_PROGRAM_URL}?tab=vod")
        return {
            "thumbnail": self._extract_thumbnail_from_text(text),
            "episode_no": self._extract_episode_no_from_text(text),
            "display_title": self._extract_display_title_from_text(text),
        }

    def analyze_program_url(self, url_or_code: str, step_id: str | None = None) -> dict[str, Any]:
        remote_program_id, remote_episode_id, remote_media_id = self._normalize_input(url_or_code)
        if step_id:
            remote_media_id = step_id.strip()
        if remote_program_id and not remote_episode_id:
            latest = self._find_latest_row(remote_program_id, remote_media_id)
            if latest:
                remote_episode_id = latest.get("remote_episode_id") or remote_episode_id
                remote_media_id = latest.get("remote_media_id") or remote_media_id
        if not remote_program_id:
            return {
                "success": False,
                "message": "courseId를 찾지 못했습니다.",
                "data": {"input": url_or_code, "episodes": [], "seasons": [], "debug": {"source": "tv_show"}},
            }

        show_html = self._fetch_show_page(remote_program_id, remote_episode_id, remote_media_id)
        option = self._parse_vod_option(show_html)
        remote_program_id = option.get("courseId") or remote_program_id
        remote_media_id = option.get("stepId") or remote_media_id
        remote_episode_id = option.get("lectId") or remote_episode_id
        program_title = option.get("courseNm") or option.get("stepNm") or remote_program_id
        display_title = option.get("stepNm") or program_title
        prod_id = option.get("prodId") or ""
        default_thumbnail = self._extract_thumbnail_from_text(show_html)

        if not remote_episode_id:
            return {
                "success": False,
                "message": "기준 에피소드(lectId)를 찾지 못했습니다.",
                "data": {
                    "input": url_or_code,
                    "course_id": remote_program_id,
                    "step_id": remote_media_id,
                    "program_title": program_title,
                    "display_title": display_title,
                    "seasons": [{"step_id": remote_media_id, "name": display_title}],
                    "episodes": [],
                    "debug": {"source": "tv_show", "prod_id": prod_id},
                },
            }

        pages = self._collect_show_episode_pages(
            remote_program_id=remote_program_id,
            remote_episode_id=remote_episode_id,
            remote_media_id=remote_media_id,
            display_title=display_title,
            prod_id=prod_id,
        )
        episodes = self._assign_episode_numbers(pages)
        for episode in episodes:
            if not episode.thumbnail:
                episode.thumbnail = default_thumbnail
        return {
            "success": len(episodes) > 0,
            "message": f"에피소드 {len(episodes)}개 분석 완료" if episodes else "에피소드 목록을 찾지 못했습니다.",
            "data": {
                "input": url_or_code,
                "course_id": remote_program_id,
                "lect_id": remote_episode_id,
                "step_id": remote_media_id,
                "program_title": program_title,
                "display_title": display_title,
                "seasons": [{"step_id": remote_media_id, "name": display_title}],
                "episodes": [ep.as_dict() for ep in episodes],
                "debug": {
                    "source": "tv_show",
                    "prod_id": prod_id,
                    "episode_count": len(episodes),
                },
            },
        }

    def _assign_episode_numbers(self, episodes: list[EpisodeRow]) -> list[EpisodeRow]:
        if not episodes:
            return episodes
        if any((ep.episode_no or "").strip() for ep in episodes):
            return episodes
        ordered = sorted(episodes, key=lambda ep: (_date_key(ep.release_date), ep.remote_episode_id))
        for idx, episode in enumerate(ordered, start=1):
            episode.episode_no = str(idx)
        episodes.sort(key=lambda ep: (_date_key(ep.release_date), ep.remote_episode_id), reverse=True)
        return episodes

    def resolve_play_info(
        self, remote_program_id: str, remote_episode_id: str, remote_media_id: str
    ) -> dict[str, Any]:
        show_url = self.build_show_url(remote_program_id, remote_episode_id, remote_media_id)
        text = self.get_text(show_url, referer=f"{TV_PROGRAM_URL}?tab=vod")
        vod_state = self._parse_vod_state(text)
        qualities = {}
        for match in QUALITY_RE.finditer(text):
            code = (match.group("code") or "").strip()
            src = (match.group("src") or "").strip()
            if code and src:
                qualities[code] = html.unescape(src)
        preview_match = PREVIEW_RANGE_RE.search(text)
        preview_end = int(preview_match.group("end")) if preview_match else 0
        return {
            "is_login": vod_state.get("isLogin", "N") == "Y",
            "buy_state": vod_state.get("buyState", ""),
            "qualities": qualities,
            "subtitles": {},
            "show_url": show_url,
            "preview_end": preview_end,
        }

    def get_episode_play_info(
        self, remote_program_id: str, remote_episode_id: str, remote_media_id: str
    ) -> dict[str, Any]:
        return self.resolve_play_info(remote_program_id, remote_episode_id, remote_media_id)

    def download_binary(self, url: str, filepath: str, referer: str | None = None) -> None:
        headers = {}
        if referer:
            headers["Referer"] = referer
        response = self.session.get(url, timeout=self.timeout, headers=headers, stream=True)
        response.raise_for_status()
        with open(filepath, "wb") as handle:
            for chunk in response.iter_content(chunk_size=65536):
                if chunk:
                    handle.write(chunk)

    def build_show_url(self, remote_program_id: str, remote_episode_id: str, remote_media_id: str) -> str:
        query = {"courseId": remote_program_id}
        if remote_episode_id:
            query["lectId"] = remote_episode_id
        if remote_media_id:
            query["stepId"] = remote_media_id
        return f"{TV_SHOW_URL}?{urlencode(query)}"

    @staticmethod
    def is_preview_url(url: str) -> bool:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        try:
            end_val = int((query.get("end") or ["0"])[0])
        except Exception:
            end_val = 0
        return end_val > 0 and end_val <= 300

    def _fetch_show_page(self, remote_program_id: str, remote_episode_id: str, remote_media_id: str) -> str:
        show_url = self.build_show_url(remote_program_id, remote_episode_id, remote_media_id)
        return self.get_text(show_url, referer=f"{TV_PROGRAM_URL}?tab=vod")

    def _collect_show_episode_pages(
        self,
        remote_program_id: str,
        remote_episode_id: str,
        remote_media_id: str,
        display_title: str,
        prod_id: str,
        max_pages: int = 300,
    ) -> list[EpisodeRow]:
        first_page = self._post_show_vod_list(
            remote_program_id=remote_program_id,
            remote_episode_id=remote_episode_id,
            remote_media_id=remote_media_id,
            page_num=1,
            prod_id=prod_id,
        )
        total_pages = self._extract_total_pages(first_page)
        total_pages = max(1, min(total_pages, max_pages))

        episodes: list[EpisodeRow] = []
        seen = set()
        for page_num in range(1, total_pages + 1):
            text = first_page if page_num == 1 else self._post_show_vod_list(
                remote_program_id=remote_program_id,
                remote_episode_id=remote_episode_id,
                remote_media_id=remote_media_id,
                page_num=page_num,
                prod_id=prod_id,
            )
            page_rows = 0
            for match in VOD_LIST_ITEM_RE.finditer(text):
                script = match.group("script") or ""
                link_match = VOD_LIST_LINK_RE.search(script)
                if not link_match:
                    continue
                item_episode_id = (link_match.group("lect_id") or "").strip()
                if not item_episode_id:
                    continue
                key = (remote_program_id, item_episode_id, remote_media_id)
                if key in seen:
                    continue
                seen.add(key)
                page_rows += 1
                raw_title = match.group("title") or match.group("label") or ""
                episode_title = _strip_html_preserve_text(raw_title)
                release_date = _strip_html_preserve_text(match.group("date") or "")
                episode_no = _extract_episode_no(episode_title)
                thumb = self._extract_inline_thumbnail(match.group(0) or "")
                episodes.append(
                    EpisodeRow(
                        remote_program_id=remote_program_id,
                        remote_episode_id=item_episode_id,
                        remote_media_id=remote_media_id,
                        program_title=display_title,
                        display_title=display_title,
                        episode_no=episode_no,
                        episode_title=episode_title,
                        release_date=release_date,
                        show_url=self.build_show_url(remote_program_id, item_episode_id, remote_media_id),
                        thumbnail=thumb,
                    )
                )
            if page_rows == 0:
                break
        episodes.sort(key=lambda ep: (_date_key(ep.release_date), ep.remote_episode_id), reverse=True)
        return episodes

    def _post_show_vod_list(
        self,
        remote_program_id: str,
        remote_episode_id: str,
        remote_media_id: str,
        page_num: int,
        prod_id: str,
    ) -> str:
        response = self.session.post(
            TV_SHOW_VOD_LIST_API,
            data={
                "courseId": remote_program_id,
                "stepId": remote_media_id,
                "lectId": remote_episode_id,
                "vodStepNm": "",
                "srchType": "0",
                "srchText": "",
                "srchYear": "",
                "srchMonth": "",
                "pageNum": str(page_num),
                "pageMode": "v",
                "vodProdId": prod_id,
            },
            headers={"X-Requested-With": "XMLHttpRequest", "Referer": self.build_show_url(remote_program_id, remote_episode_id, remote_media_id)},
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.text or ""

    def _normalize_input(self, value: str) -> tuple[str, str, str]:
        value = (value or "").strip()
        if value == "":
            return "", "", ""

        if value.startswith("http://") or value.startswith("https://"):
            return self._extract_ids_from_url(value)

        program_match = re.search(r"courseId=([A-Z0-9]+)", value, re.I)
        episode_match = re.search(r"lectId=([A-Z0-9]+)", value, re.I)
        media_match = re.search(r"stepId=([A-Z0-9]+)", value, re.I)
        if program_match:
            return (
                program_match.group(1),
                episode_match.group(1) if episode_match else "",
                media_match.group(1) if media_match else "",
            )
        return value, "", ""

    def _extract_ids_from_url(self, url: str) -> tuple[str, str, str]:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        return (
            (query.get("courseId") or [""])[0],
            (query.get("lectId") or [""])[0],
            (query.get("stepId") or [""])[0],
        )

    def _find_latest_row(self, remote_program_id: str, remote_media_id: str, max_pages: int = 5) -> dict[str, Any] | None:
        for page in range(1, max_pages + 1):
            rows = self.collect_daily_vods(page=page)
            if not rows:
                break
            for row in rows:
                if row.get("remote_program_id") != remote_program_id:
                    continue
                if remote_media_id and row.get("remote_media_id") != remote_media_id:
                    continue
                if row.get("source_type") == "tv_show":
                    return row
        return None

    def _parse_vod_option(self, text: str) -> dict[str, str]:
        match = VOD_OPTION_RE.search(text)
        if not match:
            return {}
        body = match.group("body") or ""
        result = {}
        for field in JS_FIELD_RE.finditer(body):
            result[field.group("key")] = html.unescape(field.group("value") or "")
        return result

    def _parse_vod_state(self, text: str) -> dict[str, str]:
        match = VOD_STATE_RE.search(text)
        if not match:
            return {}
        body = match.group("body") or ""
        result = {}
        for field in JS_FIELD_RE.finditer(body):
            result[field.group("key")] = html.unescape(field.group("value") or "")
        return result

    def _extract_total_pages(self, text: str) -> int:
        match = VOD_PAGE_RE.search(text or "")
        if not match:
            return 1
        try:
            return int(match.group("total"))
        except Exception:
            return 1

    def _extract_thumbnail_from_text(self, text: str) -> str:
        for regex in (THUMBNAIL_URL_RE, OG_IMAGE_RE):
            match = regex.search(text or "")
            if match:
                return _normalize_url(match.group("url") or "")
        share_match = re.search(r"fn_Share\([^\n]+,'(?P<url>https?://[^']+\.(?:png|jpg|jpeg|gif))'", text or "", re.I)
        if share_match:
            return _normalize_url(share_match.group("url") or "")
        return ""

    def _extract_inline_thumbnail(self, text: str) -> str:
        img_match = re.search(r'<img[^>]+src="(?P<url>https?://[^"]+)"', text or "", re.I)
        if img_match:
            return _normalize_url(img_match.group("url") or "")
        return ""

    def _extract_episode_no_from_text(self, text: str) -> str:
        for pattern in [
            r"<p class=\"view\">\s*(?P<num>\d+)화\b",
            r"<strong[^>]*>\s*(?P<num>\d+)화\b",
            r"<title>[^<]*?(?P<num>\d+)화",
        ]:
            match = re.search(pattern, text or "", re.I)
            if match:
                return match.group("num")
        return ""

    def _extract_display_title_from_text(self, text: str) -> str:
        match = re.search(r"<meta\s+property=\"og:title\"\s+content=\"(?P<title>[^\"]+)\"", text or "", re.I)
        if match:
            title = html.unescape(match.group("title") or "")
            title = title.split("/")[0].strip()
            return title
        return ""

    def _classify_source(self, url: str) -> str:
        if not url:
            return "unknown"
        parsed = urlparse(url)
        host = (parsed.netloc or "").lower()
        path = parsed.path or ""
        if host.endswith("www.ebs.co.kr") and path.startswith("/tv/show"):
            return "tv_show"
        if host.endswith("news.ebs.co.kr"):
            return "news"
        if host.endswith("anikids.ebs.co.kr"):
            return "anikids"
        if host.endswith("ebs.co.kr"):
            return "ebs_external"
        return "external"


def _normalize_url(url: str) -> str:
    return urljoin(BASE_URL, html.unescape(url or "").strip())


def _safe_url_for_message(url: str) -> str:
    try:
        parsed = urlparse(url or "")
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    except Exception:
        return url or ""


def _origin_for_url(url: str) -> str:
    try:
        parsed = urlparse(url or "")
        if not parsed.scheme or not parsed.netloc:
            return ""
        return f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        return ""


def _is_sso_or_login_url(url: str) -> bool:
    lower = (url or "").lower()
    return ("sso.ebs.co.kr" in lower) or ("/login" in lower)


def _parse_kc_feedback(text: str) -> str:
    if not text:
        return ""
    match = KC_FEEDBACK_RE.search(text)
    if not match:
        return ""
    msg = (match.group("msg") or "").strip()
    low = msg.lower()
    if "invalid username or password" in low:
        return "아이디 또는 비밀번호가 올바르지 않습니다."
    if "account is disabled" in low:
        return "계정이 비활성화 상태입니다."
    return msg


def _parse_attrs(tag: str) -> dict[str, str]:
    attrs: dict[str, str] = {}
    for match in ATTR_RE.finditer(tag):
        key = match.group(1).lower()
        val = match.group(3) if match.group(3) is not None else match.group(4)
        attrs[key] = html.unescape(val or "")
    return attrs


def _parse_form_block(form_html: str, base_url: str) -> tuple[str | None, str | None, list[dict[str, str]], str, str]:
    form_open = FORM_OPEN_RE.search(form_html)
    if not form_open:
        return None, None, [], "", ""
    form_attrs = _parse_attrs(form_open.group(0))
    action_raw = form_attrs.get("action", "").strip()
    method = (form_attrs.get("method", "get") or "get").strip().lower()
    action = urljoin(base_url, action_raw) if action_raw else ""
    form_id = (form_attrs.get("id") or "").strip().lower()
    inputs: list[dict[str, str]] = []
    for input_match in INPUT_RE.finditer(form_html):
        attrs = _parse_attrs(input_match.group(0))
        name = (attrs.get("name") or "").strip()
        if not name:
            continue
        inputs.append({
            "name": name,
            "value": attrs.get("value", ""),
            "type": (attrs.get("type", "text") or "text").lower(),
        })
    return action, method, inputs, action_raw, form_id


def _score_form_candidate(action: str | None, inputs: list[dict[str, str]], form_id: str) -> int:
    names = {inp.get("name", "") for inp in inputs}
    action_lower = (action or "").lower()
    score = 0
    if form_id == "kc-form-login":
        score += 100
    if ("username" in names) and ("password" in names):
        score += 80
    relay_fields = {"scope", "response_type", "redirect_uri", "state", "client_id"}
    if any(key in names for key in relay_fields):
        score += 50
    if "openid-connect/auth" in action_lower:
        score += 50
    if "login-actions/authenticate" in action_lower:
        score += 50
    if action:
        score += 5
    return score


def _extract_best_form(text: str, base_url: str) -> tuple[str | None, str | None, list[dict[str, str]], str]:
    best: tuple[str | None, str | None, list[dict[str, str]], str, int] | None = None
    for match in FORM_BLOCK_RE.finditer(text):
        action, method, inputs, action_raw, form_id = _parse_form_block(match.group(0), base_url)
        score = _score_form_candidate(action, inputs, form_id)
        if best is None or score > best[4]:
            best = (action, method, inputs, action_raw, score)
    if best is None:
        form_open = FORM_OPEN_RE.search(text)
        if not form_open:
            return None, None, [], ""
        form_html = text[form_open.start():]
        action, method, inputs, action_raw, _ = _parse_form_block(form_html, base_url)
        return action, method, inputs, action_raw
    return best[0], best[1], best[2], best[3]


def _join_cookie_header(cookiejar: Any) -> str:
    values: dict[str, str] = {}
    for cookie in cookiejar:
        domain = (cookie.domain or "").lstrip(".")
        if not domain.endswith("ebs.co.kr"):
            continue
        values[cookie.name] = cookie.value or ""
    return "; ".join(f"{name}={value}" for name, value in values.items())


def _extract_cookie_header_from_raw(raw: str) -> str:
    raw_stripped = (raw or "").strip()
    if not raw_stripped:
        return ""
    if ("# Netscape HTTP Cookie File" in raw_stripped) or ("\t" in raw_stripped):
        values: dict[str, str] = {}
        now = int(time.time())
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            if line.startswith("#HttpOnly_"):
                line = line[1:]
            elif line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < 7:
                continue
            domain = (parts[0] or "").strip().lstrip(".")
            if domain.startswith("HttpOnly_"):
                domain = domain[len("HttpOnly_") :]
            if not domain.endswith("ebs.co.kr"):
                continue
            expiry_raw = (parts[4] or "").strip()
            try:
                expiry = int(expiry_raw) if expiry_raw else 0
            except Exception:
                expiry = 0
            if expiry and expiry < now:
                continue
            name = (parts[5] or "").strip()
            value = parts[6] if len(parts) >= 7 else ""
            if name:
                values[name] = value
        return "; ".join(f"{k}={v}" for k, v in values.items())
    if raw_stripped.lower().startswith("cookie:"):
        raw_stripped = raw_stripped.split(":", 1)[1].strip()
    return raw_stripped


def _strip_html_preserve_text(text: str) -> str:
    text = html.unescape(text or "")
    text = re.sub(r"<span class=\"date\">.*?</span>", "", text, flags=re.S)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _extract_episode_no(title: str) -> str:
    title = html.unescape(title or "")
    match = re.match(r"\s*(\d+)\s*(?:회|화|편)\b", title)
    if match:
        return match.group(1)
    match = re.match(r"\s*제\s*(\d+)\s*(?:회|화|편)", title)
    if match:
        return match.group(1)
    return ""


def _date_key(value: str) -> tuple[int, int, int]:
    match = re.search(r"(\d{4})\.(\d{2})\.(\d{2})", value or "")
    if not match:
        return (0, 0, 0)
    return (int(match.group(1)), int(match.group(2)), int(match.group(3)))
