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
ANIKIDS_BASE_URL = "https://anikids.ebs.co.kr"
TV_PROGRAM_URL = f"{BASE_URL}/tv/program"
TV_PROGRAM_LIST_API = f"{BASE_URL}/tv/search/programListNew"
TV_SHOW_URL = f"{BASE_URL}/tv/show"
TV_SHOW_VOD_LIST_API = f"{BASE_URL}/tv/show/vodListNew"
TV_SHOW_LOGIN_PROBE_URL = f"{BASE_URL}/tv/show?courseId=10207460&lectId=60696407&stepId=60058016"
CLASSE_DETAIL_LOGIN_PROBE_URL = (
    "https://classe.ebs.co.kr/classe/detail/show?"
    "siteCd=CL&prodId=452564&courseId=10207460&stepId=60058016&lectId=60696453&clsfn_syst_id=40009039"
)

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
    r"\{\s*code\s*:\s*['\"]?(?P<code>M\d+)['\"]?\s*,\s*label\s*:\s*['\"]?(?P<label>[^,'\"\}\]]*)['\"]?\s*,\s*src\s*:\s*['\"](?P<src>[^'\"]+)['\"]",
    re.S,
)
PREVIEW_RANGE_RE = re.compile(r"preview:\s*\{\s*data:\s*\[\s*\{start:\s*(?P<start>\d+),\s*end:\s*(?P<end>\d+)\}", re.S)
JS_FIELD_RE = re.compile(r"(?P<key>[A-Za-z0-9_]+)\s*:\s*\"?(?P<value>[^\",\n]+)\"?")
OG_IMAGE_RE = re.compile(r'<meta\s+property="og:image"\s+content="(?P<url>[^"]+)"', re.I)
THUMBNAIL_URL_RE = re.compile(r'"thumbnailUrl"\s*:\s*"(?P<url>[^"]+)"', re.I)
SOURCE_ARRAY_RE = re.compile(r"source\s*=\s*\[(?P<body>.*?)\]\s*;", re.S)
OG_URL_RE = re.compile(r'<meta\s+property="og:url"\s+content="(?P<url>[^"]+)"', re.I)
CONTENT_URL_RE = re.compile(r'"contentUrl"\s*:\s*"(?P<url>[^"]+)"', re.I)
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
            for chunk in cookie.split(";"):
                piece = chunk.strip()
                if not piece or ("=" not in piece):
                    continue
                name, value = piece.split("=", 1)
                name = name.strip()
                if not name:
                    continue
                self.session.cookies.set(name, value.strip(), domain=".ebs.co.kr")
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
                    "success": login_state == "Y",
                    "message": (
                        f"{browser_name} 브라우저에서 쿠키를 가져왔습니다. (isLogin: {login_state})"
                        if login_state != "N"
                        else f"{browser_name} 브라우저 쿠키를 읽었지만 로그인 상태가 아닙니다. (isLogin: {login_state})"
                    ),
                    "cookie": cookie_header if login_state == "Y" else "",
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
            "success": login_state == "Y",
            "message": (
                f"쿠키 파일에서 쿠키를 가져왔습니다. (isLogin: {login_state})"
                if login_state != "N"
                else f"쿠키 파일에서 쿠키를 읽었지만 로그인 상태가 아닙니다. (isLogin: {login_state})"
            ),
            "cookie": cookie_header if login_state == "Y" else "",
        }

    def quick_login_state(self) -> str:
        try:
            login_state, _final_url = self._probe_login_state(self.session, self.timeout)
            return login_state
        except Exception:
            return "미검출"

    @staticmethod
    def login_and_get_cookie(user_id: str, password: str, user_agent: str, timeout: int = 45) -> dict[str, Any]:
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

            return_url = TV_SHOW_LOGIN_PROBE_URL
            login_page_url = f"{BASE_URL}/login?{urlencode({'returnUrl': return_url, 'j_returnurl': return_url})}"
            login_resp = EbsTvClient._safe_session_get(
                session,
                login_page_url,
                headers={"Referer": BASE_URL},
                timeout=(10, 15),
                retries=2,
            )
            login_page_text = login_resp.text or ""
            login_page_final = login_resp.url or login_page_url

            form_action = ""
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
            if not form_action:
                return {"success": False, "message": "로그인 폼 action을 찾지 못했습니다.", "cookie": ""}

            payload = dict(form_fields)
            payload["i"] = user_id
            payload["c"] = password
            payload.setdefault("r", "false")
            payload.setdefault("userId", "")
            payload.setdefault("snsSite", "")
            payload.setdefault("j_logintype", "")

            response = EbsTvClient._safe_session_post(
                session,
                form_action,
                data=payload,
                timeout=(15, max(timeout, 45)),
                retries=2,
                headers={"Referer": login_page_final, "Origin": _origin_for_url(login_page_final)},
            )

            auto_submit_tried = False
            final_url = response.url or ""
            for _ in range(15):
                current_url = response.url or ""
                current_url_lower = current_url.lower()
                if (("anikids.ebs.co.kr" in current_url_lower) or ("www.ebs.co.kr" in current_url_lower)) and ("sso.ebs.co.kr" not in current_url_lower) and ("/login" not in current_url_lower):
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
                    response = EbsTvClient._safe_session_post(
                        session,
                        submit_url,
                        data=post_data,
                        timeout=(15, max(timeout, 45)),
                        retries=2,
                        headers=submit_headers,
                    )
                else:
                    response = EbsTvClient._safe_session_get(
                        session,
                        action or current_url,
                        params=post_data,
                        headers=submit_headers,
                        timeout=(5, 15),
                        retries=2,
                    )
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
            login_state = "N"
            if cookie_header:
                try:
                    login_state, final_url = EbsTvClient(cookie=cookie_header, user_agent=user_agent or "Mozilla/5.0", timeout=timeout)._probe_login_state(
                        session,
                        timeout,
                    )
                    cookie_header = _join_cookie_header(session.cookies)
                except Exception:
                    login_state = "미검출"
            if login_state == "Y" and cookie_header:
                return {"success": True, "message": "로그인 성공. 쿠키를 생성했습니다.", "cookie": cookie_header}

            return {
                "success": False,
                "message": (
                    f"로그인에 실패했습니다. (최종 URL: {_safe_url_for_message(final_url)}, "
                    f"isLogin: {login_state}, sso.authenticated: {'Y' if has_sso_auth else 'N'}, "
                    f"KEYCLOAK_IDENTITY: {'Y' if has_kc_identity else 'N'})"
                ),
                "cookie": "",
            }
        except Exception as e:
            logger.exception("[LOGIN] 로그인 처리 중 예외 발생")
            return {"success": False, "message": f"로그인 처리 중 오류: {e}", "cookie": ""}

    @staticmethod
    def _safe_session_get(
        session: requests.Session,
        url: str,
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        timeout: tuple[int, int] = (5, 15),
        retries: int = 2,
    ) -> requests.Response:
        last_error: Exception | None = None
        for attempt in range(retries):
            try:
                response = session.get(url, params=params, timeout=timeout, allow_redirects=True, headers=headers or {})
                response.raise_for_status()
                return response
            except requests.exceptions.Timeout as e:
                last_error = e
                if attempt < retries - 1:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                raise
        raise last_error or requests.exceptions.Timeout("session get timeout")

    @staticmethod
    def _safe_session_post(
        session: requests.Session,
        url: str,
        data: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        timeout: tuple[int, int] = (10, 30),
        retries: int = 2,
    ) -> requests.Response:
        last_error: Exception | None = None
        for attempt in range(retries):
            try:
                response = session.post(url, data=data, timeout=timeout, allow_redirects=True, headers=headers or {})
                response.raise_for_status()
                return response
            except requests.exceptions.Timeout as e:
                last_error = e
                if attempt < retries - 1:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                raise
        raise last_error or requests.exceptions.Timeout("session post timeout")

    def _probe_login_state(self, session: requests.Session, timeout: int | None = None) -> tuple[str, str]:
        probe_targets = [
            (TV_SHOW_LOGIN_PROBE_URL, f"{TV_PROGRAM_URL}?tab=vod"),
            (CLASSE_DETAIL_LOGIN_PROBE_URL, TV_SHOW_LOGIN_PROBE_URL),
        ]
        resolved_timeout = max(int(timeout or self.timeout or 15), 5)
        last_final_url = ""
        for probe_url, referer in probe_targets:
            response = self._safe_session_get(
                session,
                probe_url,
                headers={"Referer": referer},
                timeout=(5, resolved_timeout),
                retries=2,
            )
            text = response.text or ""
            final_url = response.url or probe_url
            last_final_url = final_url
            if _is_sso_or_login_url(final_url):
                continue

            vod_state = self._parse_vod_state(text)
            login_state = vod_state.get("isLogin", "")
            if login_state == "Y":
                return "Y", final_url
            header_cookie = session.headers.get("Cookie", "")
            if isinstance(header_cookie, bytes):
                header_cookie = header_cookie.decode("utf-8", errors="ignore")
            auth_signals = _has_auth_signal(session.cookies, header_cookie)
            if _is_classe_detail_url(final_url) and auth_signals:
                return "Y", final_url

        if last_final_url and _is_sso_or_login_url(last_final_url):
            return "N", last_final_url
        return ("미검출", last_final_url)

    def get_text(self, url: str, referer: str | None = None) -> str:
        response = self.get_response(url, referer=referer)
        return response.text or ""

    def get_response(self, url: str, referer: str | None = None) -> requests.Response:
        headers = {}
        if referer:
            headers["Referer"] = referer
        return self._safe_session_get(
            self.session,
            url,
            headers=headers,
            timeout=(10, max(self.timeout, 30)),
            retries=2,
        )

    def collect_daily_vods(self, page: int = 1) -> list[dict[str, Any]]:
        response = self._safe_session_post(
            self.session,
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
            timeout=(10, max(self.timeout, 30)),
            retries=2,
        )
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
        response = self.get_response(show_url, referer=f"{TV_PROGRAM_URL}?tab=vod")
        text = response.text or ""
        option = self._parse_vod_option(text)
        course_name = (option.get("courseNm") or "").strip()
        program_title = course_name if (course_name and (not _title_looks_generic(course_name))) else self._extract_program_title_from_text(text)
        if not program_title:
            program_title = course_name
        episode_no = self._extract_episode_no_from_text(text)
        if not episode_no:
            for candidate in [
                option.get("stepNm") or "",
                option.get("lectNm") or "",
                self._extract_episode_title_from_text(text),
            ]:
                episode_no = _extract_episode_no(candidate)
                if episode_no:
                    break
        return {
            "thumbnail": self._extract_thumbnail_from_text(text),
            "episode_no": episode_no,
            "program_title": program_title,
            "display_title": program_title or self._extract_display_title_from_text(text),
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
        response = self.get_response(show_url, referer=f"{TV_PROGRAM_URL}?tab=vod")
        final_url = response.url or show_url
        text = response.text or ""
        vod_state = self._parse_vod_state(text)
        qualities = self._extract_qualities(text)
        resolved_show_url = final_url or show_url
        final_url_lower = (final_url or "").lower()

        if (not qualities) and ("/vodcommon/show" in final_url_lower):
            qualities = self._extract_qualities(text)
        elif (not qualities) and (resolved_show_url != show_url):
            try:
                detail_text = self.get_text(resolved_show_url, referer=show_url)
                detail_state = self._parse_vod_state(detail_text)
                detail_qualities = self._extract_qualities(detail_text)
                if detail_qualities:
                    qualities = detail_qualities
                    if detail_state:
                        vod_state = detail_state
                    text = detail_text
            except Exception:
                pass
        elif (not qualities):
            detail_url = self._extract_detail_show_url(text)
            if detail_url and detail_url != resolved_show_url:
                try:
                    detail_text = self.get_text(detail_url, referer=resolved_show_url)
                    detail_state = self._parse_vod_state(detail_text)
                    detail_qualities = self._extract_qualities(detail_text)
                    if detail_qualities:
                        qualities = detail_qualities
                        if detail_state:
                            vod_state = detail_state
                        text = detail_text
                        resolved_show_url = detail_url
                except Exception:
                    pass
        preview_match = PREVIEW_RANGE_RE.search(text)
        preview_end = int(preview_match.group("end")) if preview_match else 0
        result = {
            "is_login": vod_state.get("isLogin", "N") == "Y",
            "buy_state": vod_state.get("buyState", ""),
            "qualities": qualities,
            "subtitles": {},
            "show_url": resolved_show_url,
            "preview_end": preview_end,
        }
        try:
            show_url_changed = (_safe_url_for_message(resolved_show_url) != _safe_url_for_message(show_url))
            if (not qualities) or preview_end > 0 or (result["is_login"] is False) or show_url_changed:
                logger.debug(
                    "[PLAY] resolve_play_info remote=%s/%s/%s final=%s is_login=%s buy_state=%s quality_count=%s quality_codes=%s preview_end=%s show_url_changed=%s",
                    remote_program_id,
                    remote_episode_id,
                    remote_media_id,
                    _safe_url_for_message(resolved_show_url),
                    "Y" if result["is_login"] else "N",
                    result["buy_state"],
                    len(qualities),
                    sorted(list(qualities.keys())),
                    preview_end,
                    show_url_changed,
                )
        except Exception:
            pass
        return result

    def _extract_qualities(self, text: str) -> dict[str, str]:
        qualities: dict[str, str] = {}
        search_spaces = [text or ""]
        source_match = SOURCE_ARRAY_RE.search(text or "")
        if source_match:
            search_spaces.insert(0, source_match.group("body") or "")
        for search_text in search_spaces:
            for match in QUALITY_RE.finditer(search_text):
                code = (match.group("code") or "").strip()
                src = (match.group("src") or "").strip()
                if code and src:
                    qualities[code] = html.unescape(src)
            if qualities:
                return qualities
        fallback_patterns = [
            ("M50", r"https://[^'\"\s>]+_m50\.(?:mp4|m3u8)[^'\"\s<]*"),
            ("M20", r"https://[^'\"\s>]+_m20\.(?:mp4|m3u8)[^'\"\s<]*"),
            ("M10", r"https://[^'\"\s>]+_m10\.(?:mp4|m3u8)[^'\"\s<]*"),
            ("M05", r"https://[^'\"\s>]+_m05\.(?:mp4|m3u8)[^'\"\s<]*"),
        ]
        for code, pattern in fallback_patterns:
            match = re.search(pattern, text or "", re.I)
            if match:
                qualities[code] = html.unescape(match.group(0))
        return qualities

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
            r"<p class=\"view\">[^<]*?(?P<num>\d+)\s*(?:회|화|편|부|강)\b",
            r"<strong[^>]*>[^<]*?(?P<num>\d+)\s*(?:회|화|편|부|강)\b",
            r"<title>[^<]*?(?P<num>\d+)\s*(?:회|화|편|부|강)",
        ]:
            match = re.search(pattern, text or "", re.I)
            if match:
                return match.group("num")
        return ""

    def _extract_display_title_from_text(self, text: str) -> str:
        program_title = self._extract_program_title_from_text(text)
        if program_title:
            return program_title
        return ""

    def _extract_program_title_from_text(self, text: str) -> str:
        parts = _extract_og_title_parts(text)
        if len(parts) >= 2:
            return parts[-1]
        if parts:
            return parts[0]
        return ""

    def _extract_episode_title_from_text(self, text: str) -> str:
        parts = _extract_og_title_parts(text)
        if len(parts) >= 2:
            return parts[0]
        return ""

    def _extract_detail_show_url(self, text: str) -> str:
        for regex in (CONTENT_URL_RE, OG_URL_RE):
            match = regex.search(text or "")
            if match:
                return _normalize_url(match.group("url") or "")
        return ""

    def _classify_source(self, url: str) -> str:
        if not url:
            return "unknown"
        parsed = urlparse(url)
        host = (parsed.netloc or "").lower()
        path = parsed.path or ""
        if host.endswith("www.ebs.co.kr") and path.startswith("/tv/show"):
            return "tv_show"
        if host.endswith("classe.ebs.co.kr") and path.startswith("/classe/detail/show"):
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
    parsed = urlparse(url or "")
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "").lower()
    return (
        host.endswith("sso.ebs.co.kr")
        or path.startswith("/login")
        or path.startswith("/sso/callback")
        or path.startswith("/classe/dummy")
    )


def _is_authenticated_content_url(url: str) -> bool:
    parsed = urlparse(url or "")
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""
    if host.endswith("www.ebs.co.kr") and path.startswith("/tv/show"):
        return True
    if host.endswith("classe.ebs.co.kr") and path.startswith("/classe/detail/show"):
        return True
    return False


def _is_classe_detail_url(url: str) -> bool:
    parsed = urlparse(url or "")
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""
    return host.endswith("classe.ebs.co.kr") and path.startswith("/classe/detail/show")


def _has_auth_signal(cookiejar: Any, cookie_header: str = "") -> bool:
    has_sso_auth = any(
        c.name == "sso.authenticated" and c.value == "1"
        for c in cookiejar
        if (c.domain or "").endswith("ebs.co.kr")
    )
    has_kc_identity = any(
        c.name == "KEYCLOAK_IDENTITY"
        for c in cookiejar
        if (c.domain or "").endswith("ebs.co.kr")
    )
    header_lower = (cookie_header or "").lower()
    return has_sso_auth or has_kc_identity or ("sso.authenticated=1" in header_lower) or ("keycloak_identity=" in header_lower)


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


def _extract_og_title_parts(text: str) -> list[str]:
    match = re.search(r"<meta\s+[^>]*property=[\"']og:title[\"'][^>]*content=[\"'](?P<title>[^\"']+)", text or "", re.I)
    if not match:
        return []
    title = html.unescape(match.group("title") or "")
    if " / " in title:
        parts = title.split(" / ")
    else:
        parts = title.split("/")
    return [part.strip() for part in parts if part.strip()]


def _title_looks_generic(title: str) -> bool:
    norm = re.sub(r"\s+", "", (title or "").strip().lower())
    return norm in {"", "ebs", "ebs애니키즈", "애니키즈", "ebsenglish", "ebsenglishtv"} or norm.isdigit()


def _extract_episode_no(title: str) -> str:
    title = html.unescape(title or "")
    match = re.search(r"(?:^|\s|\()제?\s*(\d+)\s*(?:회|화|편|부|강)\b", title)
    if match:
        return match.group(1)
    match = re.search(r"(?:episode|ep)\s*[-.]?\s*(\d+)\s*(?:회|화|편|부|강)?\b", title, re.I)
    if match:
        return match.group(1)
    return ""


def _date_key(value: str) -> tuple[int, int, int]:
    match = re.search(r"(\d{4})\.(\d{2})\.(\d{2})", value or "")
    if not match:
        return (0, 0, 0)
    return (int(match.group(1)), int(match.group(2)), int(match.group(3)))
