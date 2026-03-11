import dataclasses
import html
import logging
import re
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
                    "thumbnail": "",
                    "source_type": source_type,
                }
            )
        return rows

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


def _strip_html_preserve_text(text: str) -> str:
    text = html.unescape(text or "")
    text = re.sub(r"<span class=\"date\">.*?</span>", "", text, flags=re.S)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _extract_episode_no(title: str) -> str:
    title = html.unescape(title or "")
    match = re.match(r"\s*(\d+)\s*[회화편]\b", title)
    if match:
        return match.group(1)
    match = re.match(r"\s*제\s*(\d+)\s*[회화편]", title)
    if match:
        return match.group(1)
    return ""


def _date_key(value: str) -> tuple[int, int, int]:
    match = re.search(r"(\d{4})\.(\d{2})\.(\d{2})", value or "")
    if not match:
        return (0, 0, 0)
    return (int(match.group(1)), int(match.group(2)), int(match.group(3)))
