import datetime
import pathlib
import re

from sqlalchemy import desc, func, or_

from plugin.model_base import ModelBase
from .setup import F, P


def _make_safe_filename(value: str) -> str:
    text = (value or "").strip()
    text = re.sub(r'[\\/:*?"<>|]+', "_", text)
    text = re.sub(r"\s+", " ", text).strip(" .")
    return text or "EBS"


class ModelEbsEpisode(ModelBase):
    P = P
    __tablename__ = f"{P.package_name}_auto"
    __table_args__ = {"mysql_collate": "utf8_general_ci"}
    __bind_key__ = P.package_name

    id = F.db.Column(F.db.Integer, primary_key=True)
    created_time = F.db.Column(F.db.DateTime)
    updated_time = F.db.Column(F.db.DateTime)
    completed_time = F.db.Column(F.db.DateTime)

    remote_program_id = F.db.Column(F.db.String(64))
    remote_episode_id = F.db.Column(F.db.String(64))
    remote_media_id = F.db.Column(F.db.String(64))

    program_title = F.db.Column(F.db.String(255))
    display_title = F.db.Column(F.db.String(255))
    episode_no = F.db.Column(F.db.String(64))
    episode_title = F.db.Column(F.db.String(255))
    release_date = F.db.Column(F.db.String(32))
    show_url = F.db.Column(F.db.String(512))
    thumbnail = F.db.Column(F.db.String(512))
    source_type = F.db.Column(F.db.String(32))

    quality_code = F.db.Column(F.db.String(32))
    play_url = F.db.Column(F.db.Text)
    is_preview = F.db.Column(F.db.Boolean)
    is_login = F.db.Column(F.db.String(1))
    buy_state = F.db.Column(F.db.String(16))
    status = F.db.Column(F.db.String(32))
    message = F.db.Column(F.db.String(1024))
    retry = F.db.Column(F.db.Integer)
    completed = F.db.Column(F.db.Boolean)
    filesize = F.db.Column(F.db.Integer)
    filepath = F.db.Column(F.db.String(512))

    def __init__(self, remote_program_id: str, remote_episode_id: str, remote_media_id: str) -> None:
        now = datetime.datetime.now()
        self.created_time = now
        self.updated_time = now
        self.remote_program_id = remote_program_id
        self.remote_episode_id = remote_episode_id
        self.remote_media_id = remote_media_id
        self.retry = 0
        self.completed = False
        self.status = "PENDING"
        self.message = ""
        self.thumbnail = ""
        self.display_title = ""
        self.source_type = "tv_show"
        self.is_preview = False
        self.is_login = "N"
        self.buy_state = ""

    def set_info(
        self,
        program_title: str,
        episode_no: str,
        episode_title: str,
        release_date: str,
        show_url: str,
        thumbnail: str = "",
        display_title: str = "",
        source_type: str = "tv_show",
    ) -> None:
        self.program_title = program_title
        self.display_title = display_title or program_title
        self.episode_no = episode_no
        self.episode_title = episode_title
        self.release_date = release_date
        self.show_url = show_url
        self.source_type = source_type
        if thumbnail:
            self.thumbnail = thumbnail
        self.updated_time = datetime.datetime.now()

    @classmethod
    def get_by_keys(
        cls, remote_program_id: str, remote_episode_id: str, remote_media_id: str
    ) -> "ModelEbsEpisode":
        with F.app.app_context():
            return (
                F.db.session.query(cls)
                .filter_by(
                    remote_program_id=remote_program_id,
                    remote_episode_id=remote_episode_id,
                    remote_media_id=remote_media_id,
                )
                .order_by(desc(cls.id))
                .first()
            )

    @classmethod
    def get_by_id(cls, db_id: int) -> "ModelEbsEpisode":
        with F.app.app_context():
            return F.db.session.query(cls).filter_by(id=db_id).first()

    @classmethod
    def get_candidates(cls, max_retry: int) -> list["ModelEbsEpisode"]:
        with F.app.app_context():
            return (
                F.db.session.query(cls)
                .filter(cls.completed == False)
                .filter(
                    or_(
                        cls.status.in_(["PENDING", "WAITING", "FILTERED"]),
                        (cls.status == "FAILED") & (cls.retry < max_retry),
                    )
                )
                .order_by(desc(cls.id))
                .all()
            )

    @classmethod
    def get_failed(cls, max_retry: int) -> list["ModelEbsEpisode"]:
        with F.app.app_context():
            return (
                F.db.session.query(cls)
                .filter(cls.completed == False)
                .filter(cls.status == "FAILED")
                .filter(cls.retry < max_retry)
                .order_by(desc(cls.id))
                .all()
            )

    @classmethod
    def get_queue_states(cls) -> list["ModelEbsEpisode"]:
        with F.app.app_context():
            return (
                F.db.session.query(cls)
                .filter(cls.completed == False)
                .filter(cls.status.in_(["WAITING", "DOWNLOADING"]))
                .order_by(desc(cls.id))
                .all()
            )

    @classmethod
    def get_blank_episode_no_items(cls, limit: int = 100) -> list["ModelEbsEpisode"]:
        with F.app.app_context():
            query = (
                F.db.session.query(cls)
                .filter(cls.completed == False)
                .filter((cls.episode_no == None) | (func.trim(cls.episode_no) == ""))
                .filter(cls.source_type == "tv_show")
                .order_by(desc(cls.release_date), desc(cls.id))
            )
            if limit > 0:
                query = query.limit(limit)
            return query.all()

    @classmethod
    def get_program_group_items(cls, remote_program_id: str, remote_media_id: str) -> list["ModelEbsEpisode"]:
        with F.app.app_context():
            return (
                F.db.session.query(cls)
                .filter(cls.remote_program_id == remote_program_id)
                .filter(cls.remote_media_id == remote_media_id)
                .order_by(desc(cls.release_date), desc(cls.remote_episode_id), desc(cls.id))
                .all()
            )

    @classmethod
    def make_query(
        cls, req, order: str = "desc", search: str = "", option1: str = "all", option2: str = "all"
    ):
        with F.app.app_context():
            query = F.db.session.query(cls)
            option1 = (option1 or "all").strip().lower()
            if option1 == "completed":
                query = query.filter(cls.completed == True)
            elif option1 == "waiting":
                query = query.filter(cls.completed == False).filter(cls.status == "WAITING")
            elif option1 == "downloading":
                query = query.filter(cls.completed == False).filter(cls.status == "DOWNLOADING")
            elif option1 == "failed":
                query = query.filter(cls.completed == False).filter(cls.status.in_(["FAILED", "GIVEUP"]))
            elif option1 == "filtered":
                query = query.filter(cls.completed == False).filter(cls.status == "FILTERED")
            elif option1 == "preview":
                query = query.filter(cls.completed == False).filter(cls.status == "PREVIEW_BLOCKED")
            if search:
                like = f"%{search}%"
                query = query.filter(
                    or_(
                        cls.program_title.like(like),
                        cls.episode_title.like(like),
                        cls.episode_no.like(like),
                        cls.remote_program_id.like(like),
                        cls.remote_episode_id.like(like),
                        cls.remote_media_id.like(like),
                    )
                )
            if order == "desc":
                query = query.order_by(desc(cls.id))
            else:
                query = query.order_by(cls.id)
            return query

    @classmethod
    def delete_by_id(cls, db_id: int) -> bool:
        with F.app.app_context():
            entity = F.db.session.query(cls).filter_by(id=db_id).first()
            if entity is None:
                return False
            F.db.session.delete(entity)
            F.db.session.commit()
            return True

    def as_dict(self) -> dict:
        return {
            "id": self.id,
            "remote_program_id": self.remote_program_id,
            "remote_episode_id": self.remote_episode_id,
            "remote_media_id": self.remote_media_id,
            "program_title": self.program_title,
            "display_title": self.display_title,
            "episode_no": self.episode_no,
            "episode_title": self.episode_title,
            "release_date": self.release_date,
            "status": self.status,
            "message": self.message,
            "filepath": self.filepath,
            "filesize": self.filesize,
            "show_url": self.show_url,
            "thumbnail": self.thumbnail,
            "source_type": self.source_type,
            "is_preview": self.is_preview,
            "is_login": self.is_login,
            "buy_state": self.buy_state,
            "retry": self.retry,
            "completed": self.completed,
            "completed_time": self.completed_time.strftime("%Y-%m-%d %H:%M:%S") if self.completed_time else "",
        }

    def save(self) -> None:
        with F.app.app_context():
            F.db.session.add(self)
            F.db.session.commit()

    def make_filename(self, quality_code: str) -> str:
        title = (self.display_title or self.program_title or self.remote_program_id or "EBS").strip()
        date_digits = "000000"
        if self.release_date:
            parts = self.release_date.replace("-", ".").split(".")
            if len(parts) >= 3:
                y = parts[0][-2:]
                m = parts[1].zfill(2)
                d = parts[2].zfill(2)
                date_digits = f"{y}{m}{d}"
        ep = (self.episode_no or "0").strip()
        ep_digits = "".join(ch for ch in ep if ch.isdigit())
        if not ep_digits:
            title_match = re.match(r"\s*(\d+)\s*[회화편]\b", self.episode_title or "")
            if title_match:
                ep_digits = title_match.group(1)
        ep_part = f"E{int(ep_digits or '0'):02d}"
        quality_label = {"M50": "1080p", "M20": "720p", "M10": "480p", "M05": "360p"}.get(
            quality_code, quality_code or "NA"
        )
        safe = _make_safe_filename(title)
        return f"{safe}.{ep_part}.{date_digits}.{quality_label}-EBS.mp4"

    def get_target_path(self) -> pathlib.Path:
        base = pathlib.Path(P.ModelSetting.get("basic_save_path") or ".")
        quality_code = P.ModelSetting.get("basic_quality") or "M50"
        return base / self.make_filename(quality_code)
