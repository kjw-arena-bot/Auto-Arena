#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
LoL 아레나 3v3 (6팀 · 18인) 실측 데이터 수집 및 집계기
- 프리즘 아이템(44xxxx)과 일반 전설급 코어템 엄격 분리 (지배자의 피갑옷 등 오분류 차단)
- 협곡 ID(xxxx) / 아레나 복사 ID(22xxxx) 동일 아이템 병합
- 픽률(pick_rate) 계산 및 픽률순 + 평균 등수 함께 저장
- gzip 압축 체크포인트(arena_3v3_checkpoint.jsonl.gz) 지원 및 기존 .jsonl 자동 마이그레이션
"""

import argparse
import gzip
import json
import os
import sys
import time
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

# =============================================================================
# 1. 설정 상수
# =============================================================================
PLATFORM_HOST = "https://kr.api.riotgames.com"
REGIONAL_HOST = "https://asia.api.riotgames.com"

DDRAGON_VERSIONS_URL = "https://ddragon.leagueoflegends.com/api/versions.json"
CDRAGON_ASSET_PREFIX = (
    "https://raw.communitydragon.org/latest/plugins/rcp-be-lol-game-data/global/default/"
)
CHERRY_AUGMENTS_KO_URL = (
    "https://raw.communitydragon.org/latest/plugins/rcp-be-lol-game-data/global/ko_kr/v1/cherry-augments.json"
)
CDRAGON_ARENA_KO_URL = (
    "https://raw.communitydragon.org/latest/cdragon/arena/ko_kr.json"
)

CHECKPOINT_GZ_PATH = Path("arena_3v3_checkpoint.jsonl.gz")
CHECKPOINT_LEGACY_PATH = Path("arena_3v3_checkpoint.jsonl")
OUTPUT_JSON_PATH = Path("arena_3v3_guide_all_champs.json")

ARENA_QUEUE_ID = 1700

# 아레나 시작템 / 여눈 진화템
ARENA_STARTER_IDS = {2051, 3112, 3177, 3184, 222051, 223112, 223177, 223184}
TEAR_EVOLVED_IDS = {3040, 3042, 3121, 223040, 223042, 223121}
EXCLUDED_ITEM_TAGS = {"Boots", "Consumable", "Trinket", "Jungle", "Lane"}

# 공식 증강 등급 강제 보정 사전
OFFICIAL_AUGMENT_RARITY_KO: Dict[str, str] = {
    # 프리즘
    "감바 모루": "프리즘", "거석상의 용기": "프리즘", "거인": "프리즘", "거인 학살자": "프리즘",
    "공포를 부르는 자": "프리즘", "광대 대학": "프리즘", "돌진": "프리즘", "보석 건틀릿": "프리즘",
    "신비한 주먹": "프리즘", "양손잡이": "프리즘", "유레카": "프리즘", "전장의 교향곡": "프리즘",
    "정조준 일격 신동": "프리즘", "주문 각성": "프리즘", "지옥의 전도체": "프리즘", "최종 형태": "프리즘",
    "탄환 세례": "프리즘", "탭 댄서": "프리즘", "파멸의 예언자": "프리즘", "거인의 결의": "프리즘",
    "검무": "프리즘", "검을 뽑아라": "프리즘", "결투의 대가": "프리즘", "대지의 각성": "프리즘",
    "마법 소녀": "프리즘", "마트료시카": "프리즘", "불길한 서약": "프리즘", "야생 불꽃": "프리즘",
    "양자 연산": "프리즘", "우주의 중심": "프리즘", "유리 대포": "프리즘",
    "전환: 혼돈": "프리즘", "정신 결속": "프리즘", "찜솥": "프리즘", "최상급 조준경 부착": "프리즘",
    "케르베로스": "프리즘", "퀘스트: 우글렛의 마녀 모자": "프리즘", "프로틴 음료": "프리즘",
    "궁극기 대변혁": "프리즘", "궁극의 룰렛": "프리즘", "궤도형 레이저": "프리즘", "끝판왕": "프리즘",
    "난공불락": "프리즘", "마법 가속": "프리즘", "미친 과학자": "프리즘", "세월의 지혜": "프리즘",
    "요정 마법": "프리즘", "이케시아의 몰락": "프리즘", "전능의 영혼": "프리즘", "정의로운 분노": "프리즘",
    "죽음의 순환": "프리즘", "능력치 더하기 능력치 더하기 능력치!": "프리즘",
    # 골드
    "극악무도": "골드", "두 배 세 배": "골드", "마법 명사수": "골드", "마법 미사일": "골드",
    "바늘에 실 끼우기": "골드", "벼락": "골드", "전술적 대비": "골드", "전환: 프리즘": "골드",
    "처형 시간": "골드", "천상의 신체": "골드", "취약": "골드", "치명적 리듬": "골드",
    "퀘스트: 강철 같은 심장": "골드", "탱크 엔진": "골드", "화염 낙인": "골드", "감쇠 광선": "골드",
    "능력치 더하기 능력치!": "골드", "무법자의 투지": "골드", "불안정한 변형": "골드", "선동": "골드",
    "시작부터 끝까지": "골드", "신성한 중재": "골드", "어둠의 축복": "골드", "영혼 흡수": "골드",
    "인내심": "골드", "죽음의 손길": "골드", "최첨단 발명가": "골드", "치명적 치유": "골드",
    "치명적인 공격": "골드", "퀘스트: 우르프의 챔피언": "골드", "너만을 위해": "골드",
    "넘을 수 없는 벽": "골드", "노련한 저격수": "골드", "늘어나는 팔": "골드", "마도사의 주스 상자": "골드",
    "머리를 노려라": "골드", "방어 기동": "골드", "범람": "골드", "빵과 버터": "골드",
    "소나타": "골드", "소환사 혁명": "골드", "악마의 춤": "골드", "연계의 대가": "골드",
    "점멸 난사": "골드", "축소 엔진": "골드", "퀘스트: 응징의 천사": "골드", "환영 무기": "골드",
    "희생의 의지": "골드", "공생 돌연변이": "골드", "금방 돌아올게": "골드", "급속 성장": "골드",
    "기생 관계": "골드", "끊임없는 회복": "골드", "되풀이": "골드", "밥과 닭고기": "골드",
    "복수": "골드", "부메랑 투척": "골드", "빛의 인도자 결의": "골드", "빛의 파수꾼": "골드",
    "빵과 잼": "골드", "신난다!": "골드", "소멸": "골드",
    "퀘스트: 미치광이 모자장수": "골드", "해트트릭": "골드",
    # 실버
    "강타자": "실버", "응급처치 키트": "실버", "능수능란": "실버", "음속 폭발": "실버",
    "신성 모독자": "실버", "서리 망령": "실버", "고문자": "실버", "죄책감의 쾌락": "실버",
    "우당탕탕": "실버", "빨랫줄": "실버", "점액 천지": "실버", "불여우": "실버",
    "주스 압착기": "실버", "에너지 충전": "실버", "침탈": "실버",
}

AUGMENT_NAME_CORRECTIONS: Dict[str, str] = {
    "골리앗": "거인",
    "용기의 거인": "거석상의 용기",
    "거인의 용기": "거석상의 용기",
    "취약점": "취약",
    "인내": "인내심",
    "응급 처치 키트": "응급처치 키트",
    "처형자": "처형 시간",
    "치명적 속도 + 정복자": "전장의 교향곡",
    "모든 것을 바쳐": "너만을 위해",
    "약탈": "침탈",
}


def log_print(msg: str) -> None:
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {msg}", flush=True)


def normalize_augment_name(raw_name: str) -> str:
    name = str(raw_name or "").strip()
    return AUGMENT_NAME_CORRECTIONS.get(name, name)


def normalize_cdragon_asset(raw_path: str) -> str:
    if not raw_path:
        return ""
    trimmed = str(raw_path).strip()
    if trimmed.startswith(("http://", "https://")):
        return trimmed
    cleaned = trimmed.lower()
    if cleaned.startswith("/lol-game-data/assets/"):
        cleaned = cleaned[len("/lol-game-data/assets/"):]
    elif cleaned.startswith("lol-game-data/assets/"):
        cleaned = cleaned[len("lol-game-data/assets/"):]
    return CDRAGON_ASSET_PREFIX + cleaned.lstrip("/")


def parse_patch(game_version: str) -> str:
    parts = str(game_version or "").split(".")
    if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
        return f"{parts[0]}.{parts[1]}"
    return "unknown"


# =============================================================================
# 2. 공식 메타데이터 로더 (Data Dragon + CommunityDragon)
# =============================================================================
class OfficialMetadata:
    def __init__(self) -> None:
        self.dd_version: str = "16.23.1"
        self.champions_by_key: Dict[str, Dict[str, Any]] = {}
        self.champ_lower_to_key: Dict[str, str] = {}

        # 아이템 ID -> 메타데이터
        self.items_by_id: Dict[int, Dict[str, Any]] = {}
        # 전설템 한글명 -> 대표 DDragon ID (1000~6999 우선)
        self.legendary_name_to_canonical_id: Dict[str, int] = {}
        self.legendary_name_priority: Dict[str, int] = {}
        # 프리즘 한글명 -> 대표 44xxxx ID
        self.prismatic_name_to_id: Dict[str, int] = {}

        # 증강 ID -> {id, name, rarity, icon}
        self.augments_by_id: Dict[int, Dict[str, Any]] = {}
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Arena3v3Collector/2.0"})

    def load_all(self) -> None:
        self._load_ddragon()
        self._load_cdragon_augments_and_items()

    def _dd_item_icon(self, item_id: int) -> str:
        return f"https://ddragon.leagueoflegends.com/cdn/{self.dd_version}/img/item/{item_id}.png"

    def _load_ddragon(self) -> None:
        try:
            ver_res = self.session.get(DDRAGON_VERSIONS_URL, timeout=15)
            ver_res.raise_for_status()
            versions = ver_res.json()
            if versions:
                self.dd_version = versions[0]
            log_print(f"Data Dragon 최신 버전: {self.dd_version}")

            champ_url = f"https://ddragon.leagueoflegends.com/cdn/{self.dd_version}/data/ko_KR/champion.json"
            item_url = f"https://ddragon.leagueoflegends.com/cdn/{self.dd_version}/data/ko_KR/item.json"

            champ_data = self.session.get(champ_url, timeout=20).json().get("data", {})
            for cid, info in champ_data.items():
                self.champions_by_key[cid] = {
                    "champion_name": cid,
                    "kr_name": info.get("name", cid),
                    "roles": info.get("tags", ["Fighter"]),
                }
                self.champ_lower_to_key[cid.lower()] = cid

            item_data = self.session.get(item_url, timeout=20).json().get("data", {})
            for id_str, info in item_data.items():
                try:
                    iid = int(id_str)
                except ValueError:
                    continue
                name = str(info.get("name", "")).strip()
                if not name:
                    continue
                gold_total = int(info.get("gold", {}).get("total", 0) or 0)
                tags = set(info.get("tags", []) or [])
                into_list = info.get("into", []) or []

                self.items_by_id[iid] = {
                    "id": iid,
                    "name": name,
                    "gold_total": gold_total,
                    "tags": tags,
                    "into": into_list,
                    "icon": self._dd_item_icon(iid),
                }

                if 440000 <= iid < 450000:
                    self.prismatic_name_to_id[name] = iid
                elif (1000 <= iid < 7000) or (220100 <= iid < 230000):
                    prio = 3 if (1000 <= iid < 7000) else 2
                    if prio > self.legendary_name_priority.get(name, 0):
                        self.legendary_name_to_canonical_id[name] = iid
                        self.legendary_name_priority[name] = prio

            log_print(
                f"Data Dragon 로드 완료: 챔피언 {len(self.champions_by_key)}명, "
                f"아이템 {len(self.items_by_id)}개 (프리즘 {len(self.prismatic_name_to_id)}종)"
            )
        except Exception as e:
            log_print(f"[경고] Data Dragon 로드 실패: {e}")

    @staticmethod
    def _calibrate_rarity_codes(raw_augs: List[Dict[str, Any]]) -> Dict[int, str]:
        votes: Dict[int, Dict[str, int]] = defaultdict(lambda: {"실버": 0, "골드": 0, "프리즘": 0})
        codes: Set[int] = set()
        for a in raw_augs:
            r = a.get("rarity")
            if isinstance(r, int):
                codes.add(r)
                name = normalize_augment_name(a.get("nameTRA") or a.get("name") or "")
                known = OFFICIAL_AUGMENT_RARITY_KO.get(name)
                if known:
                    votes[r][known] += 1
        result: Dict[int, str] = {}
        for code, cnts in votes.items():
            best_label, best_val = max(cnts.items(), key=lambda x: x[1])
            if best_val > 0:
                result[code] = best_label
        sorted_codes = sorted(codes)
        if len(result) < 3 and len(sorted_codes) >= 3:
            if 4 in codes and 8 in codes:
                result.setdefault(0, "실버")
                result.setdefault(1, "실버")
                result.setdefault(4, "골드")
                result.setdefault(8, "프리즘")
            else:
                result.setdefault(sorted_codes[0], "실버")
                result.setdefault(sorted_codes[1], "골드")
                result.setdefault(sorted_codes[2], "프리즘")
        return result

    @staticmethod
    def _resolve_rarity(raw_rarity: Any, aug_name: str, num_map: Dict[int, str]) -> str:
        clean = normalize_augment_name(aug_name)
        if clean in OFFICIAL_AUGMENT_RARITY_KO:
            return OFFICIAL_AUGMENT_RARITY_KO[clean]
        if isinstance(raw_rarity, str):
            low = raw_rarity.strip().lower()
            if "prism" in low or "프리즘" in low:
                return "프리즘"
            if "gold" in low or "골드" in low:
                return "골드"
            if "silver" in low or "실버" in low:
                return "실버"
            if low.isdigit():
                raw_rarity = int(low)
        if isinstance(raw_rarity, int):
            if raw_rarity in num_map:
                return num_map[raw_rarity]
            if raw_rarity in (2, 8):
                return "프리즘"
            if raw_rarity in (1, 4):
                return "골드"
        return "실버"

    def _load_cdragon_augments_and_items(self) -> None:
        # 1) cherry-augments.json
        try:
            res = self.session.get(CHERRY_AUGMENTS_KO_URL, timeout=20)
            if res.ok:
                raw_list = res.json()
                if isinstance(raw_list, list):
                    num_map = self._calibrate_rarity_codes(raw_list)
                    for aug in raw_list:
                        aid = int(aug.get("id", 0) or 0)
                        name = normalize_augment_name(aug.get("nameTRA") or aug.get("name") or "")
                        if not aid or not name or name == "???":
                            continue
                        icon = normalize_cdragon_asset(
                            aug.get("augmentSmallIconPath") or aug.get("augmentLargeIconPath") or ""
                        )
                        self.augments_by_id[aid] = {
                            "id": aid,
                            "name": name,
                            "rarity": self._resolve_rarity(aug.get("rarity"), name, num_map),
                            "icon": icon,
                        }
        except Exception as e:
            log_print(f"[경고] cherry-augments 로드 실패: {e}")

        # 2) cdragon/arena/ko_kr.json (증강 보강 + 프리즘/아레나 아이템 보강)
        try:
            res = self.session.get(CDRAGON_ARENA_KO_URL, timeout=20)
            if res.ok:
                arena_json = res.json()
                raw_augs = arena_json.get("augments", []) or []
                num_map = self._calibrate_rarity_codes(raw_augs)
                for aug in raw_augs:
                    aid = int(aug.get("id", 0) or 0)
                    name = normalize_augment_name(aug.get("name") or "")
                    if not aid or not name or name == "???":
                        continue
                    icon = normalize_cdragon_asset(aug.get("iconSmall") or aug.get("iconLarge") or "")
                    if aid not in self.augments_by_id:
                        self.augments_by_id[aid] = {
                            "id": aid,
                            "name": name,
                            "rarity": self._resolve_rarity(aug.get("rarity"), name, num_map),
                            "icon": icon,
                        }
                    elif not self.augments_by_id[aid].get("icon") and icon:
                        self.augments_by_id[aid]["icon"] = icon

                for it in arena_json.get("items", []) or []:
                    iid = int(it.get("id", 0) or 0)
                    name = str(it.get("name", "")).strip()
                    if not iid or not name:
                        continue
                    cd_icon = normalize_cdragon_asset(it.get("iconPath") or "")
                    if iid in self.items_by_id:
                        if not self.items_by_id[iid].get("icon") and cd_icon:
                            self.items_by_id[iid]["icon"] = cd_icon
                    else:
                        self.items_by_id[iid] = {
                            "id": iid,
                            "name": name,
                            "gold_total": int(it.get("price", 0) or 0),
                            "tags": set(),
                            "into": [],
                            "icon": self._dd_item_icon(iid) if (440000 <= iid < 450000) else cd_icon,
                        }
                    if 440000 <= iid < 450000:
                        self.prismatic_name_to_id.setdefault(name, iid)

            log_print(
                f"CommunityDragon 로드 완료: 증강 {len(self.augments_by_id)}개, "
                f"총 아이템 {len(self.items_by_id)}개"
            )
        except Exception as e:
            log_print(f"[경고] cdragon/arena 로드 실패: {e}")

    # -------------------------------------------------------------------------
    # 핵심: 프리즘(44xxxx) vs 일반 전설급 코어템 엄격 분류
    # -------------------------------------------------------------------------
    def classify_item(self, item_id: int) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """
        반환값:
          ("prismatic", {id, name, icon})
          ("legendary", {id, name, icon})
          (None, None) -> 신발/시작템/모루/하위템 등 제외
        """
        if not item_id or item_id <= 0:
            return None, None

        meta = self.items_by_id.get(item_id)

        # 1) 프리즘 아이템: Riot 공식 아레나 프리즘 ID 대역 440000 ~ 449999만 인정
        if 440000 <= item_id < 450000:
            if not meta:
                return None, None
            tags = meta.get("tags", set())
            if "Consumable" in tags or "Trinket" in tags:
                return None, None
            gold_total = int(meta.get("gold_total", 0) or 0)
            if gold_total and gold_total != 4000:
                return None, None
            name = meta["name"]
            canon_id = self.prismatic_name_to_id.get(name, item_id)
            return "prismatic", {
                "id": canon_id,
                "name": name,
                "icon": self._dd_item_icon(canon_id),
            }

        # 2) 시작템 / 모루(220000~220099) / 오른 걸작템(7000~7099) / 저가 잡템 제외
        if item_id in ARENA_STARTER_IDS:
            return None, None
        if 220000 <= item_id < 220100:
            return None, None
        if 7000 <= item_id < 7100:
            return None, None
        if item_id < 1000:
            return None, None

        # 아레나 복사 전설템(22xxxx)의 메타가 없으면 기본 협곡 ID(xxxx) 참조
        base_id = item_id - 220000 if 220100 <= item_id < 230000 else item_id
        if not meta and base_id != item_id:
            meta = self.items_by_id.get(base_id)
        if not meta:
            return None, None

        tags = meta.get("tags", set())
        if tags & EXCLUDED_ITEM_TAGS:
            return None, None

        # 여눈 진화 완성템은 purchasable 여부와 무관하게 전설급으로 인정
        is_legendary = False
        if item_id in TEAR_EVOLVED_IDS or base_id in TEAR_EVOLVED_IDS:
            is_legendary = True
        else:
            gold_total = int(meta.get("gold_total", 0) or 0)
            into_list = meta.get("into", []) or []
            if 220100 <= item_id < 230000:
                is_legendary = gold_total >= 2000 or gold_total == 0
            else:
                is_legendary = gold_total >= 2000 and len(into_list) == 0

        if not is_legendary:
            return None, None

        name = meta["name"]
        # 협곡 ID(2501)와 아레나 ID(222501)를 하나의 대표 ID로 통합
        canon_id = self.legendary_name_to_canonical_id.get(name)
        if not canon_id:
            canon_id = base_id if base_id in self.items_by_id else item_id

        icon = self._dd_item_icon(canon_id) if canon_id in self.items_by_id else meta.get("icon", "")
        return "legendary", {
            "id": canon_id,
            "name": name,
            "icon": icon,
        }


# =============================================================================
# 3. Riot API 클라이언트
# =============================================================================
class RiotApiClient:
    def __init__(self, api_key: str, request_delay: float = 1.25) -> None:
        self.api_key = api_key
        self.request_delay = request_delay
        self.last_req_ts = 0.0
        self.auth_failed = False
        self.session = requests.Session()
        self.session.headers.update({"X-Riot-Token": api_key})

    def get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        if self.auth_failed:
            return None

        for attempt in range(5):
            elapsed = time.time() - self.last_req_ts
            if elapsed < self.request_delay:
                time.sleep(self.request_delay - elapsed)
            self.last_req_ts = time.time()

            try:
                res = self.session.get(url, params=params, timeout=20)
            except requests.RequestException as e:
                wait = min(10, 2 * (attempt + 1))
                log_print(f"[네트워크 오류] {e} ({wait}s 후 재시도)")
                time.sleep(wait)
                continue

            if res.status_code == 200:
                return res.json()
            if res.status_code == 429:
                retry_after = int(res.headers.get("Retry-After", "5") or 5) + 1
                log_print(f"[Rate Limit 429] {retry_after}초 대기...")
                time.sleep(retry_after)
                continue
            if res.status_code in (401, 403):
                log_print(f"[치명적 오류] Riot API 키 만료 또는 권한 거부 (HTTP {res.status_code})")
                self.auth_failed = True
                return None
            if res.status_code == 404:
                return None
            if res.status_code >= 500:
                time.sleep(3 * (attempt + 1))
                continue
            return None
        return None

    def fetch_seed_puuids(self, max_seeds: int = 120) -> List[str]:
        puuids: List[str] = []
        seen: Set[str] = set()
        endpoints = [
            f"{PLATFORM_HOST}/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5",
            f"{PLATFORM_HOST}/lol/league/v4/grandmasterleagues/by-queue/RANKED_SOLO_5x5",
            f"{PLATFORM_HOST}/lol/league/v4/masterleagues/by-queue/RANKED_SOLO_5x5",
        ]
        for ep in endpoints:
            data = self.get_json(ep)
            if not data or self.auth_failed:
                break
            entries = data.get("entries", []) or []
            for entry in entries:
                puuid = entry.get("puuid")
                if puuid and puuid not in seen:
                    seen.add(puuid)
                    puuids.append(puuid)
                    if len(puuids) >= max_seeds:
                        return puuids
        return puuids

    def fetch_arena_match_ids(self, puuid: str, count: int = 30) -> List[str]:
        url = f"{REGIONAL_HOST}/lol/match/v5/matches/by-puuid/{puuid}/ids"
        data = self.get_json(url, params={"queue": ARENA_QUEUE_ID, "start": 0, "count": count})
        return data if isinstance(data, list) else []

    def fetch_match_detail(self, match_id: str) -> Optional[Dict[str, Any]]:
        url = f"{REGIONAL_HOST}/lol/match/v5/matches/{match_id}"
        data = self.get_json(url)
        return data if isinstance(data, dict) else None


# =============================================================================
# 4. 체크포인트 입출력 (기존 .jsonl & .jsonl.gz 완벽 호환)
# =============================================================================
def extract_compact_match(raw_obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Riot 원본 Match-v5 JSON 또는 기존 압축 레코드를 표준 경량 매치 딕셔너리로 정규화.
    아레나 3v3(6팀 · 최대 18명) 판별 포함.
    """
    # 형식 A: Riot Match-v5 원본 {"metadata": ..., "info": ...}
    if "info" in raw_obj and isinstance(raw_obj["info"], dict):
        info = raw_obj["info"]
        meta = raw_obj.get("metadata", {}) or {}
        match_id = str(meta.get("matchId") or raw_obj.get("match_id") or "").strip()
        if not match_id:
            return None

        game_mode = str(info.get("gameMode", "")).upper()
        queue_id = int(info.get("queueId", 0) or 0)
        if queue_id != ARENA_QUEUE_ID and game_mode != "CHERRY":
            return None

        raw_parts = info.get("participants", []) or []
        if not raw_parts:
            return None

        placements = [int(p.get("placement", 0) or 0) for p in raw_parts if int(p.get("placement", 0) or 0) > 0]
        max_place = max(placements) if placements else 0
        # 3v3 (6팀 · 18인) 필터: 참가자 18명 또는 최대 등수 6 이하 (2v2 8팀 제외)
        if len(raw_parts) != 18 and not (1 <= max_place <= 6 and len(raw_parts) >= 15):
            return None

        compact_parts = []
        for p in raw_parts:
            champ = str(p.get("championName", "")).strip()
            placement = int(p.get("placement", 0) or 0)
            if not champ or placement <= 0 or placement > 6:
                continue
            items = []
            for slot in range(7):
                iid = int(p.get(f"item{slot}", 0) or 0)
                if iid > 0:
                    items.append(iid)
            augs = []
            for slot in range(1, 7):
                aid = int(p.get(f"playerAugment{slot}", 0) or 0)
                if aid > 0:
                    augs.append(aid)
            compact_parts.append({
                "puuid": p.get("puuid", ""),
                "champ": champ,
                "placement": placement,
                "team": int(p.get("playerSubteamId", 0) or 0),
                "items": items,
                "augs": augs,
                "dmg": int(p.get("totalDamageDealtToChampions", 0) or 0),
            })

        if not compact_parts:
            return None

        return {
            "match_id": match_id,
            "patch": parse_patch(info.get("gameVersion", "")),
            "ts": int(info.get("gameCreation", 0) or 0),
            "participants": compact_parts,
        }

    # 형식 B: 이미 경량화된 매치 레코드 {"match_id": ..., "participants": [...]}
    if "participants" in raw_obj and isinstance(raw_obj["participants"], list):
        match_id = str(raw_obj.get("match_id") or raw_obj.get("matchId") or "").strip()
        if not match_id:
            return None
        patch = str(raw_obj.get("patch") or parse_patch(raw_obj.get("gameVersion", ""))).strip()
        norm_parts = []
        for p in raw_obj["participants"]:
            if not isinstance(p, dict):
                continue
            champ = str(p.get("champ") or p.get("championName") or p.get("champion_name") or "").strip()
            placement = int(p.get("placement", 0) or 0)
            if not champ or placement <= 0 or placement > 6:
                continue
            items = p.get("items")
            if not isinstance(items, list):
                items = [int(p.get(f"item{i}", 0) or 0) for i in range(7) if int(p.get(f"item{i}", 0) or 0) > 0]
            augs = p.get("augs") if isinstance(p.get("augs"), list) else p.get("augments")
            if not isinstance(augs, list):
                augs = [
                    int(p.get(f"playerAugment{i}", 0) or 0)
                    for i in range(1, 7)
                    if int(p.get(f"playerAugment{i}", 0) or 0) > 0
                ]
            norm_parts.append({
                "puuid": p.get("puuid", ""),
                "champ": champ,
                "placement": placement,
                "team": int(p.get("team") or p.get("playerSubteamId") or p.get("subteam_id") or 0),
                "items": [int(x) for x in items if str(x).isdigit() and int(x) > 0],
                "augs": [int(x) for x in augs if str(x).isdigit() and int(x) > 0],
                "dmg": int(p.get("dmg") or p.get("damage") or p.get("totalDamageDealtToChampions") or 0),
            })
        if not norm_parts:
            return None
        return {
            "match_id": match_id,
            "patch": patch or "unknown",
            "ts": int(raw_obj.get("ts") or raw_obj.get("game_creation") or 0),
            "participants": norm_parts,
        }

    return None


def load_existing_checkpoint() -> Dict[str, Dict[str, Any]]:
    matches: Dict[str, Dict[str, Any]] = {}

    def _read_stream(stream: Any, label: str) -> None:
        loaded = 0
        for line in stream:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            compact = extract_compact_match(obj)
            if compact and compact["match_id"] not in matches:
                matches[compact["match_id"]] = compact
                loaded += 1
        log_print(f"{label}에서 {loaded:,}건 로드 완료")

    if CHECKPOINT_GZ_PATH.exists():
        with gzip.open(CHECKPOINT_GZ_PATH, "rt", encoding="utf-8") as f:
            _read_stream(f, CHECKPOINT_GZ_PATH.name)

    if CHECKPOINT_LEGACY_PATH.exists():
        with open(CHECKPOINT_LEGACY_PATH, "rt", encoding="utf-8") as f:
            _read_stream(f, CHECKPOINT_LEGACY_PATH.name)

    return matches


def save_compressed_checkpoint(matches: Dict[str, Dict[str, Any]]) -> None:
    tmp_path = CHECKPOINT_GZ_PATH.with_suffix(".jsonl.gz.tmp")
    with gzip.open(tmp_path, "wt", encoding="utf-8", compresslevel=6) as f:
        for m in matches.values():
            # 저장 용량 절감을 위해 puuid는 제외하지 않고 유지하되 불필요 공백 제거
            f.write(json.dumps(m, ensure_ascii=False, separators=(",", ":")) + "\n")
    tmp_path.replace(CHECKPOINT_GZ_PATH)

    if CHECKPOINT_LEGACY_PATH.exists():
        try:
            CHECKPOINT_LEGACY_PATH.unlink()
            log_print(f"구 비압축 파일 삭제 완료: {CHECKPOINT_LEGACY_PATH.name}")
        except OSError:
            pass

    size_mb = CHECKPOINT_GZ_PATH.stat().st_size / (1024 * 1024)
    log_print(f"압축 체크포인트 저장 완료: {CHECKPOINT_GZ_PATH.name} ({len(matches):,}매치, {size_mb:.2f} MB)")


# =============================================================================
# 5. 통계 집계 및 JSON 빌드 (프리즘/전설 분리 + 픽률순 & 평균 등수 저장)
# =============================================================================
def _patch_sort_key(patch: str) -> Tuple[int, int]:
    parts = patch.split(".")
    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
        return (int(parts[0]), int(parts[1]))
    return (0, 0)


def build_guide_json(matches: Dict[str, Dict[str, Any]], meta: OfficialMetadata) -> Dict[str, Any]:
    # 챔피언별 집계 버킷
    champ_overall = defaultdict(lambda: {"picks": 0, "place_sum": 0, "top3": 0, "win": 0, "dmg_sum": 0})
    champ_patch = defaultdict(lambda: defaultdict(lambda: {"picks": 0, "place_sum": 0, "top3": 0, "win": 0, "dmg_sum": 0}))
    champ_prismatics = defaultdict(lambda: defaultdict(lambda: {"id": 0, "name": "", "icon": "", "picks": 0, "place_sum": 0, "top3": 0, "win": 0}))
    champ_legendaries = defaultdict(lambda: defaultdict(lambda: {"id": 0, "name": "", "icon": "", "picks": 0, "place_sum": 0, "top3": 0, "win": 0}))
    champ_augments = defaultdict(lambda: defaultdict(lambda: {"id": 0, "name": "", "rarity": "실버", "icon": "", "picks": 0, "place_sum": 0, "top3": 0, "win": 0}))
    champ_partners = defaultdict(lambda: defaultdict(lambda: {"picks": 0, "place_sum": 0, "top3": 0, "win": 0}))

    all_patches: Set[str] = set()

    for m in matches.values():
        patch = m.get("patch", "unknown")
        if patch != "unknown":
            all_patches.add(patch)

        parts = m.get("participants", [])
        # 같은 팀(3인 1팀) 그룹화
        teams: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
        for p in parts:
            t_id = int(p.get("team", 0) or 0)
            if t_id > 0:
                teams[t_id].append(p)

        for p in parts:
            raw_champ = str(p.get("champ", "")).strip()
            c_key = meta.champ_lower_to_key.get(raw_champ.lower(), raw_champ)
            placement = int(p.get("placement", 0) or 0)
            if not c_key or placement <= 0 or placement > 6:
                continue

            is_top3 = 1 if placement <= 3 else 0
            is_win = 1 if placement == 1 else 0
            dmg = int(p.get("dmg", 0) or 0)

            ov = champ_overall[c_key]
            ov["picks"] += 1
            ov["place_sum"] += placement
            ov["top3"] += is_top3
            ov["win"] += is_win
            ov["dmg_sum"] += dmg

            if patch != "unknown":
                ps = champ_patch[c_key][patch]
                ps["picks"] += 1
                ps["place_sum"] += placement
                ps["top3"] += is_top3
                ps["win"] += is_win
                ps["dmg_sum"] += dmg

            # 1) 아이템 분류 집계 (한 판 내 동일 아이템 중복 집계 방지)
            seen_prism: Set[str] = set()
            seen_leg: Set[str] = set()
            for raw_iid in p.get("items", []):
                kind, info = meta.classify_item(int(raw_iid))
                if not kind or not info:
                    continue
                iname = info["name"]
                if kind == "prismatic":
                    if iname in seen_prism:
                        continue
                    seen_prism.add(iname)
                    bucket = champ_prismatics[c_key][iname]
                    bucket["id"] = info["id"]
                    bucket["name"] = iname
                    bucket["icon"] = info["icon"]
                    bucket["picks"] += 1
                    bucket["place_sum"] += placement
                    bucket["top3"] += is_top3
                    bucket["win"] += is_win
                elif kind == "legendary":
                    if iname in seen_leg:
                        continue
                    seen_leg.add(iname)
                    bucket = champ_legendaries[c_key][iname]
                    bucket["id"] = info["id"]
                    bucket["name"] = iname
                    bucket["icon"] = info["icon"]
                    bucket["picks"] += 1
                    bucket["place_sum"] += placement
                    bucket["top3"] += is_top3
                    bucket["win"] += is_win

            # 2) 증강체 집계
            seen_augs: Set[str] = set()
            for raw_aid in p.get("augs", []):
                aug_info = meta.augments_by_id.get(int(raw_aid))
                if not aug_info:
                    continue
                aname = aug_info["name"]
                if aname in seen_augs:
                    continue
                seen_augs.add(aname)
                ab = champ_augments[c_key][aname]
                ab["id"] = aug_info["id"]
                ab["name"] = aname
                ab["rarity"] = aug_info["rarity"]
                ab["icon"] = aug_info["icon"]
                ab["picks"] += 1
                ab["place_sum"] += placement
                ab["top3"] += is_top3
                ab["win"] += is_win

            # 3) 3v3 팀 파트너 집계
            t_id = int(p.get("team", 0) or 0)
            if t_id > 0:
                seen_mates: Set[str] = set()
                for mate in teams.get(t_id, []):
                    if mate is p:
                        continue
                    m_raw = str(mate.get("champ", "")).strip()
                    m_key = meta.champ_lower_to_key.get(m_raw.lower(), m_raw)
                    if not m_key or m_key == c_key or m_key in seen_mates:
                        continue
                    seen_mates.add(m_key)
                    pb = champ_partners[c_key][m_key]
                    pb["picks"] += 1
                    pb["place_sum"] += placement
                    pb["top3"] += is_top3
                    pb["win"] += is_win

    # 패치 최신 가중치 곡선 (index.html과 동일)
    patches_desc = sorted(all_patches, key=_patch_sort_key, reverse=True)
    weight_curve = [1.0, 0.75, 0.5, 0.35, 0.25]
    patch_weights = {
        p: weight_curve[min(i, len(weight_curve) - 1)]
        for i, p in enumerate(patches_desc)
    }

    def _finalize_item_list(bucket_map: Dict[str, Dict[str, Any]], total_picks: int, limit: int = 20) -> List[Dict[str, Any]]:
        rows = []
        for b in bucket_map.values():
            n = b["picks"]
            if n <= 0:
                continue
            avg_p = round(b["place_sum"] / n, 2)
            pick_rate = round(min(100.0, (n / total_picks) * 100.0), 1) if total_picks > 0 else 0.0
            rows.append({
                "id": b["id"],
                "name": b["name"],
                "icon": b["icon"],
                "picks": n,
                "pick_rate": pick_rate,
                "avg_placement": avg_p,
                "avg": avg_p,
                "top3_rate": round((b["top3"] / n) * 100.0, 1),
                "win_rate": round((b["win"] / n) * 100.0, 1),
            })
        # 픽률 내림차순 → 표본 수 내림차순 → 평균 등수 오름차순
        rows.sort(key=lambda x: (-x["pick_rate"], -x["picks"], x["avg_placement"]))
        return rows[:limit]

    def _finalize_aug_list(bucket_map: Dict[str, Dict[str, Any]], total_picks: int, limit: int = 45) -> List[Dict[str, Any]]:
        rows = []
        for b in bucket_map.values():
            n = b["picks"]
            if n <= 0:
                continue
            avg_p = round(b["place_sum"] / n, 2)
            pick_rate = round(min(100.0, (n / total_picks) * 100.0), 1) if total_picks > 0 else 0.0
            rows.append({
                "id": b["id"],
                "name": b["name"],
                "rarity": b["rarity"],
                "icon": b["icon"],
                "picks": n,
                "pick_rate": pick_rate,
                "avg_placement": avg_p,
                "avg": avg_p,
                "top3_rate": round((b["top3"] / n) * 100.0, 1),
                "win_rate": round((b["win"] / n) * 100.0, 1),
            })
        rows.sort(key=lambda x: (-x["pick_rate"], -x["picks"], x["avg_placement"]))
        return rows[:limit]

    def _finalize_partner_list(bucket_map: Dict[str, Dict[str, Any]], total_picks: int, limit: int = 12) -> List[Dict[str, Any]]:
        rows = []
        for partner_key, b in bucket_map.items():
            n = b["picks"]
            if n < 2:
                continue
            avg_p = round(b["place_sum"] / n, 2)
            pick_rate = round(min(100.0, (n / total_picks) * 100.0), 1) if total_picks > 0 else 0.0
            p_meta = meta.champions_by_key.get(partner_key, {})
            rows.append({
                "champion_name": partner_key,
                "kr_name": p_meta.get("kr_name", partner_key),
                "picks": n,
                "pick_rate": pick_rate,
                "avg_placement": avg_p,
                "avg": avg_p,
                "top3_rate": round((b["top3"] / n) * 100.0, 1),
                "win_rate": round((b["win"] / n) * 100.0, 1),
            })
        rows.sort(key=lambda x: (-x["pick_rate"], -x["picks"], x["avg_placement"]))
        return rows[:limit]

    guide: Dict[str, Any] = {}
    tier_to_num = {"S": 1, "A": 2, "B": 3, "C": 4}

    for c_key, ov in champ_overall.items():
        total_picks = ov["picks"]
        if total_picks <= 0:
            continue

        c_meta = meta.champions_by_key.get(c_key, {})
        patch_stats_out: Dict[str, Dict[str, Any]] = {}
        w_picks_sum = 0.0
        w_place_sum = 0.0
        w_top3_sum = 0.0
        w_win_sum = 0.0
        w_dmg_sum = 0.0

        for p_ver in patches_desc:
            ps = champ_patch[c_key].get(p_ver)
            if not ps or ps["picks"] <= 0:
                continue
            n = ps["picks"]
            p_avg = round(ps["place_sum"] / n, 2)
            p_top3 = round((ps["top3"] / n) * 100.0, 1)
            p_win = round((ps["win"] / n) * 100.0, 1)
            p_dmg = round(ps["dmg_sum"] / n)
            patch_stats_out[p_ver] = {
                "picks": n,
                "avg_placement": p_avg,
                "top3_rate": p_top3,
                "win_rate": p_win,
                "avg_damage": p_dmg,
            }
            w = patch_weights.get(p_ver, 0.5)
            eff_n = n * w
            w_picks_sum += eff_n
            w_place_sum += p_avg * eff_n
            w_top3_sum += p_top3 * eff_n
            w_win_sum += p_win * eff_n
            w_dmg_sum += p_dmg * eff_n

        if w_picks_sum > 0:
            avg_placement = round(w_place_sum / w_picks_sum, 2)
            bayes_placement = round((w_picks_sum * avg_placement + 25 * 3.5) / (w_picks_sum + 25), 2)
            top3_rate = round(w_top3_sum / w_picks_sum, 1)
            win_rate = round(w_win_sum / w_picks_sum, 1)
            avg_damage = round(w_dmg_sum / w_picks_sum)
        else:
            avg_placement = round(ov["place_sum"] / total_picks, 2)
            bayes_placement = round((total_picks * avg_placement + 25 * 3.5) / (total_picks + 25), 2)
            top3_rate = round((ov["top3"] / total_picks) * 100.0, 1)
            win_rate = round((ov["win"] / total_picks) * 100.0, 1)
            avg_damage = round(ov["dmg_sum"] / total_picks)

        if total_picks < 20:
            tier = "B"
        elif bayes_placement <= 3.32:
            tier = "S"
        elif bayes_placement <= 3.46:
            tier = "A"
        elif bayes_placement <= 3.60:
            tier = "B"
        else:
            tier = "C"

        guide[c_key] = {
            "champion_name": c_key,
            "kr_name": c_meta.get("kr_name", c_key),
            "roles": c_meta.get("roles", ["Fighter"]),
            "summary": {
                "tier": tier,
                "tier_num": tier_to_num[tier],
                "picks": total_picks,
                "avg_placement": avg_placement,
                "bayes_placement": bayes_placement,
                "top3_rate": top3_rate,
                "win_rate": win_rate,
                "avg_damage": avg_damage,
            },
            "patch_stats": patch_stats_out,
            "best_prismatics": _finalize_item_list(champ_prismatics[c_key], total_picks, limit=20),
            "core_items": _finalize_item_list(champ_legendaries[c_key], total_picks, limit=20),
            "best_augments": _finalize_aug_list(champ_augments[c_key], total_picks, limit=45),
            "best_partners": _finalize_partner_list(champ_partners[c_key], total_picks, limit=12),
        }

    # 베이지안 평균 등수 순 정렬
    sorted_guide = dict(
        sorted(
            guide.items(),
            key=lambda kv: (kv[1]["summary"]["bayes_placement"], -kv[1]["summary"]["picks"]),
        )
    )
    return sorted_guide


# =============================================================================
# 6. 신규 매치 수집 루프 (BFS 탐색)
# =============================================================================
def collect_new_matches(
    client: RiotApiClient,
    existing_matches: Dict[str, Dict[str, Any]],
    target_new: int,
    max_runtime_minutes: int,
) -> int:
    start_ts = time.time()
    deadline_ts = start_ts + max_runtime_minutes * 60

    puuid_queue: deque = deque()
    visited_puuids: Set[str] = set()
    seen_match_ids: Set[str] = set(existing_matches.keys())

    # 기존 체크포인트에 있는 최근 유저를 시드로 우선 활용
    recent_matches = sorted(
        existing_matches.values(),
        key=lambda m: int(m.get("ts", 0) or 0),
        reverse=True,
    )[:40]
    for m in recent_matches:
        for p in m.get("participants", []):
            puuid = p.get("puuid", "")
            if puuid and puuid not in visited_puuids:
                visited_puuids.add(puuid)
                puuid_queue.append(puuid)

    # 시드가 부족하면 KR 상위 랭커 시드 보충
    if len(puuid_queue) < 40:
        log_print("KR 리그 시드 유저 조회 중...")
        for puuid in client.fetch_seed_puuids(max_seeds=100):
            if puuid not in visited_puuids:
                visited_puuids.add(puuid)
                puuid_queue.append(puuid)

    log_print(f"수집 시작 (시드 {len(puuid_queue)}명, 목표 신규 매치 {target_new:,}건)")

    new_count = 0
    while puuid_queue and new_count < target_new and not client.auth_failed:
        if time.time() >= deadline_ts:
            log_print(f"최대 실행 시간({max_runtime_minutes}분) 도달로 수집을 안전하게 종료합니다.")
            break

        puuid = puuid_queue.popleft()
        match_ids = client.fetch_arena_match_ids(puuid, count=30)
        if client.auth_failed:
            break

        for mid in match_ids:
            if mid in seen_match_ids:
                continue
            seen_match_ids.add(mid)

            if time.time() >= deadline_ts or new_count >= target_new or client.auth_failed:
                break

            detail = client.fetch_match_detail(mid)
            if not detail:
                continue

            compact = extract_compact_match(detail)
            if not compact:
                continue

            existing_matches[mid] = compact
            new_count += 1

            # BFS 확장: 해당 매치 참가자를 탐색 큐에 추가
            if len(puuid_queue) < 5000:
                for p in compact.get("participants", []):
                    npuu = p.get("puuid", "")
                    if npuu and npuu not in visited_puuids:
                        visited_puuids.add(npuu)
                        puuid_queue.append(npuu)

            if new_count % 100 == 0:
                elapsed_m = (time.time() - start_ts) / 60.0
                log_print(
                    f"진행 상황: 신규 {new_count:,}/{target_new:,}건 "
                    f"(누적 {len(existing_matches):,}건, 경과 {elapsed_m:.1f}분)"
                )
            if new_count % 300 == 0:
                save_compressed_checkpoint(existing_matches)

    log_print(f"신규 매치 수집 완료: +{new_count:,}건 (총 누적 {len(existing_matches):,}건)")
    return new_count


# =============================================================================
# 7. 메인 진입점
# =============================================================================
def main() -> None:
    parser = argparse.ArgumentParser(description="LoL 아레나 3v3 실측 데이터 수집 및 집계기")
    parser.add_argument("--api-key", default=os.getenv("RIOT_API_KEY", ""), help="Riot API Key")
    parser.add_argument(
        "--target-matches",
        type=int,
        default=int(os.getenv("TARGET_NEW_MATCHES", "3000")),
        help="회당 목표 신규 매치 수",
    )
    parser.add_argument(
        "--max-runtime-minutes",
        type=int,
        default=int(os.getenv("MAX_RUNTIME_MINUTES", "110")),
        help="최대 수집 실행 시간(분)",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=float(os.getenv("REQUEST_DELAY", "1.25")),
        help="API 요청 간 최소 대기 시간(초)",
    )
    parser.add_argument(
        "--rebuild-only",
        action="store_true",
        help="API 수집 없이 기존 체크포인트로 JSON만 즉시 재집계",
    )
    args = parser.parse_args()

    meta = OfficialMetadata()
    meta.load_all()

    matches = load_existing_checkpoint()

    if not args.rebuild_only:
        if args.api_key:
            client = RiotApiClient(api_key=args.api_key, request_delay=args.request_delay)
            collect_new_matches(
                client=client,
                existing_matches=matches,
                target_new=args.target_matches,
                max_runtime_minutes=args.max_runtime_minutes,
            )
        else:
            log_print("[안내] RIOT_API_KEY가 없어 기존 체크포인트 재집계만 수행합니다.")

    if not matches:
        log_print("[오류] 집계할 매치 데이터가 없습니다.")
        sys.exit(1)

    save_compressed_checkpoint(matches)

    guide = build_guide_json(matches, meta)
    with open(OUTPUT_JSON_PATH, "wt", encoding="utf-8") as f:
        json.dump(guide, f, ensure_ascii=False, indent=2)

    log_print(
        f"최종 가이드 JSON 저장 완료: {OUTPUT_JSON_PATH.name} "
        f"(챔피언 {len(guide)}명, 누적 매치 {len(matches):,}건)"
    )


if __name__ == "__main__":
    main()
