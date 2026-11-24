#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
LoL 아레나 3v3 (6팀 · 18인 모드) 자동 수집 및 통합 집계기
=======================================================
- 모든 통계(승률, 평균 순위, 1등 확률, 픽률, 시너지)는 오직 직접 수집한 Riot Match-V5 데이터로만 계산합니다.
- 챔피언/아이템/증강 명칭과 등급(실버·골드·프리즘)만 Riot 공식 한국어 클라이언트 데이터로 동기화합니다.
  1순위: Riot LCU 공식 ko_kr/v1/cherry-augments.json (nameTRA + kSilver/kGold/kPrismatic)
  2순위: CommunityDragon cdragon/arena/ko_kr.json (프리즘 아이템 및 누락 증강 보완)
  3순위: 한국어 공식 명칭/등급 교차 검증 사전 (통계 없음, 오직 명칭·등급 보정용)
- 체크포인트는 GitHub 100MB 제한 회피를 위해 gzip 압축(arena_3v3_checkpoint.jsonl.gz)으로 자동 저장됩니다.
"""

import gzip
import json
import math
import os
import time
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

# =============================================================================
# 1. 실행 설정 (Google Colab / 로컬 / GitHub Actions 공용)
# =============================================================================


def _env_bool(key: str, default: bool) -> bool:
    raw = os.environ.get(key)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(key: str, default: int) -> int:
    raw = os.environ.get(key)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


def _env_csv_list(key: str, default: List[str]) -> List[str]:
    raw = os.environ.get(key)
    if raw is None or not raw.strip():
        return default
    return [item.strip() for item in raw.split(",") if item.strip()]


COLAB_CONFIG: Dict[str, Any] = {
    # Riot Developer Portal API 키 (GitHub Actions에서는 Secrets의 RIOT_API_KEY 자동 주입)
    "api_key": os.environ.get("RIOT_API_KEY", "RGAPI-YOUR-KEY-HERE"),
    # 라우팅 리전 (한국 서버: asia)
    "routing_region": os.environ.get("ROUTING_REGION", "asia"),
    # True: 기존 체크포인트만 읽어 즉시 재집계
    # False: Riot Match-V5 API로 신규 3v3 아레나 매치를 수집한 뒤 통합 집계
    "aggregate_only": _env_bool("AGGREGATE_ONLY", True),
    # 신규 수집할 최대 매치 수 (aggregate_only=False일 때 적용)
    "max_new_matches": _env_int("MAX_NEW_MATCHES", 500),
    # 아레나 큐 ID (1750: 3v3 아레나, 1700: 아레나 공통)
    "target_queue_ids": [1750, 1700],
    # 수집할 패치 버전 (빈 리스트 []이면 모든 패치 허용)
    "target_patches": _env_csv_list("TARGET_PATCHES", []),
    # 체크포인트가 없을 때 크롤링 시작용 시드 소환사 Riot ID (gameName#tagLine)
    "seed_riot_ids": _env_csv_list("SEED_RIOT_IDS", ["Hide on bush#KR1"]),
    # 입출력 파일 경로 (GitHub 100MB 제한 대응: .jsonl.gz 압축 기본 사용)
    "checkpoint_path": os.environ.get("CHECKPOINT_PATH", "arena_3v3_checkpoint.jsonl.gz"),
    "output_json_path": os.environ.get("OUTPUT_JSON_PATH", "arena_3v3_guide_all_champs.json"),
    # 증강 집계 최소 표본 수 및 등급별 최대 출력 개수
    "min_aug_picks": _env_int("MIN_AUG_PICKS", 3),
    "max_augs_per_rarity": _env_int("MAX_AUGS_PER_RARITY", 10),
}

# =============================================================================
# 2. 공식 한국어 명칭 & 등급 사전 (통계 없음 · 오직 명칭/등급 정규화 용도)
# =============================================================================

CDRAGON_ASSET_PREFIX = (
    "https://raw.communitydragon.org/latest/plugins/rcp-be-lol-game-data/global/default/"
)
CHERRY_AUGMENTS_KO_URL = (
    "https://raw.communitydragon.org/latest/plugins/rcp-be-lol-game-data/global/ko_kr/v1/cherry-augments.json"
)
CDRAGON_ARENA_KO_URL = (
    "https://raw.communitydragon.org/latest/cdragon/arena/ko_kr.json"
)

# 아레나 프리즘 아이템 공식 한국어 명칭 목록
KNOWN_PRISMATIC_ITEMS_KO: Set[str] = {
    "블랙홀 건틀릿", "별빛밤 망토", "부서진 여왕의 왕관", "잔혹 행위", "흑강철 발톱",
    "참수자", "불사대마왕의 왕관", "악마의 포옹", "폭발의 구", "다이아몬드 창",
    "신성한 파괴자", "용의 심장", "드락사르의 황혼검", "일라이자의 기적", "창공의 서약",
    "만년서리", "살점포식자", "엔트로피의 힘", "질책", "돌풍", "도박꾼의 칼날",
    "가고일 돌갑옷", "불귀신", "혈마법사의 투구", "마공화살 동료", "활력증진의 펜던트",
    "킨코우 십수", "신기루 검", "달빛 마법검", "번개 막대", "밤의 수확자", "자객의 발톱",
    "조종의 손아귀", "화염술사의 망토", "광휘의 미덕", "현실 균열", "사신의 대가",
    "섭정 시해", "반향", "룬 조각기", "핏빛 선물", "용암의 방패", "신성의 검",
    "승천의 부적", "터보 화공 탱크", "황혼의 끝자락", "쌍둥이 가면", "워모그의 갑옷",
}

# 한국어 클라이언트 기준 공식 증강 등급 검증 사전 (숫자 enum 밀림 방지용 앵커)
OFFICIAL_AUGMENT_RARITY_KO: Dict[str, str] = {
    # 프리즘 (Prismatic)
    "감바 모루": "프리즘", "거석상의 용기": "프리즘", "거인": "프리즘", "거인 학살자": "프리즘",
    "공포를 부르는 자": "프리즘", "광대 대학": "프리즘", "돌진": "프리즘", "보석 건틀릿": "프리즘",
    "신비한 주먹": "프리즘", "양손잡이": "프리즘", "유레카": "프리즘", "전장의 교향곡": "프리즘",
    "정조준 일격 신동": "프리즘", "주문 각성": "프리즘", "지옥의 전도체": "프리즘", "최종 형태": "프리즘",
    "탄환 세례": "프리즘", "탭 댄서": "프리즘", "파멸의 예언자": "프리즘", "거인의 결의": "프리즘",
    "검무": "프리즘", "검을 뽑아라": "프리즘", "결투의 대가": "프리즘", "대지의 각성": "프리즘",
    "마법 소녀": "프리즘", "마트료시카": "프리즘", "불길한 서약": "프리즘", "야생 불꽃": "프리즘",
    "양자 연산": "프리즘", "우주의 중심": "프리즘", "유리 대포": "프리즘", "저주의 화염": "프리즘",
    "전환: 혼돈": "프리즘", "정신 결속": "프리즘", "찜솥": "프리즘", "최상급 조준경 부착": "프리즘",
    "케르베로스": "프리즘", "퀘스트: 우글렛의 마녀 모자": "프리즘", "프로틴 음료": "프리즘",
    "궁극기 대변혁": "프리즘", "궁극의 룰렛": "프리즘", "궤도형 레이저": "프리즘", "끝판왕": "프리즘",
    "난공불락": "프리즘", "마법 가속": "프리즘", "미친 과학자": "프리즘", "세월의 지혜": "프리즘",
    "요정 마법": "프리즘", "이케시아의 몰락": "프리즘", "전능의 영혼": "프리즘", "정의로운 분노": "프리즘",
    "죽음의 순환": "프리즘", "능력치 더하기 능력치 더하기 능력치!": "프리즘",
    # 골드 (Gold)
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
    # 실버 (Silver)
    "강타자": "실버", "응급처치 키트": "실버", "능수능란": "실버", "음속 폭발": "실버",
    "신성 모독자": "실버", "서리 망령": "실버", "고문자": "실버", "죄책감의 쾌락": "실버",
    "우당탕탕": "실버", "빨랫줄": "실버", "점액 천지": "실버", "불여우": "실버",
    "주스 압착기": "실버", "에너지 충전": "실버", "약탈": "실버",
}

# 구 명칭 -> 공식 한국어 명칭 치환 사전
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
}

# 코어 아이템 집계에서 제외할 시작 아이템·소모품·모루
EXCLUDED_CORE_ITEMS_KO: Set[str] = {
    "수호자의 뿔피리", "수호자의 검", "수호자의 보주", "수호자의 망치",
    "수호자의 단검", "수호자의 부적", "수호자의 수의",
    "능력치 모루", "전설급 아이템 모루", "프리즘 아이템 모루",
    "전설 전사 아이템", "전설 탱커 아이템", "전설 마법사 아이템",
    "전설 암살자 아이템", "전설 원거리 딜러 아이템", "전설 서포터 아이템",
    "프리즘 아이템", "힘의 주스", "활력의 주스", "민첩의 주스", "모자 주스",
    "투명 와드", "예언자의 렌즈", "망원형 개조", "장화", "충전형 물약", "체력 물약",
}

ROLE_KO_MAP: Dict[str, str] = {
    "Fighter": "전사",
    "Mage": "마법사",
    "Assassin": "암살자",
    "Marksman": "원거리 딜러",
    "Support": "서포터",
    "Tank": "탱커",
}

# =============================================================================
# 3. 공용 유틸리티 함수
# =============================================================================


def log_print(msg: str) -> None:
    """타임스탬프와 함께 즉시 플러시하여 출력합니다."""
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {msg}", flush=True)


def normalize_cdragon_asset(raw_path: str) -> str:
    """CommunityDragon 내부 에셋 경로를 HTTPS URL로 변환합니다."""
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


def normalize_augment_name(raw_name: str) -> str:
    """비공식/구 증강 명칭을 Riot 한국어 클라이언트 공식 명칭으로 정규화합니다."""
    cleaned = str(raw_name or "").strip()
    return AUGMENT_NAME_CORRECTIONS.get(cleaned, cleaned)


def extract_patch_version(game_version: str) -> str:
    """Match-V5 gameVersion('16.23.xxx.xxxx')에서 '메이저.마이너'만 추출합니다."""
    parts = str(game_version or "").split(".")
    if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
        return f"{parts[0]}.{parts[1]}"
    return "unknown"


def patch_sort_key(patch: str) -> Tuple[int, int]:
    """패치 문자열을 최신순 정렬 가능한 숫자 튜플로 변환합니다."""
    parts = str(patch or "").split(".")
    try:
        return (int(parts[0]), int(parts[1]))
    except (ValueError, IndexError):
        return (0, 0)


def calibrate_numeric_rarity_map(augments_list: List[Dict[str, Any]]) -> Dict[int, str]:
    """앵커 증강의 실제 숫자 코드를 다수결 투표해 숫자 등급 체계를 자동 보정합니다."""
    votes: Dict[int, Counter] = defaultdict(Counter)
    int_values: Set[int] = set()

    for aug in augments_list:
        raw_r = aug.get("rarity")
        if not isinstance(raw_r, int):
            continue
        int_values.add(raw_r)
        name = normalize_augment_name(aug.get("nameTRA") or aug.get("name") or "")
        known = OFFICIAL_AUGMENT_RARITY_KO.get(name)
        if known:
            votes[raw_r][known] += 1

    mapping: Dict[int, str] = {}
    for code, counter in votes.items():
        if counter:
            mapping[code] = counter.most_common(1)[0][0]

    if len(mapping) < 3:
        sorted_vals = sorted(int_values)
        if sorted_vals == [0, 1, 2]:
            mapping.setdefault(0, "실버")
            mapping.setdefault(1, "골드")
            mapping.setdefault(2, "프리즘")
        elif {4, 8}.issubset(int_values):
            mapping.setdefault(0, "실버")
            mapping.setdefault(1, "실버")
            mapping.setdefault(4, "골드")
            mapping.setdefault(8, "프리즘")
        elif len(sorted_vals) >= 3:
            mapping.setdefault(sorted_vals[0], "실버")
            mapping.setdefault(sorted_vals[1], "골드")
            mapping.setdefault(sorted_vals[2], "프리즘")

    return mapping


def normalize_rarity_ko(
    raw_rarity: Any,
    numeric_rarity_map: Optional[Dict[int, str]] = None,
    aug_name: str = "",
) -> str:
    """증강 등급 코드(kSilver/kGold/kPrismatic 또는 숫자)를 공식 한국어 등급으로 변환합니다."""
    clean_name = normalize_augment_name(aug_name)
    if clean_name in OFFICIAL_AUGMENT_RARITY_KO:
        return OFFICIAL_AUGMENT_RARITY_KO[clean_name]

    if isinstance(raw_rarity, str):
        text = raw_rarity.strip().lower()
        if "prism" in text or "프리즘" in text:
            return "프리즘"
        if "gold" in text or "골드" in text:
            return "골드"
        if "silver" in text or "실버" in text:
            return "실버"
        try:
            raw_rarity = int(text)
        except ValueError:
            return "실버"

    if isinstance(raw_rarity, int):
        if numeric_rarity_map and raw_rarity in numeric_rarity_map:
            return numeric_rarity_map[raw_rarity]
        if raw_rarity in (2, 8):
            return "프리즘"
        if raw_rarity in (1, 4):
            return "골드"
        return "실버"

    return "실버"


def bayes_smooth(n: float, avg_val: float, m: float = 25.0, prior: float = 3.5) -> float:
    """소표본 극단값을 보정하는 베이지안 스무딩을 계산합니다."""
    if n <= 0:
        return prior
    return (n * avg_val + m * prior) / (n + m)


def compute_composite_augment_score(
    aug_picks: int,
    champ_picks: int,
    raw_avg_place: float,
    raw_win_rate: float,
    champ_avg_place: float,
    champ_win_rate: float,
) -> Dict[str, float]:
    """직접 수집한 픽률(35%) + 1등 확률(35%) + 평균 순위(30%)로 종합 점수를 산출합니다."""
    pick_rate = round((aug_picks / max(champ_picks, 1)) * 100.0, 1)
    m = 12.0
    bayes_place = round(bayes_smooth(aug_picks, raw_avg_place, m=m, prior=champ_avg_place), 2)
    bayes_win = round(bayes_smooth(aug_picks, raw_win_rate, m=m, prior=champ_win_rate), 1)

    win_score = min(max((bayes_win / 42.0) * 100.0, 0.0), 100.0)
    place_score = min(max(((4.5 - bayes_place) / 2.5) * 100.0, 0.0), 100.0)
    pick_score = min(max((math.log1p(pick_rate) / math.log1p(24.0)) * 100.0, 0.0), 100.0)

    composite = round(win_score * 0.35 + place_score * 0.30 + pick_score * 0.35, 1)
    return {
        "pick_rate": pick_rate,
        "bayes_placement": bayes_place,
        "bayes_win_rate": bayes_win,
        "composite_score": composite,
    }


# =============================================================================
# 4. Riot 공식 메타데이터 로더 (명칭 · 등급 · 아이콘 동기화)
# =============================================================================


class GameMetadata:
    """Riot Data Dragon(챔피언·아이템) + LCU cherry-augments.json(증강 명칭·등급) 로더"""

    def __init__(self) -> None:
        self.dd_version: str = "16.23.1"
        self.champions_by_id: Dict[str, Dict[str, Any]] = {}
        self.lower_champ_to_canonical: Dict[str, str] = {}
        self.items_by_id: Dict[int, Dict[str, Any]] = {}
        self.item_name_to_latest_id: Dict[str, int] = {}
        self.cdragon_item_icon_by_name: Dict[str, str] = {}
        self.prismatic_item_ids: Set[int] = set()
        self.prismatic_item_names: Set[str] = set(KNOWN_PRISMATIC_ITEMS_KO)
        self.augments_by_id: Dict[int, Dict[str, Any]] = {}
        self.augments_by_name: Dict[str, Dict[str, Any]] = {}

        self._load_ddragon()
        self._load_cdragon_arena()

    def _load_ddragon(self) -> None:
        log_print("[메타데이터] Riot Data Dragon 최신 버전·챔피언·아이템 명칭 동기화 중...")
        try:
            ver_resp = requests.get("https://ddragon.leagueoflegends.com/api/versions.json", timeout=20)
            ver_resp.raise_for_status()
            versions = ver_resp.json()
            if versions:
                self.dd_version = versions[0]

            champ_url = f"https://ddragon.leagueoflegends.com/cdn/{self.dd_version}/data/ko_KR/champion.json"
            champ_resp = requests.get(champ_url, timeout=25)
            champ_resp.raise_for_status()
            for key, info in (champ_resp.json().get("data") or {}).items():
                self.champions_by_id[key] = {
                    "id": key,
                    "kr_name": info.get("name", key),
                    "roles": info.get("tags") or ["Fighter"],
                }
                self.lower_champ_to_canonical[key.lower()] = key

            item_url = f"https://ddragon.leagueoflegends.com/cdn/{self.dd_version}/data/ko_KR/item.json"
            item_resp = requests.get(item_url, timeout=25)
            item_resp.raise_for_status()
            item_priority: Dict[str, int] = {}

            for id_str, info in (item_resp.json().get("data") or {}).items():
                try:
                    iid = int(id_str)
                except ValueError:
                    continue
                name = str(info.get("name") or "").strip()
                if not name:
                    continue

                if 1000 <= iid < 7000:
                    prio = 3
                elif (220000 <= iid < 230000) or (440000 <= iid < 450000):
                    prio = 2
                else:
                    prio = 1

                if name not in self.item_name_to_latest_id or prio > item_priority.get(name, 0):
                    self.item_name_to_latest_id[name] = iid
                    item_priority[name] = prio

                gold_total = int((info.get("gold") or {}).get("total") or 0)
                into_list = info.get("into") or []
                is_prismatic = (name in self.prismatic_item_names) or (440000 <= iid < 450000)
                is_consumable = (name in EXCLUDED_CORE_ITEMS_KO) or bool(info.get("consumed"))
                is_completed = (
                    not is_consumable
                    and not is_prismatic
                    and (gold_total >= 900 or "Boots" in (info.get("tags") or []))
                    and len(into_list) == 0
                )

                self.items_by_id[iid] = {
                    "id": iid,
                    "name": name,
                    "icon_url": f"https://ddragon.leagueoflegends.com/cdn/{self.dd_version}/img/item/{iid}.png",
                    "is_completed": is_completed,
                    "is_consumable": is_consumable,
                    "is_prismatic": is_prismatic,
                }
                if is_prismatic:
                    self.prismatic_item_ids.add(iid)
                    self.prismatic_item_names.add(name)

            log_print(
                f"  -> Data Dragon v{self.dd_version}: 챔피언 {len(self.champions_by_id)}명, "
                f"아이템 {len(self.items_by_id)}개 동기화 완료"
            )
        except Exception as exc:
            log_print(f"  [경고] Data Dragon 로드 실패: {exc}")

    def _load_cdragon_arena(self) -> None:
        log_print("[메타데이터] Riot LCU cherry-augments.json (ko_kr) 증강 명칭·등급 동기화 중...")

        # 1순위: Riot 클라이언트 공식 cherry-augments.json
        try:
            cherry_resp = requests.get(CHERRY_AUGMENTS_KO_URL, timeout=25)
            cherry_resp.raise_for_status()
            cherry_list = cherry_resp.json()
            if isinstance(cherry_list, list):
                cherry_num_map = calibrate_numeric_rarity_map(cherry_list)
                for aug in cherry_list:
                    try:
                        aug_id = int(aug.get("id", 0))
                    except (TypeError, ValueError):
                        continue
                    name = normalize_augment_name(aug.get("nameTRA") or aug.get("name") or "")
                    if not aug_id or not name or name == "???":
                        continue
                    rarity_ko = normalize_rarity_ko(aug.get("rarity"), cherry_num_map, aug_name=name)
                    icon_url = normalize_cdragon_asset(
                        aug.get("augmentSmallIconPath") or aug.get("augmentLargeIconPath") or ""
                    )
                    entry = {
                        "id": aug_id,
                        "name": name,
                        "rarity": rarity_ko,
                        "icon_url": icon_url,
                    }
                    self.augments_by_id[aug_id] = entry
                    self.augments_by_name[name] = entry

                dist = Counter(a["rarity"] for a in self.augments_by_id.values())
                log_print(f"  -> [1순위] cherry-augments.json {len(self.augments_by_id)}개 로드 (분포: {dict(dist)})")
        except Exception as exc:
            log_print(f"  [경고] cherry-augments.json 로드 실패 -> cdragon/arena 폴백: {exc}")

        # 2순위: CommunityDragon arena/ko_kr.json (프리즘 아이템 아이콘 + 누락 증강 보완)
        try:
            resp = requests.get(CDRAGON_ARENA_KO_URL, timeout=25)
            resp.raise_for_status()
            payload = resp.json()

            raw_augments = payload.get("augments", [])
            numeric_rarity_map = calibrate_numeric_rarity_map(raw_augments)

            for aug in raw_augments:
                try:
                    aug_id = int(aug.get("id", 0))
                except (TypeError, ValueError):
                    continue
                name = normalize_augment_name(aug.get("name", ""))
                if not aug_id or not name or name == "???":
                    continue

                rarity_ko = normalize_rarity_ko(aug.get("rarity"), numeric_rarity_map, aug_name=name)
                icon_url = normalize_cdragon_asset(aug.get("iconSmall") or aug.get("iconLarge") or "")

                if aug_id in self.augments_by_id:
                    if not self.augments_by_id[aug_id].get("icon_url") and icon_url:
                        self.augments_by_id[aug_id]["icon_url"] = icon_url
                    continue

                entry = {
                    "id": aug_id,
                    "name": name,
                    "rarity": rarity_ko,
                    "icon_url": icon_url,
                }
                self.augments_by_id[aug_id] = entry
                if name not in self.augments_by_name:
                    self.augments_by_name[name] = entry

            for item in payload.get("items", []):
                try:
                    iid = int(item.get("id", 0))
                except (TypeError, ValueError):
                    continue
                name = str(item.get("name", "")).strip()
                if not iid or not name:
                    continue

                icon_url = normalize_cdragon_asset(item.get("iconPath") or "")
                if icon_url:
                    self.cdragon_item_icon_by_name[name] = icon_url

                is_prismatic = (
                    name in self.prismatic_item_names
                    or (440000 <= iid < 450000 and name not in EXCLUDED_CORE_ITEMS_KO)
                    or str(item.get("epicness", "")).lower() == "prismatic"
                )
                if is_prismatic:
                    self.prismatic_item_ids.add(iid)
                    self.prismatic_item_names.add(name)

                existing = self.items_by_id.get(iid)
                if existing:
                    existing["is_prismatic"] = existing["is_prismatic"] or is_prismatic
                    if (is_prismatic or not existing.get("icon_url")) and icon_url:
                        existing["icon_url"] = icon_url
                else:
                    self.items_by_id[iid] = {
                        "id": iid,
                        "name": name,
                        "icon_url": icon_url,
                        "is_completed": (name not in EXCLUDED_CORE_ITEMS_KO) and (not is_prismatic),
                        "is_consumable": name in EXCLUDED_CORE_ITEMS_KO,
                        "is_prismatic": is_prismatic,
                    }

            final_dist = Counter(a["rarity"] for a in self.augments_by_id.values())
            log_print(
                f"  -> 최종 공식 증강 {len(self.augments_by_id)}개 (분포: {dict(final_dist)}), "
                f"프리즘 아이템 {len(self.prismatic_item_names)}개 동기화 완료"
            )
        except Exception as exc:
            log_print(f"  [경고] CommunityDragon 아레나 데이터 로드 실패: {exc}")

    def resolve_champion_key(self, raw_key: str) -> str:
        """챔피언 ID 대소문자 정규화 (예: FiddleSticks -> Fiddlesticks)"""
        cleaned = str(raw_key or "").strip()
        if not cleaned:
            return ""
        return self.lower_champ_to_canonical.get(cleaned.lower(), cleaned)

    def resolve_augment(self, aug_id: int) -> Optional[Dict[str, Any]]:
        """증강 ID로 Riot 공식 한국어 증강 정보를 조회합니다."""
        return self.augments_by_id.get(int(aug_id))


# =============================================================================
# 5. Riot Match-V5 API 클라이언트
# =============================================================================


class RiotApiClient:
    """Riot Account-V1 / Match-V5 안전 호출 래퍼 (429 자동 대기 지원)"""

    def __init__(self, api_key: str, routing_region: str = "asia") -> None:
        self.api_key = api_key.strip()
        self.routing_region = routing_region.strip().lower()
        self.session = requests.Session()
        self.session.headers.update({"X-Riot-Token": self.api_key})

    def _get_json(self, url: str, params: Optional[Dict[str, Any]] = None, retries: int = 5) -> Optional[Any]:
        for attempt in range(retries):
            try:
                resp = self.session.get(url, params=params, timeout=20)
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 429:
                    wait_sec = int(resp.headers.get("Retry-After", 5)) + 1
                    log_print(f"  [429 레이트 리밋] {wait_sec}초 대기 후 재시도...")
                    time.sleep(wait_sec)
                    continue
                if resp.status_code in (401, 403):
                    raise PermissionError(f"Riot API 키 인증 실패 (HTTP {resp.status_code}).")
                if resp.status_code == 404:
                    return None
                time.sleep(1.5 * (attempt + 1))
            except requests.RequestException:
                time.sleep(2.0 * (attempt + 1))
        return None

    def get_puuid_by_riot_id(self, game_name: str, tag_line: str) -> Optional[str]:
        url = (
            f"https://{self.routing_region}.api.riotgames.com/"
            f"riot/account/v1/accounts/by-riot-id/{requests.utils.quote(game_name)}/{requests.utils.quote(tag_line)}"
        )
        data = self._get_json(url)
        return data.get("puuid") if isinstance(data, dict) else None

    def get_match_ids_by_puuid(self, puuid: str, count: int = 40) -> List[str]:
        url = f"https://{self.routing_region}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
        data = self._get_json(url, params={"start": 0, "count": count})
        return data if isinstance(data, list) else []

    def get_match(self, match_id: str) -> Optional[Dict[str, Any]]:
        url = f"https://{self.routing_region}.api.riotgames.com/lol/match/v5/matches/{match_id}"
        data = self._get_json(url)
        return data if isinstance(data, dict) else None


# =============================================================================
# 6. 3v3 아레나 매치 파싱 및 체크포인트 로드/저장 (gzip 압축 지원)
# =============================================================================


def parse_arena_3v3_match(
    match_json: Dict[str, Any],
    meta: GameMetadata,
    target_queue_ids: Set[int],
    target_patches: Set[str],
) -> List[Dict[str, Any]]:
    """Match-V5 응답에서 3v3(6팀 18인) 아레나 참가자 데이터를 추출합니다."""
    info = match_json.get("info") or {}
    metadata = match_json.get("metadata") or {}
    match_id = str(metadata.get("matchId") or "")
    queue_id = int(info.get("queueId") or 0)

    if target_queue_ids and queue_id not in target_queue_ids:
        return []

    patch = extract_patch_version(info.get("gameVersion", ""))
    if target_patches and patch not in target_patches:
        return []

    participants = info.get("participants") or []
    subteams: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    for p in participants:
        st_id = int(p.get("playerSubteamId") or 0)
        if st_id > 0:
            subteams[st_id].append(p)

    # 3인 1팀(3v3 아레나) 검증
    if not subteams or any(len(members) != 3 for members in subteams.values()):
        return []

    game_creation_ms = int(info.get("gameCreation") or 0)
    game_time = (
        datetime.fromtimestamp(game_creation_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        if game_creation_ms
        else ""
    )

    records: List[Dict[str, Any]] = []
    for p in participants:
        subteam_id = int(p.get("playerSubteamId") or 0)
        placement = int(p.get("subteamPlacement") or p.get("placement") or 0)
        if placement <= 0 or placement > 6:
            continue

        champ_key = meta.resolve_champion_key(p.get("championName", ""))
        if not champ_key:
            continue

        combat_augments: List[str] = []
        augment_ids: List[int] = []
        for idx in range(1, 7):
            aug_id = int(p.get(f"playerAugment{idx}") or 0)
            if aug_id <= 0:
                continue
            augment_ids.append(aug_id)
            aug_info = meta.resolve_augment(aug_id)
            if aug_info:
                combat_augments.append(aug_info["name"])

        prismatic_items: List[str] = []
        core_items: List[Dict[str, Any]] = []
        for slot in range(0, 6):
            iid = int(p.get(f"item{slot}") or 0)
            if iid <= 0:
                continue
            item_info = meta.items_by_id.get(iid)
            if not item_info:
                continue
            iname = item_info["name"]
            if iname in EXCLUDED_CORE_ITEMS_KO:
                continue
            if item_info.get("is_prismatic") or iname in meta.prismatic_item_names:
                if iname not in prismatic_items:
                    prismatic_items.append(iname)
            elif item_info.get("is_completed", True):
                latest_id = meta.item_name_to_latest_id.get(iname, iid)
                core_items.append({"name": iname, "id": latest_id})

        partners = [
            meta.resolve_champion_key(mate.get("championName", ""))
            for mate in subteams.get(subteam_id, [])
            if mate.get("puuid") != p.get("puuid") and mate.get("championName")
        ]

        records.append({
            "match_id": match_id,
            "patch": patch,
            "queue_id": queue_id,
            "team_size": 3,
            "game_time": game_time,
            "puuid": str(p.get("puuid") or ""),
            "champion_name": champ_key,
            "subteam_id": subteam_id,
            "placement": placement,
            "is_top_half": placement <= 3,
            "is_win": placement == 1,
            "damage_dealt": int(p.get("totalDamageDealtToChampions") or 0),
            "gold_earned": int(p.get("goldEarned") or 0),
            "augment_ids": augment_ids,
            "combat_augments": combat_augments,
            "prismatic_items": prismatic_items,
            "core_items": core_items,
            "partner_champions": partners,
        })

    return records


def normalize_checkpoint_row(row: Dict[str, Any], meta: GameMetadata) -> Optional[Dict[str, Any]]:
    """체크포인트 JSONL의 각 행을 최신 공식 증강명/챔피언 키로 정규화합니다."""
    match_id = str(row.get("match_id") or "")
    puuid = str(row.get("puuid") or "")
    placement = int(row.get("placement") or 0)
    if not match_id or placement <= 0 or placement > 6:
        return None

    patch = str(row.get("patch") or "unknown").strip()
    champ_key = meta.resolve_champion_key(row.get("champion_name", ""))
    if not champ_key:
        return None

    # augment_ids가 있으면 공식 cherry-augments.json 이름으로 100% 정확하게 재매핑
    raw_aug_ids = [
        int(aid) for aid in (row.get("augment_ids") or [])
        if str(aid).isdigit() and int(aid) > 0
    ]
    if raw_aug_ids:
        resolved_names: List[str] = []
        for aid in raw_aug_ids:
            info_a = meta.resolve_augment(aid)
            if info_a:
                resolved_names.append(info_a["name"])
        combat_augments = resolved_names or [
            normalize_augment_name(a) for a in (row.get("combat_augments") or []) if a
        ]
    else:
        combat_augments = [
            normalize_augment_name(a) for a in (row.get("combat_augments") or []) if a
        ]

    prismatics = list(row.get("prismatic_items") or [])
    core_items: List[Dict[str, Any]] = []
    for c_item in (row.get("core_items") or []):
        iname = str(c_item.get("name") if isinstance(c_item, dict) else c_item).strip()
        if not iname or iname in EXCLUDED_CORE_ITEMS_KO:
            continue
        if iname in meta.prismatic_item_names:
            prismatics.append(iname)
        else:
            iid = (c_item.get("id") if isinstance(c_item, dict) else None) or meta.item_name_to_latest_id.get(iname)
            core_items.append({"name": iname, "id": iid})

    return {
        "match_id": match_id,
        "patch": patch,
        "queue_id": int(row.get("queue_id", 1750)),
        "team_size": 3,
        "game_time": row.get("game_time", ""),
        "puuid": puuid,
        "champion_name": champ_key,
        "subteam_id": int(row.get("subteam_id", 0)),
        "placement": placement,
        "is_top_half": bool(row.get("is_top_half", placement <= 3)),
        "is_win": bool(row.get("is_win", placement == 1)),
        "damage_dealt": int(row.get("damage_dealt", 0)),
        "gold_earned": int(row.get("gold_earned", 0)),
        "augment_ids": raw_aug_ids,
        "combat_augments": combat_augments,
        "prismatic_items": list(dict.fromkeys(prismatics)),
        "core_items": core_items,
        "partner_champions": [
            meta.resolve_champion_key(p_name)
            for p_name in (row.get("partner_champions") or [])
            if p_name
        ],
    }


def _open_checkpoint_read(filepath: str):
    if filepath.endswith(".gz"):
        return gzip.open(filepath, "rt", encoding="utf-8")
    return open(filepath, "r", encoding="utf-8")


def save_checkpoint_compact(filepath: str, records: List[Dict[str, Any]]) -> None:
    """전체 레코드를 단일 스트림으로 깔끔하게 저장(gzip 사용 시 최대 압축)합니다."""
    if not records:
        return
    tmp_path = filepath + ".tmp"
    opener = (
        (lambda p: gzip.open(p, "wt", encoding="utf-8", compresslevel=6))
        if filepath.endswith(".gz")
        else (lambda p: open(p, "w", encoding="utf-8"))
    )
    with opener(tmp_path) as fp:
        for r in records:
            fp.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")
    os.replace(tmp_path, filepath)
    size_mb = os.path.getsize(filepath) / (1024 * 1024)
    log_print(f"[체크포인트 압축 저장] '{filepath}' ({size_mb:.2f} MB, {len(records):,}건)")


def load_checkpoint(filepath: str, meta: GameMetadata) -> Tuple[List[Dict[str, Any]], Set[str], Set[str]]:
    """압축(.jsonl.gz) 및 기존 비압축(.jsonl) 체크포인트를 모두 읽어 중복 제거 후 자동 마이그레이션합니다."""
    records: List[Dict[str, Any]] = []
    seen_match_ids: Set[str] = set()
    seen_puuids: Set[str] = set()
    seen_row_keys: Set[Tuple[str, str, str]] = set()

    candidate_files: List[str] = []
    legacy_plain_path = filepath[:-3] if filepath.endswith(".gz") else ""
    if legacy_plain_path and os.path.exists(legacy_plain_path):
        candidate_files.append(legacy_plain_path)
    if os.path.exists(filepath):
        candidate_files.append(filepath)

    if not candidate_files:
        return records, seen_match_ids, seen_puuids

    for path in candidate_files:
        with _open_checkpoint_read(path) as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError:
                    continue
                norm = normalize_checkpoint_row(raw, meta)
                if not norm:
                    continue
                row_key = (norm["match_id"], norm["puuid"], norm["champion_name"])
                if row_key in seen_row_keys:
                    continue
                seen_row_keys.add(row_key)
                records.append(norm)
                seen_match_ids.add(norm["match_id"])
                if norm["puuid"]:
                    seen_puuids.add(norm["puuid"])

    log_print(
        f"[체크포인트] 로드 완료: {len(records):,}건 ({len(seen_match_ids):,}개 매치) <- {candidate_files}"
    )

    # 기존 비압축 .jsonl 파일이 있었다면 .jsonl.gz로 통합 압축 저장 후 비압축 원본 삭제
    if legacy_plain_path and os.path.exists(legacy_plain_path) and filepath.endswith(".gz"):
        log_print(f"[자동 마이그레이션] '{legacy_plain_path}' -> '{filepath}' 압축 변환 중...")
        save_checkpoint_compact(filepath, records)
        try:
            os.remove(legacy_plain_path)
            log_print(f"[자동 마이그레이션] 기존 비압축 파일 '{legacy_plain_path}' 정리 완료")
        except OSError as exc:
            log_print(f"[경고] 비압축 원본 삭제 실패: {exc}")

    return records, seen_match_ids, seen_puuids


def append_checkpoint(filepath: str, new_records: List[Dict[str, Any]]) -> None:
    """신규 매치 레코드를 체크포인트 파일에 추가 기록합니다 (.gz 지원)."""
    if not new_records:
        return
    if filepath.endswith(".gz"):
        with gzip.open(filepath, "at", encoding="utf-8", compresslevel=6) as fp:
            for r in new_records:
                fp.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")
    else:
        with open(filepath, "a", encoding="utf-8") as fp:
            for r in new_records:
                fp.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")


# =============================================================================
# 7. Riot API 크롤러 (aggregate_only=False일 때만 실행)
# =============================================================================


def crawl_new_matches(
    config: Dict[str, Any],
    meta: GameMetadata,
    existing_records: List[Dict[str, Any]],
    seen_match_ids: Set[str],
    seen_puuids: Set[str],
) -> Tuple[List[Dict[str, Any]], int]:
    api_key = str(config.get("api_key") or "").strip()
    if not api_key or api_key == "RGAPI-YOUR-KEY-HERE":
        log_print("[오류] 유효한 Riot API 키가 설정되지 않았습니다.")
        return existing_records, 0

    client = RiotApiClient(api_key=api_key, routing_region=config.get("routing_region", "asia"))
    queue_puuids: deque = deque(list(seen_puuids)[:200])
    visited_puuids: Set[str] = set()

    if not queue_puuids:
        for riot_id in config.get("seed_riot_ids") or []:
            if "#" not in riot_id:
                continue
            gname, tag = riot_id.split("#", 1)
            puuid = client.get_puuid_by_riot_id(gname.strip(), tag.strip())
            if puuid:
                queue_puuids.append(puuid)
                log_print(f"  -> 시드 소환사 '{riot_id}' PUUID 확보 완료")

    max_new = int(config.get("max_new_matches", 300))
    target_queues = set(config.get("target_queue_ids") or [])
    target_patches = set(config.get("target_patches") or [])
    checkpoint_path = config["checkpoint_path"]

    collected_matches = 0
    log_print(f"[수집 시작] 목표 신규 3v3 매치: {max_new}개 | 대상 패치: {sorted(target_patches) or '전체'}")

    while queue_puuids and collected_matches < max_new:
        puuid = queue_puuids.popleft()
        if puuid in visited_puuids:
            continue
        visited_puuids.add(puuid)

        match_ids = client.get_match_ids_by_puuid(puuid, count=30)
        for mid in match_ids:
            if collected_matches >= max_new:
                break
            if mid in seen_match_ids:
                continue
            seen_match_ids.add(mid)

            match_json = client.get_match(mid)
            if not match_json:
                continue
            parsed = parse_arena_3v3_match(match_json, meta, target_queues, target_patches)
            if not parsed:
                continue

            append_checkpoint(checkpoint_path, parsed)
            existing_records.extend(parsed)
            collected_matches += 1

            for rec in parsed:
                p_id = rec.get("puuid")
                if p_id and p_id not in visited_puuids and len(queue_puuids) < 2000:
                    queue_puuids.append(p_id)

            if collected_matches % 15 == 0:
                log_print(f"  -> 진행: 신규 {collected_matches}/{max_new} 매치 (누적 {len(existing_records):,}건)")

    log_print(f"[수집 완료] 신규 3v3 매치 {collected_matches}개 추가 (총 {len(existing_records):,}건)")
    return existing_records, collected_matches


# =============================================================================
# 8. 순수 실측 데이터 기반 다중 패치 통합 집계
# =============================================================================


def aggregate_all_champions(
    records: List[Dict[str, Any]],
    meta: GameMetadata,
    min_aug_picks: int = 3,
    max_augs_per_rarity: int = 10,
) -> Dict[str, Any]:
    """수집된 3v3 아레나 실측 레코드만을 사용해 챔피언별 통계를 집계합니다."""
    if not records:
        return {}

    all_patches_desc = sorted({r["patch"] for r in records}, key=patch_sort_key, reverse=True)
    weight_curve = [1.0, 0.75, 0.5, 0.35, 0.25]
    patch_weights = {
        p: weight_curve[min(idx, len(weight_curve) - 1)]
        for idx, p in enumerate(all_patches_desc)
    }
    patch_scope_str = " · ".join(all_patches_desc[:4])

    by_champ: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in records:
        by_champ[r["champion_name"]].append(r)

    total_matches_est = max(len(records) / 18.0, 1.0)
    guide_output: Dict[str, Any] = {}

    for champ_key, rows in by_champ.items():
        raw_total_picks = len(rows)
        if raw_total_picks < 3:
            continue

        # 1) 패치별 통계 & 최신 가중 통합 요약
        by_patch: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for r in rows:
            by_patch[r["patch"]].append(r)

        patch_stats: Dict[str, Dict[str, Any]] = {}
        w_picks_sum = 0.0
        w_place_sum = 0.0
        w_top3_sum = 0.0
        w_win_sum = 0.0
        w_dmg_sum = 0.0

        for p_ver in sorted(by_patch.keys(), key=patch_sort_key, reverse=True):
            p_rows = by_patch[p_ver]
            pn = len(p_rows)
            p_avg = round(sum(x["placement"] for x in p_rows) / pn, 2)
            p_top3 = round(sum(100.0 for x in p_rows if x["is_top_half"]) / pn, 1)
            p_win = round(sum(100.0 for x in p_rows if x["is_win"]) / pn, 1)
            p_dmg = int(round(sum(x["damage_dealt"] for x in p_rows) / pn))

            patch_stats[p_ver] = {
                "picks": pn,
                "avg_placement": p_avg,
                "top3_rate": p_top3,
                "win_rate": p_win,
                "avg_damage": p_dmg,
            }

            w = patch_weights.get(p_ver, 0.5)
            eff_n = pn * w
            w_picks_sum += eff_n
            w_place_sum += p_avg * eff_n
            w_top3_sum += p_top3 * eff_n
            w_win_sum += p_win * eff_n
            w_dmg_sum += p_dmg * eff_n

        overall_avg = round(w_place_sum / w_picks_sum, 2)
        overall_bayes = round(bayes_smooth(w_picks_sum, overall_avg, m=25.0, prior=3.5), 2)
        overall_top3 = round(w_top3_sum / w_picks_sum, 1)
        overall_win = round(w_win_sum / w_picks_sum, 1)
        overall_dmg = int(round(w_dmg_sum / w_picks_sum))
        pick_rate_pct = round((raw_total_picks / total_matches_est) * 100.0, 2)

        # 6팀 아레나 실측 분포 기준 티어 산정
        if raw_total_picks < 20:
            tier, tier_num = "B", 3
        elif overall_bayes <= 3.32:
            tier, tier_num = "S", 1
        elif overall_bayes <= 3.46:
            tier, tier_num = "A", 2
        elif overall_bayes <= 3.60:
            tier, tier_num = "B", 3
        else:
            tier, tier_num = "C", 4

        # 2) 증강 집계 (프리즘 / 골드 / 실버 등급별 최대 10개)
        aug_bucket: Dict[str, Dict[str, float]] = defaultdict(
            lambda: {"picks": 0, "w_picks": 0.0, "w_place": 0.0, "w_win": 0.0, "w_top3": 0.0}
        )
        for r in rows:
            w = patch_weights.get(r["patch"], 0.5)
            for aug_name in set(r.get("combat_augments") or []):
                aug_clean = normalize_augment_name(aug_name)
                if not aug_clean or aug_clean == "???":
                    continue
                b = aug_bucket[aug_clean]
                b["picks"] += 1
                b["w_picks"] += w
                b["w_place"] += r["placement"] * w
                b["w_win"] += (100.0 if r["is_win"] else 0.0) * w
                b["w_top3"] += (100.0 if r["is_top_half"] else 0.0) * w

        eff_min_aug = max(min_aug_picks, int(raw_total_picks * 0.012))
        aug_by_rarity: Dict[str, List[Dict[str, Any]]] = {"프리즘": [], "골드": [], "실버": []}

        for aug_name, stats in aug_bucket.items():
            n = int(stats["picks"])
            if n < eff_min_aug and raw_total_picks >= 40:
                continue
            if n < 2:
                continue
            wp = stats["w_picks"]
            avg_p = round(stats["w_place"] / wp, 2)
            win_r = round(stats["w_win"] / wp, 1)
            top3_r = round(stats["w_top3"] / wp, 1)

            scored = compute_composite_augment_score(
                aug_picks=n,
                champ_picks=raw_total_picks,
                raw_avg_place=avg_p,
                raw_win_rate=win_r,
                champ_avg_place=overall_avg,
                champ_win_rate=overall_win,
            )

            aug_info = meta.augments_by_name.get(aug_name, {})
            rarity_ko = normalize_rarity_ko(aug_info.get("rarity", "실버"), aug_name=aug_name)

            aug_by_rarity[rarity_ko].append({
                "name": aug_name,
                "rarity": rarity_ko,
                "picks": n,
                "pick_rate": scored["pick_rate"],
                "avg": avg_p,
                "avg_placement": avg_p,
                "bayes": scored["bayes_placement"],
                "win": win_r,
                "win_rate": win_r,
                "top3_rate": top3_r,
                "composite_score": scored["composite_score"],
                "icon": aug_info.get("icon_url", ""),
                "icon_url": aug_info.get("icon_url", ""),
            })

        best_augments: List[Dict[str, Any]] = []
        per_tier_limit = max(max_augs_per_rarity, 10)
        for rarity_key in ("프리즘", "골드", "실버"):
            candidates = sorted(
                aug_by_rarity[rarity_key],
                key=lambda x: (-x["composite_score"], -x["picks"], x["bayes"]),
            )
            best_augments.extend(candidates[:per_tier_limit])

        # 3) 프리즘 아이템 집계
        prism_bucket: Dict[str, Dict[str, float]] = defaultdict(
            lambda: {"picks": 0, "w_picks": 0.0, "w_place": 0.0, "w_win": 0.0, "w_top3": 0.0}
        )
        for r in rows:
            w = patch_weights.get(r["patch"], 0.5)
            for pname in set(r.get("prismatic_items") or []):
                b = prism_bucket[pname]
                b["picks"] += 1
                b["w_picks"] += w
                b["w_place"] += r["placement"] * w
                b["w_win"] += (100.0 if r["is_win"] else 0.0) * w
                b["w_top3"] += (100.0 if r["is_top_half"] else 0.0) * w

        prism_list: List[Dict[str, Any]] = []
        for pname, stats in prism_bucket.items():
            n = int(stats["picks"])
            if n < 2:
                continue
            wp = stats["w_picks"]
            avg_p = round(stats["w_place"] / wp, 2)
            win_r = round(stats["w_win"] / wp, 1)
            top3_r = round(stats["w_top3"] / wp, 1)
            pr = round((n / raw_total_picks) * 100.0, 1)
            bp = round(bayes_smooth(n, avg_p, m=10.0, prior=overall_avg), 2)
            latest_id = meta.item_name_to_latest_id.get(pname)
            cd_icon = meta.cdragon_item_icon_by_name.get(pname, "")
            dd_icon = (meta.items_by_id.get(latest_id) or {}).get("icon_url", "") if latest_id else ""
            item_icon = cd_icon or dd_icon
            prism_list.append({
                "name": pname,
                "id": latest_id,
                "picks": n,
                "pick_rate": pr,
                "avg": avg_p,
                "avg_placement": avg_p,
                "bayes": bp,
                "win": win_r,
                "win_rate": win_r,
                "top3_rate": top3_r,
                "icon": item_icon,
                "icon_url": item_icon,
            })
        prism_list.sort(key=lambda x: (x["bayes"], -x["picks"]))
        best_prismatics = prism_list[:8]

        # 4) 일반 코어 아이템 집계
        core_bucket: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {"id": None, "picks": 0, "w_picks": 0.0, "w_place": 0.0, "w_win": 0.0, "w_top3": 0.0}
        )
        for r in rows:
            w = patch_weights.get(r["patch"], 0.5)
            seen_items_in_game: Set[str] = set()
            for c_item in r.get("core_items") or []:
                iname = c_item.get("name") if isinstance(c_item, dict) else str(c_item)
                if not iname or iname in seen_items_in_game or iname in EXCLUDED_CORE_ITEMS_KO or iname in meta.prismatic_item_names:
                    continue
                seen_items_in_game.add(iname)
                b = core_bucket[iname]
                b["picks"] += 1
                b["w_picks"] += w
                b["w_place"] += r["placement"] * w
                b["w_win"] += (100.0 if r["is_win"] else 0.0) * w
                b["w_top3"] += (100.0 if r["is_top_half"] else 0.0) * w
                if not b["id"]:
                    b["id"] = (
                        (c_item.get("id") if isinstance(c_item, dict) else None)
                        or meta.item_name_to_latest_id.get(iname)
                    )

        core_list: List[Dict[str, Any]] = []
        for iname, stats in core_bucket.items():
            n = int(stats["picks"])
            if n < 2:
                continue
            wp = stats["w_picks"]
            avg_p = round(stats["w_place"] / wp, 2)
            win_r = round(stats["w_win"] / wp, 1)
            top3_r = round(stats["w_top3"] / wp, 1)
            pr = round((n / raw_total_picks) * 100.0, 1)
            iid = stats["id"] or meta.item_name_to_latest_id.get(iname)
            dd_icon = (
                f"https://ddragon.leagueoflegends.com/cdn/{meta.dd_version}/img/item/{iid}.png"
                if iid
                else meta.cdragon_item_icon_by_name.get(iname, "")
            )
            core_list.append({
                "name": iname,
                "id": iid,
                "picks": n,
                "pick_rate": pr,
                "avg": avg_p,
                "avg_placement": avg_p,
                "win": win_r,
                "win_rate": win_r,
                "top3_rate": top3_r,
                "icon": dd_icon,
                "icon_url": dd_icon,
            })
        core_list.sort(key=lambda x: (-x["picks"], x["avg"]))
        core_items = core_list[:10]

        # 5) 3인 팀 파트너 시너지 집계
        partner_bucket: Dict[str, Dict[str, float]] = defaultdict(
            lambda: {"games": 0, "w_games": 0.0, "w_place": 0.0, "w_top3": 0.0, "w_win": 0.0}
        )
        for r in rows:
            w = patch_weights.get(r["patch"], 0.5)
            for partner_raw in r.get("partner_champions") or []:
                partner_key = meta.resolve_champion_key(partner_raw)
                if not partner_key or partner_key == champ_key:
                    continue
                b = partner_bucket[partner_key]
                b["games"] += 1
                b["w_games"] += w
                b["w_place"] += r["placement"] * w
                b["w_top3"] += (100.0 if r["is_top_half"] else 0.0) * w
                b["w_win"] += (100.0 if r["is_win"] else 0.0) * w

        partner_list: List[Dict[str, Any]] = []
        for pkey, stats in partner_bucket.items():
            g = int(stats["games"])
            if g < 2:
                continue
            wg = stats["w_games"]
            avg_p = round(stats["w_place"] / wg, 2)
            top3_r = round(stats["w_top3"] / wg, 1)
            win_r = round(stats["w_win"] / wg, 1)
            bp = round(bayes_smooth(g, avg_p, m=8.0, prior=overall_avg), 2)
            p_kr = (meta.champions_by_id.get(pkey) or {}).get("kr_name", pkey)
            partner_list.append({
                "partner": pkey,
                "champion": pkey,
                "name": p_kr,
                "games": g,
                "avg": avg_p,
                "avg_placement": avg_p,
                "bayes_placement": bp,
                "win_rate": win_r,
                "top3_rate": top3_r,
            })
        partner_list.sort(key=lambda x: (x["bayes_placement"], -x["games"]))
        best_partners = partner_list[:10]

        champ_meta = meta.champions_by_id.get(champ_key) or {}
        kr_name = champ_meta.get("kr_name", champ_key)
        roles = champ_meta.get("roles") or ["Fighter"]

        guide_output[champ_key] = {
            "champion_name": champ_key,
            "kr_name": kr_name,
            "roles": roles,
            "patch_scope": patch_scope_str,
            "patch_stats": patch_stats,
            "summary": {
                "tier": tier,
                "tier_num": tier_num,
                "picks": raw_total_picks,
                "pick_rate": pick_rate_pct,
                "avg_placement": overall_avg,
                "bayes_placement": overall_bayes,
                "top3_rate": overall_top3,
                "win_rate": overall_win,
                "avg_damage": overall_dmg,
            },
            "best_augments": best_augments,
            "best_prismatics": best_prismatics,
            "core_items": core_items,
            "best_partners": best_partners,
        }

    return guide_output


# =============================================================================
# 9. 메인 엔트리포인트
# =============================================================================


def main() -> None:
    log_print("=" * 64)
    log_print("LoL 아레나 3v3 실측 데이터 수집 및 통합 집계기 시작")
    log_print("=" * 64)

    meta = GameMetadata()
    checkpoint_path = COLAB_CONFIG["checkpoint_path"]
    output_path = COLAB_CONFIG["output_json_path"]

    records, seen_match_ids, seen_puuids = load_checkpoint(checkpoint_path, meta)

    new_collected = 0
    if not COLAB_CONFIG.get("aggregate_only", True):
        try:
            records, new_collected = crawl_new_matches(COLAB_CONFIG, meta, records, seen_match_ids, seen_puuids)
        except PermissionError as exc:
            log_print(
                f"[인증 오류] {exc}\n"
                "  -> Development Key는 24시간마다 만료됩니다. GitHub Secrets의 RIOT_API_KEY를 갱신하거나\n"
                "     Riot Developer Portal에서 만료 없는 Personal API Key를 발급받아 등록해 주세요."
            )
            if os.environ.get("GITHUB_ACTIONS") == "true":
                raise SystemExit(1)
    else:
        log_print("[재집계 모드] aggregate_only=True -> 체크포인트 즉시 재집계 진행")

    # 신규 매치가 추가되었으면 단일 gzip 스트림으로 재압축해 용량을 최소화
    if new_collected > 0 and checkpoint_path.endswith(".gz"):
        save_checkpoint_compact(checkpoint_path, records)

    if not records:
        log_print(
            f"[안내] '{checkpoint_path}' 파일이 없거나 비어 있습니다.\n"
            "  1) 기존 arena_3v3_checkpoint.jsonl(.gz)을 업로드 후 실행하거나,\n"
            "  2) aggregate_only=False 및 Riot API 키 설정 후 신규 수집을 실행하세요."
        )
        return

    guide_data = aggregate_all_champions(
        records=records,
        meta=meta,
        min_aug_picks=int(COLAB_CONFIG.get("min_aug_picks", 3)),
        max_augs_per_rarity=int(COLAB_CONFIG.get("max_augs_per_rarity", 10)),
    )

    with open(output_path, "w", encoding="utf-8") as fp:
        json.dump(guide_data, fp, ensure_ascii=False, indent=2)

    rarity_summary = Counter(
        aug["rarity"]
        for c in guide_data.values()
        for aug in c.get("best_augments", [])
    )
    log_print(
        f"[완료] '{output_path}' 저장 성공! "
        f"총 {len(guide_data)}명 챔피언 집계 (증강 등급 분포: {dict(rarity_summary)})"
    )

    if os.environ.get("GITHUB_ACTIONS") != "true":
        try:
            from google.colab import files  # type: ignore
            log_print(f"[Colab] '{output_path}' 자동 다운로드 시작...")
            files.download(output_path)
        except ImportError:
            pass
        except Exception as exc:
            log_print(f"[안내] 자동 다운로드 건너뜀(좌측 파일 탭에서 수동 다운로드 가능): {exc}")


if __name__ == "__main__":
    main()
