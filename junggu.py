# -*- coding: utf-8 -*-
"""
중구청 외국인 지원 정책 크롤러 (자동 키 분류 + 본문만 파싱 + 통일 칼럼 저장)
- 목록 URL에서 게시물 자동 수집
- 본문은 '▶ 제목' + 'ㅇ 키:값' + 자유 형식 자동 파싱
- 키워드 포함 기반 자동 분류 (대상, 기간, 방법, 문의, 장소, 금액 등)
- 실행 때마다 새 파일로 저장 (CSV / JSON)
"""

import re
import json
import time
import pathlib
import pandas as pd
import requests
from bs4 import BeautifulSoup

# ===== 지역/상수 =====
REGION = "중구"
LIST_URL = ["https://www.junggu.seoul.kr/content.do?cmsid=16539",
            "https://www.junggu.seoul.kr/content.do?cmsid=16540",]
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; DataCollectionBot/1.0)"}

COLS = [
    "region","source_category","item_title","target","period","content",
    "method","contact","location","full_text","source_url"
]

# ===== 공통 정규식 =====
ZERO_WIDTH = re.compile(r"[\u200b\u200c\u200d\u2060\ufeff]")
PHONE_SEQ  = re.compile(r"(?:\d{2,4}\s*-\s*\d{3,4}\s*-\s*\d{4})(?:\s*/\s*(?:\d{2,4}\s*-\s*\d{3,4}\s*-\s*\d{4}))*")
KEY_VALUE_PAT = re.compile(r"\s*(.+?)\s*[:：\-–]\s*(.+)$")
ANGLE_TITLE_PAT = re.compile(r"^[\s]*[<〈《](.+?)[>〉》]\s*$")

# ========== 자동 키 분류 함수 ==========
def normalize_key(k: str) -> str:
    """한글 키워드 비교용: 특수문자, 공백, 괄호 제거"""
    k = ZERO_WIDTH.sub("", k)
    k = k.replace("\u00A0", " ").replace("\u3000", " ")
    k = re.sub(r"[★☆•·▶▷■○●※\-\=\+\(\)\[\]\{\}<>]", "", k)
    k = re.sub(r"\s+", "", k)
    # 숫자나 콜론(:) 뒤에 붙은 문자는 버림
    k = re.sub(r"[:0-9].*$", "", k)
    return k.strip()



def auto_map_key(pre_norm: str) -> str:
    """키 문자열 안에 포함된 단어를 기준으로 자동 매핑."""
    if any(word in pre_norm for word in ["대상", "자격"]):
        return "target"
    if any(word in pre_norm for word in ["기간", "일시", "시간", "기한","일정"]):
        return "period"
    if any(word in pre_norm for word in ["방법","금액"]):
        return "method"
    if any(word in pre_norm for word in ["문의", "연락처", "전화번호", "담당","접수"]):
        return "contact"
    if any(word in pre_norm for word in ["장소", "위치", "지역", "운영장소","현황"]):
        return "location"
    if any(word in pre_norm for word in ["내용", "소개", "개요", "설명", "보장내용", "지원내용", "사업내용"]):
        return "content"  # ✅ 새로 추가
    return None  # 인식 안 되면 content로


# ========== 본문 정리 함수 ==========
def fetch_html(url: str) -> str:
    for i in range(3):
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.ok and r.text.strip():
            return r.text
        time.sleep(1 + i)
    r.raise_for_status()

def node_text_with_newlines(node: BeautifulSoup) -> str:
    """HTML 본문 → 줄바꿈 포함된 텍스트 정리 (운영기간/운영대상 줄 구분 강화)"""
    
    # <br>을 \n으로 변환
    for br in node.find_all("br"):
        br.replace_with("\n")

    # 링크 처리 (href 남기기)
    for a in node.find_all("a"):
        href = a.get("href", "").strip()
        text = a.get_text(" ", strip=True)
        if href:
            a.replace_with(f"{text} ({href})")
        else:
            a.replace_with(text)

    # ✅ 제목(<strong>이나 <b>)에 ▶가 포함된 경우 줄바꿈 추가
    for strong_tag in node.find_all(["strong", "b"]):
        text = strong_tag.get_text(" ", strip=True)
        # ▶, ▷, ► 등으로 시작하는 경우 새 줄 앞뒤로 추가
        if re.match(r"^[▶▷►]", text):
            strong_tag.replace_with(f"\n{text}\n")
        else:
            strong_tag.replace_with(text)

    # 인라인 태그 평탄화
    for tag in node.find_all(["u", "em", "span"]):
        tag.replace_with(tag.get_text(" ", strip=True))

    # 블록 태그 개행 추가
    for tag in node.find_all(["p", "li", "div"]):
        tag.replace_with(tag.get_text(" ", strip=True) + "\n")

    # 전체 텍스트
    text = node.get_text("\n")
    text = ZERO_WIDTH.sub("", text)
    text = re.sub(r"&nbsp;?", " ", text)
    text = re.sub(r"[ \t]+", " ", text)

    # ✅ URL 앞뒤로 개행
    text = re.sub(r"(https?://[^\s]+)", r"\n\1\n", text)

    # ✅ 중구청 스타일 (○, ㅇ, - 등)
    text = re.sub(r"(?=[ㅇ○●\-※★]\s*[가-힣])", "\n", text)

    # ✅ 핵심 추가 — ▶, ▷, ► 등 앞뒤로 강제 개행 추가
    text = re.sub(r"([▶▷►])", r"\n\1", text)

    # ✅ 중복 개행 정리
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()




# ========== 목록 수집 ==========
def collect_detail_urls_from_list(list_url: str, max_pages: int = 30, sleep_sec: float = 0.6):
    urls, seen = [], set()
    for page in range(1, max_pages + 1):
        list_page = f"{list_url}&page={page}"
        print(f"📄 페이지 {page} 요청 중: {list_page}")
        try:
            html = fetch_html(list_page)
        except Exception as e:
            print(f"❌ 요청 실패: {e}")
            break

        soup = BeautifulSoup(html, "lxml")
        before = len(urls)
        for a in soup.select("a[href*='mode=view'][href*='cid=']"):
            href = a.get("href", "").strip()
            if not href:
                continue
            if not href.startswith("http"):
                href = requests.compat.urljoin(list_url, href)
            if href not in seen:
                seen.add(href)
                urls.append(href)
        if len(urls) == before:
            break
        time.sleep(sleep_sec)
    print(f"✅ 총 {len(urls)}건 수집 완료")
    return urls


# ========== 본문 파싱 ==========
def parse_detail_lines_to_fields(detail_lines):
    fields = {}
    current_section = None
    locked_section = None  # 현재 필드 고정 여부

    for raw in detail_lines:
        text = ZERO_WIDTH.sub("", raw).strip()
        if not text:
            continue
        text = re.sub(r"&nbsp;?", " ", text)

        # 🔒 잠금 상태인 경우
        if locked_section:
            # 새 키워드 탐지 (ex. 접수, 문의, 장소 등)
            check_key = re.sub(r"^[\s○●※\-–•·∙⋅☆▶▷]*", "", text)
            key_norm = normalize_key(check_key.split(":", 1)[0].split("-", 1)[0])
            new_mapped = auto_map_key(key_norm)

            # ✅ method 외 다른 명시적 키 등장 시에만 해제
            if new_mapped and new_mapped not in (None, locked_section):
                locked_section = None
                current_section = new_mapped
                # 그 줄은 새 필드로 저장
                val = ""
                if re.search(r"[:：\-–]", text):
                    parts = re.split(r"[:：\-–]", text, maxsplit=1)
                    val = parts[1].strip() if len(parts) > 1 else ""
                fields[new_mapped] = f"{fields.get(new_mapped, '')} / {val}".strip(" /")
                continue

            # 그 외에는 그대로 현재 섹션(method)에 이어붙이기
            fields[locked_section] = f"{fields.get(locked_section, '')} / {text}".strip(" /")
            continue

                # 🟢 일반 처리 시작
        # 기존 stripped = re.sub(...) → 수정
        if text.lstrip().startswith("※"):
            # ※ 문장은 주석이나 참고 문장으로 간주 → 내용 유지
            stripped = text.strip()
        else:
            stripped = re.sub(
                r"^[\s　]*(?:[▶▷○●□■◆◇☆\-–•·∙⋅●◦❍])\s*(?=\(?[가-힣A-Za-z0-9])",
                "",
                text
            ).strip()

        

        if re.search(r"[:：]\s*", stripped):
            key, val = re.split(r"[:：]", stripped, maxsplit=1)
            key_norm = normalize_key(key)
            mapped = auto_map_key(key_norm)
            if mapped:
                # 기존
                # fields[mapped] = f"{fields.get(mapped, '')} / {val.strip()}".strip(" /")
                # 변경 ✅
                fields[mapped] = f"{fields.get(mapped, '')} / {key.strip()} : {val.strip()}".strip(" /")
                current_section = mapped
                if mapped == "method":
                    locked_section = "method"
                continue


        # (2) "키 - 값"
        m_dash = re.match(r"^([가-힣\s]+?)\s*[-–]\s*(.+)$", stripped)
        if m_dash:
            key, val = m_dash.groups()
            key_norm = normalize_key(key)
            mapped = auto_map_key(key_norm)
            if mapped:
                # 기존
                # fields[mapped] = f"{fields.get(mapped, '')} / {val.strip()}".strip(" /")
                # 변경 ✅
                fields[mapped] = f"{fields.get(mapped, '')} / {key.strip()} - {val.strip()}".strip(" /")
                current_section = mapped
                if mapped == "method":
                    locked_section = "method"
                continue


        # (3) 단독 키워드 줄 (‘이용방법’, ‘응모방법’ 등)
        key_norm = normalize_key(stripped)
        mapped = auto_map_key(key_norm)
        if mapped:
            current_section = mapped
            if mapped == "method":
                locked_section = "method"
            continue

        # (4) 일반 문장 — 현재 섹션 유지
        target = current_section or "content"
        fields[target] = f"{fields.get(target, '')} / {text}".strip(" /")

    return fields









# ========== 아이템 단위 분리 ==========
def split_items(text: str, fallback_title: str = None):
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    items, current_title, current_details = [], None, []
    drop_tokens = ("첨부", "바로보기", "목록", "이전글", "다음글")

    def flush_if_has_details():
        nonlocal items, current_title, current_details
        if current_title and current_details:
            items.append({
                "item_title": current_title.strip(),
                "detail_lines": current_details
            })
        current_title, current_details = None, []

    for ln in lines:
        if ln.startswith(drop_tokens):
            continue

        # ✅ "▶" 또는 "〈제목〉" 패턴 등장 시 새 item으로 분리
        if ln.startswith("▶") or re.match(r"^[<〈《].+[>〉》]$", ln) or re.match(r"^\d+\.\s*[A-Za-z가-힣]", ln):
            flush_if_has_details()
            current_title = re.sub(r"^[▶\s]*", "", ln).strip()
            continue

        # ✅ 본문 첫 줄이 fallback 제목과 같으면 생략
        if fallback_title:
            clean_ln = re.sub(r"[<〈《>〉》]", "", ln).strip()
            clean_title = re.sub(r"[<〈《>〉》]", "", fallback_title).strip()
            if clean_ln == clean_title:
                continue

        # ✅ 일반 본문 줄
        # 글머리표(ㅇ, ○, - 등)는 그대로 유지해서 parse_detail_lines_to_fields()로 넘김
        if current_title:
            current_details.append(ln)

    # 마지막 항목 추가
    flush_if_has_details()

    # ✅ 만약 ▶ 구분이 없으면 fallback으로 전체를 하나로 묶음
    if not items and lines:
        items = [{
            "item_title": fallback_title or "(본문)",
            "detail_lines": lines
        }]

    return items




# ========== 1건 파싱 ==========
def build_records(url: str):
    html = fetch_html(url)
    soup = BeautifulSoup(html, "lxml")

    # 제목 탐색
    title_el = soup.select_one("th.view_tit, div.board_view_02 h3, div.board_view h3")
    item_title = title_el.get_text(strip=True) if title_el else ""

    # 본문 노드
    node = soup.select_one("div.board_view_02 td.view_txt, td.view_txt, div.view_txt, .board_view, article")
    if not node:
        node = soup

    text = node_text_with_newlines(node)
    items = split_items(text, fallback_title=item_title)
    records = []

    for it in items:
        fields = parse_detail_lines_to_fields(it["detail_lines"])
        rec = {
            "region": REGION,
            "source_category": "",
            "item_title": it["item_title"],
            "target": fields.get("target", ""),
            "period": fields.get("period", ""),
            "content": fields.get("content", ""),
            "method": fields.get("method", ""),
            "contact": fields.get("contact", ""),
            "location": fields.get("location", ""),
            "full_text": "▶ " + it["item_title"] + "\n" + "\n".join(it["detail_lines"]),
            "source_url": url
        }
        records.append(rec)
    return records


# ========== 저장 ==========
def save_records(records, outdir="out", base_name="중구"):
    out = pathlib.Path(outdir)
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(records)
    for c in COLS:
        if c not in df.columns:
            df[c] = ""
    df = df[COLS]
    df.to_csv(out / f"{base_name}.csv", index=False, encoding="utf-8-sig")
    df.to_json(out / f"{base_name}.json", orient="records", force_ascii=False, indent=2)
    print(f"✅ 저장 완료 ({len(df)}건)\n{out / f'{base_name}.csv'}")

# ========== 실행 ==========
if __name__ == "__main__":
    all_records = []

    for list_url in LIST_URL:  # ✅ 여러 목록 URL 순회
        print(f"🌐 목록 수집 시작: {list_url}")
        urls = collect_detail_urls_from_list(list_url, max_pages=30)
        for u in urls:
            try:
                all_records.extend(build_records(u))
            except Exception as e:
                print(f"⚠️ {u} 오류: {e}")

    save_records(all_records)
