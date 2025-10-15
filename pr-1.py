# -*- coding: utf-8 -*-
"""
중구청 외국인 지원 정책 크롤러 (칼럼 통일 + 매번 새로 저장)
- '▶ 제목' + 'ㅇ 키:값' 형식 파싱
- 〈제목〉 / <제목> 도 인식
- 불필요한 네비/설문/만족도 꼬리 문구 제거
- "지원자격", "가 격" 같은 단독 헤더도 보존 + 값 누락 방지
- full_text = 원본 clean_lines 그대로 (파싱에 영향 안 받음)
- 실행할 때마다 이전 파일 무시 → 새로 덮어쓰기
"""

import re
import json
import time
import pathlib
import pandas as pd
import requests
from bs4 import BeautifulSoup

# ===== 지역 상수 =====
REGION = "중구"  # <- 이 스크립트는 중구 전용. 다른 구로 확장 시 여기만 바꿔주면 됨.

# ===== URL 목록 =====
URLS = [
    "https://www.junggu.seoul.kr/content.do?cmsid=16539&mode=view&cid=1371153241",
    "https://www.junggu.seoul.kr/content.do?cmsid=16539&mode=view&cid=1371321624",
    "https://www.junggu.seoul.kr/content.do?cmsid=16539&mode=view&cid=1371339757",
    "https://www.junggu.seoul.kr/content.do?cmsid=16539&mode=view&cid=1371342445",
    "https://www.junggu.seoul.kr/content.do?cmsid=16539&mode=view&cid=1371356544",
    "https://www.junggu.seoul.kr/content.do?cmsid=16539&mode=view&cid=1371367132",
    "https://www.junggu.seoul.kr/content.do?cmsid=16539&mode=view&cid=1371382243",
    "https://www.junggu.seoul.kr/content.do?cmsid=16539&mode=view&cid=1371412107",
    "https://www.junggu.seoul.kr/content.do?cmsid=16539&mode=view&cid=1371424516",
    "https://www.junggu.seoul.kr/content.do?cmsid=16539&mode=view&cid=1371436405",
    "https://www.junggu.seoul.kr/content.do?cmsid=16539&mode=view&cid=1375812929",
]

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; DataCollectionBot/1.0)"}

# 고정 칼럼 
COLS = [
    "region", 
    "source_category",
    "item_title",
    "target",
    "application_period",
    "content",
    "application_method",
    "contact",
    "notes",
    "purchase_method",
    "location",
    "full_text",
    "source_url"
]

# 정규식
ANGLE_TITLE_PAT = re.compile(r"^[\s]*[<〈《](.+?)[>〉》]\s*$")
KEY_VALUE_PAT   = re.compile(r"\s*(.+?)\s*[:：\-–]\s*(.+)$")
ZERO_WIDTH      = re.compile(r"[\u200b\u200c\u200d\u2060\ufeff]")
PHONE_SEQ       = re.compile(r"(?:\d{2,4}\s*-\s*\d{3,4}\s*-\s*\d{4})(?:\s*/\s*(?:\d{2,4}\s*-\s*\d{3,4}\s*-\s*\d{4}))*")

KEY_ALIAS = {
    "대상":"target", "지원대상":"target", "지원자격":"target",
    "내용":"content", "주요내용":"content"," 주요내용":"content", "지원내용":"content", "보장내용":"content",
    "제출서류":"content", "지원한도":"content", "지원금액":"content", "지원 금액":"content", "한도":"content", "지원형태":"content",
    "사용처":"notes", "기타":"notes", "비고":"notes",
    "이용방법":"application_method", "신청방법":"application_method", "신청":"application_method",
    "청구방법":"application_method", "인증방법":"application_method",
    "문의":"contact", "문 의":"contact", "연락처":"contact",
    "기간":"application_period", "신청기간":"application_period", "신청기한":"application_period",
    "청구기간":"application_period", "보장기간":"application_period", "추진일정":"application_period",
    "일시":"application_period", "일 시":"application_period",
    "구입방법":"purchase_method", "구 입 방법":"purchase_method",
    "장소":"location", "장 소":"location"
}

# ---------------- 잡음 제거 ----------------
NOISE_PATTERNS = [
    r"\[.+?\].*",
    r"해당 메뉴에 대한 만족도",
    r"아주\s*만족", r"보통", r"아주\s*불만"
]
NOISE_LINE_PAT = re.compile("|".join(NOISE_PATTERNS))

def trim_noise_lines(lines):
    cleaned = []
    for ln in lines:
        text = ln.strip()
        if re.match(r"^ㅇ\s*문\s*의", text) or re.match(r"^ㅇ\s*연락처", text):
            text = re.split(r"\[.*?\]|해당 메뉴에 대한 만족도|아주\s*만족|보통|아주\s*불만", text)[0].strip()
            if text:
                cleaned.append(text)
            continue
        if NOISE_LINE_PAT.search(text):
            break
        cleaned.append(ln)
    return cleaned

# ---------------- Fetch ----------------
def fetch_html(url: str) -> str:
    for i in range(3):
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.ok and r.text.strip():
            return r.text
        time.sleep(1 + i)
    r.raise_for_status()

# ---------------- Extract ----------------
def get_main_node_and_title(html: str):
    soup = BeautifulSoup(html, "lxml")
    page_title = (soup.title.get_text(strip=True) if soup.title else "").strip()

    candidates = [".board_view", ".board-view", ".bbs_view", ".bbs-view", "#content", "article", "main"]
    node = None
    for sel in candidates:
        node = soup.select_one(sel)
        if node and node.get_text(strip=True):
            break
    if node is None:
        node = soup

    page_heading = ""
    h3 = soup.select_one("h3")
    if h3:
        page_heading = h3.get_text(strip=True)
    if not page_heading:
        page_heading = page_title

    return soup, node, page_title, page_heading

def node_text_with_newlines(node: BeautifulSoup) -> str:
    for br in node.find_all(["br"]):
        br.replace_with("\n")
    for p in node.find_all(["p", "li", "div"]):
        if p.text and not p.text.endswith("\n"):
            p.append("\n")
    text = node.get_text("\n")
    text = re.sub(r"\r", "", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text

# ---------------- Split items ----------------
def split_items(text: str, fallback_title: str = None):
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    items, current_title, current_details = [], None, []
    drop_tokens = ("첨부", "바로보기", "목록", "이전글", "다음글")

    def flush_if_has_details():
        nonlocal items, current_title, current_details
        if current_title and current_details:
            items.append({"item_title": current_title, "detail_lines": current_details})
        current_title, current_details = None, []

    for ln in lines:
        if ln.startswith(drop_tokens):
            continue

        if ln.startswith("▶"):
            flush_if_has_details()
            current_title = ln.lstrip("▶").strip()
            continue

        m = ANGLE_TITLE_PAT.match(ln)
        if m:
            flush_if_has_details()
            current_title = m.group(1).strip()
            continue

        if ln.startswith("ㅇ"):
            if current_title is None:
                continue
            current_details.append(ln)
        else:
            if current_details:
                current_details[-1] += " " + ln
            elif current_title:
                current_details.append(ln)

    if current_title and current_details:
        items.append({"item_title": current_title, "detail_lines": current_details})

    if not items and lines:
        title = fallback_title or "(본문)"
        items = [{"item_title": title, "detail_lines": lines}]

    return items

# ---------------- Parse fields ----------------
def normalize_key(k: str) -> str:
    k = ZERO_WIDTH.sub("", k)
    k = k.replace("\u00A0"," ").replace("\u3000"," ")
    k = re.sub(r"[★☆•·\[\](){}]", "", k)
    k = re.sub(r"[-–—]", "", k)
    k = re.sub(r"\s+", "", k)
    return k

def append_field(fields, key, val):
    if not val: return
    if key in fields and fields[key]:
        fields[key] = f"{fields[key]} / {val}"
    else:
        fields[key] = val

def parse_detail_lines_to_fields(detail_lines):
    fields = {}
    current_section = None
    BULLET_PREFIXES = ("☞", "•", "·", "-", "▶", "▷", "■")
    NUM_BULLET = re.compile(r"^\d+\.\s*|^[①②③④⑤⑥⑦⑧⑨⑩]")

    def strip_leading_oi(s: str) -> str:
        # 맨 앞 'ㅇ' + 공백 제거
        return re.sub(r"^ㅇ\s*", "", s)

    for raw in detail_lines:
        raw_norm = ZERO_WIDTH.sub("", raw).strip()
        if not raw_norm:
            continue

        # 1) 'ㅇ 키:값' 형태 (콜론/대시 기반) → 가장 먼저 처리
        m = KEY_VALUE_PAT.match(strip_leading_oi(raw_norm))
        if m:
            pre, post = m.group(1).strip(), m.group(2).strip()
            pre_norm = normalize_key(pre)
            k_std = KEY_ALIAS.get(pre_norm)
            if k_std:
                v = post
                if k_std == "contact":
                    v = re.split(r"\[|첨부|바로보기|목록|이전글|다음글|만족|불만|확인", v)[0].strip()
                    v = re.split(r"\.(pdf|hwp|hwpx|docx?|xlsx?|pptx?|zip|jpe?g|png)", v, flags=re.I)[0].strip()
                    mphone = PHONE_SEQ.search(v)
                    if mphone:
                        v = v[:mphone.end()].strip()
                append_field(fields, k_std, v)
                current_section = k_std
                continue

        # 2) 콜론 없이 헤더가 나오고, 같은 줄에 내용이 이어지는 경우까지 처리
        #    예: "ㅇ 주요내용 ☞ 대사증후군관리사업 …"
        head_candidate = strip_leading_oi(raw_norm)

        # 단어들을 쪼갠 뒤, 뒤에서 앞으로 "최장 매칭"으로 헤더를 찾는다
        # (normalize_key가 공백 제거해 주므로 '주 요 내 용'도 '주요내용'으로 매칭됨)
        words = head_candidate.split()
        matched = False
        if words:
            for i in range(len(words), 0, -1):
                pre_part = " ".join(words[:i])
                pre_norm = normalize_key(pre_part)
                if pre_norm in KEY_ALIAS:
                    k_std = KEY_ALIAS[pre_norm]
                    current_section = k_std
                    # 같은 줄의 나머지 텍스트(인라인 내용)가 있으면 바로 누적
                    rest = head_candidate[len(pre_part):].strip()
                    if rest:
                        append_field(fields, current_section, rest)
                    matched = True
                    break

        if matched:
            continue  # 섹션 전환/누적 완료

        # 3) 글머리나 번호 목록 → 현재 섹션이 있으면 거기에, 없으면 content로
        if raw_norm.startswith(BULLET_PREFIXES) or NUM_BULLET.match(raw_norm):
            if current_section:
                append_field(fields, current_section, raw_norm)
            else:
                append_field(fields, "content", raw_norm)
            continue

        # 4) 일반 줄 → 현재 섹션이 있으면 거기에, 없으면 content로
        if current_section:
            append_field(fields, current_section, raw_norm)
        else:
            append_field(fields, "content", raw_norm)

    return fields

# ---------------- Block text ----------------
def item_block_text(item):
    title = (item.get("item_title") or "").strip()
    lines = [ln.strip() for ln in item.get("detail_lines", []) if ln.strip()]
    body = "\n".join(lines)
    if title and body:
        return f"▶ {title}\n{body}"
    elif title:
        return f"▶ {title}"
    return body

def get_category_from_breadcrumb(soup):
    bc_node = soup.select_one(".location, .path")
    if not bc_node:
        return None
    bc_text = bc_node.get_text(" ", strip=True)
    parts = [p.strip() for p in bc_text.split("|") if p.strip()]
    if not parts:
        return None
    
    first = parts[0]
    # 앞에 "외국인 지원 정책" 같은 긴 문자열 → "정책"으로 단순화
    if "정책" in first:
        return "정책"
    if "교육·문화" in first:
        return "교육문화"
    return first   # 못 걸러내면 원문 그대로



# ---------------- One URL → records ----------------
def build_records(url: str):
    html = fetch_html(url)
    soup, node, page_title, page_heading = get_main_node_and_title(html)
    category = get_category_from_breadcrumb(soup) or "기타" 
    text = node_text_with_newlines(node)
    items = split_items(text, fallback_title=page_heading)

    records = []
    for it in items:
        clean_lines = trim_noise_lines(it["detail_lines"])
        it_clean = {"item_title": it["item_title"], "detail_lines": clean_lines}

        fields = parse_detail_lines_to_fields(clean_lines)
        rec = {
            "region": REGION, 
            "source_category": category,
            "item_title": it["item_title"],
            "target": fields.get("target"),
            "application_period": fields.get("application_period"),
            "content": fields.get("content"),
            "application_method": fields.get("application_method"),
            "contact": fields.get("contact"),
            "notes": fields.get("notes"),
            "purchase_method": fields.get("purchase_method"),
            "location": fields.get("location"),
            "full_text": item_block_text(it_clean),
            "source_url": url
        }
        records.append(rec)
    return records

# ---------------- Save ----------------
def _canon_text(x: str) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x)
    s = re.sub(r"[\u200b\u200c\u200d\u2060\ufeff]", "", s)
    s = s.replace("\u00A0", " ").replace("\u3000", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\s*\n\s*", "\n", s)
    s = re.sub(r"\n{2,}", "\n", s)
    return s.strip()

def save_records(records, outdir="out", base_name="중구_복지정책데이터추출"):
    out = pathlib.Path(outdir)
    out.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(records)

    for c in COLS:
        if c not in df.columns:
            df[c] = None
    df = df[COLS]

    # 내용이 완전히 동일한 행은 1개만 남김 (region 포함해 비교)
    compare_cols = [c for c in COLS if c not in ("source_url", "full_text")]
    for c in compare_cols:
        df[c] = df[c].apply(_canon_text)

    df = df.drop_duplicates(subset=compare_cols, keep="first").reset_index(drop=True)

    csv_path  = out / f"{base_name}.csv"
    json_path = out / f"{base_name}.json"
    xlsx_path = out / f"{base_name}.xlsx"

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(df.to_dict(orient="records"), f, ensure_ascii=False, indent=2)
    df.to_excel(xlsx_path, index=False, engine="openpyxl")

    print(
        "저장 완료 ✅\n"
        f"- {csv_path}\n- {json_path}\n- {xlsx_path}\n"
        f"총 {len(df)}개(중복 제거 후)"
    )

# ---------------- Run ----------------
if __name__ == "__main__":
    all_records = []
    for url in URLS:
        try:
            all_records.extend(build_records(url))
        except Exception as e:
            print(f"에러: {url} -> {e}")

    if not all_records:
        print("⚠️ 항목을 찾지 못했습니다.")
    else:
        save_records(all_records)
