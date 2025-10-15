# -*- coding: utf-8 -*-
"""
서울 각 구 가족센터 (liveinkorea.kr) 프로그램 크롤러
- 자동 키 분류 + 본문 파싱 + 이미지 URL 수집
- CSV / JSON 자동 저장
"""

import re
import json
import time
import pathlib
import pandas as pd
import requests
from bs4 import BeautifulSoup

# ===== 기본 상수 =====
BASE_URL = "https://liveinkorea.kr/web/lay1/bbs/S1T10C27/A/4/list.do"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; DataCollectionBot/1.0)"}

# ✅ 지역 주소코드 (D어쩌고)
SEOUL_DISTRICTS = {  "종로구": "D001", "중구": "D002", "용산구": "D003", "성동구": "D004", "광진구": "D005",
    "동대문구": "D006", "중랑구": "D007", "성북구": "D008", "강북구": "D009", "도봉구": "D010",
    "노원구": "D011", "은평구": "D012", "서대문구": "D013", "마포구": "D014", "양천구": "D015",
    "강서구": "D016", "구로구": "D017", "금천구": "D018", "영등포구": "D019", "동작구": "D020",
    "관악구": "D021", "서초구": "D022", "강남구": "D024", "송파구": "D025","강동구": "D026"
}

#고정 칼럼
COLS = [
    "region","source_category","item_title","target","period","content",
    "method","contact","location","image","full_text","source_url"
]

# ===== 공통 정규식 =====
ZERO_WIDTH = re.compile(r"[\u200b\u200c\u200d\u2060\ufeff]")

# ========== 자동 키 분류 함수 ==========
def normalize_key(k: str) -> str:
    k = ZERO_WIDTH.sub("", k)
    k = k.replace("\u00A0", " ").replace("\u3000", " ")
    k = re.sub(r"[★☆•·▶▷■○●※\-\=\+\(\)\[\]\{\}<>]", "", k)
    k = re.sub(r"\s+", "", k)
    k = re.sub(r"[:0-9].*$", "", k)
    return k.strip()

#===== 키워드로 칼럼 분류 ======
def auto_map_key(pre_norm: str) -> str:
    if any(word in pre_norm for word in ["대상", "자격"]):
        return "target"
    if any(word in pre_norm for word in ["기간", "일시", "시간", "기한","일정"]):
        return "period"
    if any(word in pre_norm for word in ["방법","금액","신청","접수"]):
        return "method"
    if any(word in pre_norm for word in ["문의", "연락처", "전화번호", "담당"]):
        return "contact"
    if any(word in pre_norm for word in ["장소", "위치", "지역", "운영장소"]):
        return "location"
    if any(word in pre_norm for word in ["내용","소개","개요","설명","지원내용"]):
        return "content"
    return None

# ========== HTML 요청 ==========
def fetch_html(url: str) -> str:
    for i in range(3):
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.ok and r.text.strip():
            return r.text
        time.sleep(1 + i)
    r.raise_for_status()



# ========== 본문 텍스트 정리 ==========
def node_text_with_newlines(node: BeautifulSoup) -> tuple[str, str]:
    image_urls = []
    for img in node.find_all("img"):
        src = img.get("src", "")
        if not src:
            continue
        src = src.strip()

        # ✅ 절대경로가 아닌 경우에만 붙이기
        if not re.match(r"^https?://", src):
            src = requests.compat.urljoin("https://liveinkorea.kr/", src)

        image_urls.append(src)
        img.decompose()  # 이미지 태그 제거

    for p in node.find_all("p"):
        if "<ul" in p.text or "<li" in p.text or "<span" in p.text:
            try:
                inner_soup = BeautifulSoup(p.text, "lxml")
                p.clear()
                p.append(inner_soup)
            except Exception:
                pass

    for tag in node.find_all(True):
        for attr in list(tag.attrs):
            if attr.startswith("data-") or attr in ("style", "class", "id", "align"):
                del tag[attr]

    for tag in node.find_all(["span", "font", "b", "i", "u", "strong", "em"]):
        tag.unwrap()

    for li in node.find_all("li"):
        li.replace_with(li.get_text(strip=True) + "\n")
    for ul in node.find_all("ul"):
        ul.replace_with(ul.get_text("\n", strip=True) + "\n")
    for br in node.find_all("br"):
        br.replace_with("\n")
    for p in node.find_all("p"):
        p.insert_before("\n")
        p.insert_after("\n")


    text = node.get_text("\n", strip=True)
    text = re.sub(r"\n{2,}", "\n", text)
    text = re.sub(r"\s+$", "", text)
    text = re.sub(r"(<[^>]+>)", "", text)
    text = text.strip()

    
    return text, "; ".join(image_urls)




# ========== 목록 수집 ==========
def collect_detail_urls(region: str, code: str, max_pages: int = 5):
    urls, seen = [], set()
    for page in range(1, max_pages + 1):
        list_page = f"{BASE_URL}?search_recruit_stat=02&area=A001&area_detail={code}&cpage={page}"
        print(f"📄 [{region}] 페이지 {page} 요청 중: {list_page}")
        try:
            html = fetch_html(list_page)
        except Exception as e:
            print(f"❌ 요청 실패: {e}")
            break
        soup = BeautifulSoup(html, "lxml")
        before = len(urls)
        for a in soup.select("a[href*='view.do']"):
            href = a.get("href", "").strip()
            if not href:
                continue
            if not href.startswith("http"):
                href = requests.compat.urljoin(list_page, href)
            if href not in seen:
                seen.add(href)
                urls.append(href)
        if len(urls) == before:
            break
        time.sleep(0.5)
    print(f"✅ {region}: {len(urls)}건 수집 완료")
    return urls

# ========== 본문 파싱 ==========
def build_record(region: str, url: str):
    html = fetch_html(url)
    soup = BeautifulSoup(html, "lxml")

    # blind 제거 (이미지보다 뒤에서 하지 않기 위해, 일단 뒤로 이동)
    for b in soup.select(".blind"):
        b.decompose()

    # ✅ 제목 추출
    title_el = soup.select_one("dt.title_v2")
    if title_el:
        for icon in title_el.select(".icon_zone_warp, .icon_zone"):
            icon.decompose()
        item_title = title_el.get_text(strip=True)
    else:
        item_title = "(제목 없음)"

    fields, period_parts, lines = {}, [], []

    # ✅ 기간·문의처 등 정보
    for dd in soup.select("dd.call_tell"):
        date_label = dd.select_one("div.date_txt span.date")
        date_value = dd.select_one("div.date_txt span.count")
        if date_label and date_value:
            label, value = date_label.get_text(strip=True), date_value.get_text(strip=True)
            key_norm, mapped = normalize_key(label), auto_map_key(normalize_key(label))
            if "기간" in label:
                period_parts.append(f"{label} : {value}")
            elif mapped:
                fields[mapped] = f"{label} : {value}" if mapped == "contact" else value
            lines.append(f"{label} : {value}")

    if period_parts:
        fields["period"] = " / ".join(period_parts)

    # ✅ 본문 노드 (없으면 sub_content까지)
    node = soup.select_one("dd.tb_content, div.tb_content, dl.tbl_view_type1 dd.tb_content")
    if not node:
        node = soup.select_one("div.sub_content, div.sub_container_inside") or soup

    # ✅ 이미지 추출 (모든 영역)
    images = []
    for img in soup.select("dd.tb_content img, div.tb_content img, div.sub_content img, div.sub_container_inside img"):
        src = img.get("src", "").strip()
        if not src:
            continue
        if not re.match(r"^https?://", src):
            src = requests.compat.urljoin("https://liveinkorea.kr/", src)
        if src not in images:
            images.append(src)
    image_urls = "; ".join(images)

    # ✅ 본문 텍스트 정리
    text, _ = node_text_with_newlines(node)
    if text:
        lines.append(text.strip())

    # ✅ full_text 구성
    full_text = f"[{region}] {item_title}\n" + "\n".join(lines)
    if images:
        full_text += "\n이미지:\n" + "\n".join(images)

    return {
        "region": region,
        "item_title": item_title,
        "target": fields.get("target", ""),
        "period": fields.get("period", ""),
        "content": text.strip(),
        "method": fields.get("method", ""),
        "contact": fields.get("contact", ""),
        "location": fields.get("location", ""),
        "image": image_urls,
        "full_text": full_text.strip(),
        "source_url": url,
    }



# ========== 전체 실행 ==========
def crawl_all_districts():
    all_records = []
    for region, code in SEOUL_DISTRICTS.items():
        print(f"\n🌐 [{region}] 수집 시작")
        urls = collect_detail_urls(region, code)
        for u in urls:
            try:
                record = build_record(region, u)
                all_records.append(record)
            except Exception as e:
                print(f"⚠️ [{region}] {u} 오류: {e}")
    return all_records

# ========== 저장 ==========
def save_all(records):
    out = pathlib.Path("out")
    out.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(records)
    for c in COLS:
        if c not in df.columns:
            df[c] = ""
    df = df[COLS]
    path_csv = out / "서울전체_가족센터.csv"
    path_json = out / "서울전체_가족센터.json"
    df.to_csv(path_csv, index=False, encoding="utf-8-sig")
    df.to_json(path_json, orient="records", force_ascii=False, indent=2)
    print(f"\n💾 통합 저장 완료 ({len(df)}건)\n📁 {path_csv}")

# ===== MAIN =====
if __name__ == "__main__":
    print("🚀 서울 25개구 가족센터 프로그램 통합 크롤링 시작")
    records = crawl_all_districts()
    save_all(records)
