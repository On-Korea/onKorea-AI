# -*- coding: utf-8 -*-
# 중구청 '교육문화' 단일 URL 크롤러 (본문 노드 강제 탐색 핫픽스)

import re, json, time, pathlib
import pandas as pd
import requests
from bs4 import BeautifulSoup

URLS = ["https://www.junggu.seoul.kr/content.do?cmsid=16540&mode=view&cid=1371261581"]
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; DataCollectionBot/1.0)"}

REGION = "중구"
CATEGORY = "교육문화"

COLS = [
    "region","source_category","item_title","target","application_period","content",
    "application_method","contact","notes","purchase_method","location","full_text","source_url"
]

ANGLE_TITLE_PAT = re.compile(r"^[\s]*[<〈《](.+?)[>〉》]\s*$")
KEY_VALUE_PAT   = re.compile(r"\s*(.+?)\s*[:：\-–]\s*(.+)$")
ZERO_WIDTH      = re.compile(r"[\u200b\u200c\u200d\u2060\ufeff]")
PHONE_SEQ       = re.compile(r"(?:\d{2,4}\s*-\s*\d{3,4}\s*-\s*\d{4})(?:\s*/\s*(?:\d{2,4}\s*-\s*\d{3,4}\s*-\s*\d{4}))*")

KEY_ALIAS = {
    "대상":"target","지원대상":"target","지원자격":"target",
    "내용":"content","주요내용":"content","지원내용":"content","보장내용":"content","제출서류":"content",
    "지원한도":"content","지원금액":"content","지원 금액":"content","한도":"content","지원형태":"content",
    "사용처":"notes","기타":"notes","비고":"notes",
    "이용방법":"application_method","신청방법":"application_method","신청":"application_method",
    "청구방법":"application_method","인증방법":"application_method",
    "문의":"contact","문 의":"contact","연락처":"contact",
    "기간":"application_period","신청기간":"application_period","신청기한":"application_period",
    "청구기간":"application_period","보장기간":"application_period","추진일정":"application_period",
    "일시":"application_period","일 시":"application_period",
    "구입방법":"purchase_method","구 입 방법":"purchase_method",
    "장소":"location","장 소":"location",
}

NOISE_LINE_PAT = re.compile("|".join([
    r"\[.+?\].*", r"해당 메뉴에 대한 만족도", r"아주\s*만족", r"보통", r"아주\s*불만"
]))

def fetch_html(url: str) -> str:
    for i in range(3):
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.ok and r.text.strip(): return r.text
        time.sleep(1+i)
    r.raise_for_status()

def choose_best_node(soup: BeautifulSoup) -> BeautifulSoup:
    # 후보 중에서 본문 신호가 있는지 확인
    for el in soup.find_all(text=True):
        t = el.strip()
        if not t:
            continue
        if re.search(r"[○ㅇ☞]", t) and re.search(r"(장\s*소|일\s*시|대\s*상|문의|주요내용)", t):
            # 조상 div/section/article 반환
            p = el.parent
            while p and p.name not in ("div","section","article","main","td"):
                p = p.parent
            if p:
                return p
    # 못 찾으면 fallback
    return soup.select_one("#content") or soup


def has_body_signals(text: str) -> bool:
    text = ZERO_WIDTH.sub("", text)
    # 글머리나 필드 키워드가 2개 이상 보이면 본문으로 판단
    signals = 0
    if re.search(r"[○ㅇ☞]", text): signals += 1
    if re.search(r"(장\s*소|일\s*시|대\s*상|주요내용|문\s*의)", text): signals += 1
    if re.search(r"\d{4}\s*-\s*\d{2}\s*-\s*\d{2}", text): signals += 1  # 작성일 같은 날짜
    return signals >= 2

def node_text_with_newlines(node: BeautifulSoup) -> str:
    for br in node.find_all("br"): br.replace_with("\n")
    for p in node.find_all(["p","li","div","tr","td"]):
        if p.text and not p.text.endswith("\n"): p.append("\n")
    text = node.get_text("\n")
    text = re.sub(r"\r","",text)
    text = re.sub(r"[ \t]+"," ",text)
    text = re.sub(r"\n{3,}","\n\n",text).strip()
    return text

def extract_item_title(text: str, soup: BeautifulSoup) -> str:
    # 1) [동명] 제목 패턴
    for ln in [ln.strip() for ln in text.splitlines() if ln.strip()]:
        if re.match(r"\[[^\]]+]\s*.+", ln):
            return ln
    # 2) DOM의 h3 중 섹션명이 아닌 것
    for h in soup.select("h3, .title, .view_title, .tit"):
        t = h.get_text(" ", strip=True)
        if t and t not in ("교육·문화","외국인 지원","복지"):
            return t
    # 3) <title>
    return (soup.title.get_text(strip=True) if soup.title else "").strip()

def trim_noise_lines(lines):
    cleaned=[]
    for ln in lines:
        t=ln.strip()
        if re.match(r"^(?:[ㅇ○]\s*)?문\s*의",t) or re.match(r"^(?:[ㅇ○]\s*)?연락처",t):
            t=re.split(r"\[.*?]|해당 메뉴에 대한 만족도|아주\s*만족|보통|아주\s*불만",t)[0].strip()
            if t: cleaned.append(t); continue
        if NOISE_LINE_PAT.search(t): break
        cleaned.append(ln)
    return cleaned

def split_items(text: str, fallback_title: str):
    lines=[ln.strip() for ln in text.splitlines() if ln.strip()]
    NAV_DROPS={"홈","복지","외국인 지원","교육·문화","공유하기","링크복사"}
    lines=[ln for ln in lines if ln not in NAV_DROPS and not ln.endswith("상세 : 제목, 작성자, 작성일, 조회, 내용, 첨부로 구성")]
    return [{"item_title": fallback_title or "(본문)", "detail_lines": lines}]

def normalize_key(k: str) -> str:
    k = ZERO_WIDTH.sub("", k)
    k = k.replace("\u00A0"," ").replace("\u3000"," ")
    k = re.sub(r"[★☆•·\[\](){}]","",k)
    k = re.sub(r"[-–—]","",k)
    k = re.sub(r"\s+","",k)
    return k

def append_field(fields, key, val):
    if not val: return
    fields[key] = f"{fields[key]} / {val}" if key in fields and fields[key] else val

def parse_detail_lines_to_fields(detail_lines):
    fields={}
    current_section=None
    BULLET_PREFIXES=("☞","•","·","-","▶","▷","■","※","●","○")
    NUM_BULLET=re.compile(r"^\d+\.\s*|^[①②③④⑤⑥⑦⑧⑨⑩]")

    def strip_leading_marker(s:str)->str:
        return re.sub(r"^[ㅇ○]\s*","",s)

    for raw in detail_lines:
        raw_norm = ZERO_WIDTH.sub("", raw).strip()
        if not raw_norm: continue

        # 1) 키:값
        m = KEY_VALUE_PAT.match(strip_leading_marker(raw_norm))
        if m:
            pre, post = m.group(1).strip(), m.group(2).strip()
            pre_norm=normalize_key(pre)
            k_std=KEY_ALIAS.get(pre_norm)
            if k_std:
                v=post
                if k_std=="contact":
                    v=re.split(r"\[|첨부|바로보기|목록|이전글|다음글|만족|불만|확인", v)[0].strip()
                    v=re.split(r"\.(pdf|hwp|hwpx|docx?|xlsx?|pptx?|zip|jpe?g|png)", v, flags=re.I)[0].strip()
                    mphone=PHONE_SEQ.search(v)
                    if mphone: v=v[:mphone.end()].strip()
                append_field(fields,k_std,v)
                current_section=k_std
                continue

        # 2) 콜론 없는 헤더(+인라인 내용)
        head_candidate = strip_leading_marker(raw_norm)
        words=head_candidate.split()
        matched=False
        for i in range(len(words),0,-1):
            pre_part=" ".join(words[:i])
            pre_norm=normalize_key(pre_part)
            if pre_norm in KEY_ALIAS:
                current_section=KEY_ALIAS[pre_norm]
                rest=head_candidate[len(pre_part):].strip()
                if rest: append_field(fields,current_section,rest)
                matched=True; break
        if matched: continue

        # 3) 글머리/번호
        if raw_norm.startswith(BULLET_PREFIXES) or NUM_BULLET.match(raw_norm):
            append_field(fields, current_section or "content", raw_norm)
            continue

        # 4) 일반 줄
        append_field(fields, current_section or "content", raw_norm)

    return fields

def item_block_text(item):
    title=(item.get("item_title") or "").strip()
    body="\n".join([ln.strip() for ln in item.get("detail_lines",[]) if ln.strip()])
    return f"▶ {title}\n{body}" if title and body else (f"▶ {title}" if title else body)

def build_records(url: str):
    html = fetch_html(url)
    soup = BeautifulSoup(html, "lxml")
    node = choose_best_node(soup)          # ★ 본문 노드 강제 선택
    text = node_text_with_newlines(node)

    item_title = extract_item_title(text, soup)
    items = split_items(text, fallback_title=item_title)

    records=[]
    for it in items:
        clean_lines = trim_noise_lines(it["detail_lines"])
        fields = parse_detail_lines_to_fields(clean_lines)
        records.append({
            "region": REGION,
            "source_category": CATEGORY,
            "item_title": it["item_title"],
            "target": fields.get("target"),
            "application_period": fields.get("application_period"),
            "content": fields.get("content"),
            "application_method": fields.get("application_method"),
            "contact": fields.get("contact"),
            "notes": fields.get("notes"),
            "purchase_method": fields.get("purchase_method"),
            "location": fields.get("location"),
            "full_text": item_block_text({"item_title": it["item_title"], "detail_lines": clean_lines}),
            "source_url": url
        })
    return records

def _canon_text(x:str)->str:
    if x is None or (isinstance(x,float) and pd.isna(x)): return ""
    s=str(x)
    s=re.sub(r"[\u200b\u200c\u200d\u2060\ufeff]","",s)
    s=s.replace("\u00A0"," ").replace("\u3000"," ")
    s=re.sub(r"[ \t]+"," ",s)
    s=re.sub(r"\s*\n\s*","\n",s)
    s=re.sub(r"\n{2,}","\n",s)
    return s.strip()

def save_records(records, outdir="out", base_name="중구_교육문화데이터추출"):
    out=pathlib.Path(outdir); out.mkdir(parents=True, exist_ok=True)
    df=pd.DataFrame(records)
    for c in COLS:
        if c not in df.columns: df[c]=None
    df=df[COLS]
    compare_cols=[c for c in COLS if c not in ("source_url","full_text")]
    for c in compare_cols: df[c]=df[c].apply(_canon_text)
    df=df.drop_duplicates(subset=compare_cols, keep="first").reset_index(drop=True)
    csv_path=out/f"{base_name}.csv"; json_path=out/f"{base_name}.json"; xlsx_path=out/f"{base_name}.xlsx"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    with open(json_path,"w",encoding="utf-8") as f: json.dump(df.to_dict(orient="records"), f, ensure_ascii=False, indent=2)
    df.to_excel(xlsx_path, index=False, engine="openpyxl")
    print("저장 완료 ✅\n-", csv_path, "\n-", json_path, "\n-", xlsx_path, f"\n총 {len(df)}개")

if __name__ == "__main__":
    all_records=[]
    for url in URLS:
        try:
            all_records.extend(build_records(url))
        except Exception as e:
            print(f"에러: {url} -> {e}")
    if not all_records:
        print("⚠️ 항목을 찾지 못했습니다.")
    else:
        save_records(all_records)
