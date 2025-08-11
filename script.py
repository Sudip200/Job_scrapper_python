"""
Careers crawler + analytics starter.

Features:
- Use requests+bs4 for static pages
- Use Playwright sync for dynamic pages (fallback)
- robots.txt checking, polite rate limiting, retries
- Keyword matching for tech stacks & languages
- Experience bucketing via regex heuristics
- Output JSON/CSV analytics
"""

import time
import json
import re
import logging
import random
from pathlib import Path
from urllib.parse import urljoin, urlparse
import tldextract
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from requests.adapters import HTTPAdapter, Retry

# Playwright (sync) for dynamic pages
from playwright.sync_api import sync_playwright

# -----------------------------
# Configuration (customize)
# -----------------------------
CAREERS_PAGES_LINKS = [
    "https://www.google.com/about/careers/applications/jobs/results/",              # Google[1]
    "https://www.microsoft.com/en-us/careers",                                     # Microsoft[2]
    "https://www.visa.co.in/en_in/jobs/",                                          # Visa
    "https://careers.x.com/en",                                                    # X (formerly Twitter)
    "https://careers.mastercard.com/us/en/search-results",                         # Mastercard
    "https://www.linkedin.com/company/linkedin/jobs/",                             # LinkedIn
    "https://careers.airbnb.com/",                                                 # Airbnb
    "https://www.salesforce.com/company/careers/",                                 # Salesforce
    "https://jobs.netflix.com/",                                                   # Netflix
    "https://www.spotifyjobs.com/",                                                # Spotify
    "https://jobs.canva.com/",                                                     # Canva
    "https://www.nvidia.com/en-us/about-nvidia/careers/",                          # Nvidia
    "https://www.adobe.com/careers.html",                                          # Adobe
    "https://www.cisco.com/c/en/us/about/careers.html",                            # Cisco
    "https://www.tesla.com/careers",                                               # Tesla
    "https://www.redbull.com/int-en/jobs",                                         # Red Bull
    "https://www.pinterestcareers.com/",                                           # Pinterest
    "https://www.openai.com/careers",                                              # OpenAI
    "https://www.slite.com/careers",                                               # Slite
    "https://jobs.booking.com/",                                                   # Booking.com
    "https://www.ikea.com/us/en/this-is-ikea/work-with-us/",                       # IKEA
    "https://www.accenture.com/in-en/careers",                                     # Accenture[3]
    "https://www.ibm.com/in-en/careers",                                           # IBM[8]
    "https://www.metacareers.com",                                                 # Meta (Facebook)[17]
    "https://www.amazon.jobs/en/",                                                 # Amazon
    "https://jobs.apple.com/en-in/search?location=india-INDC&page=2",              # Apple[19]
    "https://www.oracle.com/in/careers/",                                          # Oracle[20]
    "https://www.linkedin.com/company/intel-corporation/jobs",                     # Intel[13]
    "https://www.linkedin.com/company/sap/jobs",                                   # SAP[22]
    "https://www.pwc.com/gx/en/careers.html",                                      # PwC[23]
    "https://www.deloitte.com/in/en/careers.html",                                 # Deloitte[36]
    "https://www.linkedin.com/company/ey/jobs",                                # EY[37]
    "https://www.kpmguscareers.com",                                               # KPMG[38]
    "https://careers.unilever.com",                                                # Unilever[39]
    "https://www.pgcareers.com/in/en",                                             # Procter & Gamble[40]
    "https://www.careers.jnj.com/en/",                                             # Johnson & Johnson[41]
    "https://careers.loreal.com",                                                  # L'Oréal[34]
    "https://jobs.siemens.com",                                                    # Siemens[44]
    "https://www.ge.com/careers",                                                  # GE (General Electric)
    "https://careers.geaerospace.com/global/en/pune",                              # GE Aerospace[50]
    "https://careers.gehealthcare.com",                                            # GE HealthCare[52]
    "https://careers.gevernova.com",                                               # GE Vernova[55]
    "https://careers.walmart.com/us/jobs",                             # Walmart
    "https://www.cvshealth.com/about-cvs-health/careers",              # CVS Health
    "https://careers.unitedhealthgroup.com/",                          # UnitedHealth Group
    "https://www.berkshirehathaway.com/employment/jobs.html",          # Berkshire Hathaway
    "https://careers.mckesson.com/",                                   # McKesson
    "https://www.amerisourcebergen.com/careers",                       # AmerisourceBergen
    "https://corporate.exxonmobil.com/Careers",                        # Exxon Mobil
    "https://www.att.jobs/",                                           # AT&T
    "https://careers.costco.com/",                                     # Costco
    "https://jobs.cigna.com/",                                         # Cigna
    "https://www.cardinalhealth.com/en/careers.html",                  # Cardinal Health
    "https://jobs.walgreens.com/",                                     # Walgreens Boots Alliance
    "https://jobs.kroger.com/",                                        # Kroger
    "https://careers.homedepot.com/",                                  # Home Depot
    "https://careers.jpmorgan.com/",                                   # JPMorgan Chase
    "https://jobs.verizon.com/",                                       # Verizon
    "https://corporate.ford.com/careers.html",                         # Ford
    "https://search-careers.gm.com/",                                  # General Motors
    "https://jobs.anthem.com/",                                        # Anthem (Elevance Health)
    "https://www.centene.com/careers/",                                # Centene
    "https://www.fanniemae.com/careers",                               # Fannie Mae
    "https://jobs.comcast.com/",                                       # Comcast
    "https://careers.chevron.com/",                                    # Chevron
    "https://jobs.dell.com/",                                          # Dell Technologies
    "https://careers.bankofamerica.com/en-us",                         # Bank of America
    "https://jobs.target.com/",                                        # Target
    "https://jobs.lowes.com/",                                         # Lowe's
    "https://careers.marathonpetroleum.com/",                          # Marathon Petroleum
    "https://careers.citigroup.com/students-and-graduates/",           # Citi
    "https://www.metacareers.com/",                                    # Meta Platforms
    "https://www.jobs-ups.com/",                                       # UPS
    "https://www.careers.jnj.com/en/",                                 # Johnson & Johnson
    "https://jobs.wellsfargo.com/",                                    # Wells Fargo
    "https://www.ge.com/careers",                                      # General Electric
    "https://www.statefarm.com/careers",                               # State Farm
    "https://jobs.intel.com/",                                         # Intel
    "https://careers.humana.com/",                                     # Humana
    "https://www.ibm.com/employment/",                                 # IBM
    "https://www.pgcareers.com/in/en",                                 # Procter & Gamble
    "https://www.pepsicojobs.com/main/",                               # PepsiCo
    "https://careers.fedex.com/fedex/",                                # FedEx
    "https://careers.metlife.com/",                                    # MetLife
    "https://careers.hp.com/",                                         # HP
    "https://careers.rtx.com/",                                        # Raytheon Technologies
    "https://www.disneycareers.com/",                                  # Disney
    "https://www.lockheedmartinjobs.com/",                             # Lockheed Martin
    "https://www.boeing.com/careers/",                                 # Boeing
    "https://careers.goldmansachs.com/",                               # Goldman Sachs
    "https://www.morganstanley.com/about-us/careers",                  # Morgan Stanley
    "https://careers.hcahealthcare.com/",                              # HCA Healthcare
    "https://jobs.merck.com/",                                         # Merck
    "https://www.bestbuy-jobs.com/",                                   # Best Buy
    "https://jobs.abbvie.com/",                                        # AbbVie
    "https://careers.abbott/",                                         # Abbott Laboratories
    "https://careers.coca-colacompany.com/",                           # Coca-Cola
    "https://careers.honeywell.com/us/en",                             # Honeywell International
    "https://jobs.tjx.com/",                                           # TJX Companies
    "https://www.nike.com/careers"
]

BACKEND_TECH_STACK = [
    "spring boot", "spring", "node js", "nodejs", "express js", "express", "nest js", "nestjs",
    ".net", "flask", "django", "php", "laravel", "go", "golang", "rails", "ruby on rails"
]
FRONTEND_TECH_STACKS = ["react", "angular", "vue", "svelte", "ember", "backbone", "next.js", "nuxt"]
LANGUAGES = ["java", "python", "c++", "c#", "javascript", "typescript", "rust", "go", "php", "ruby"]
EXPERIENCE_BUCKETS = ["0", "1-2", "2-4", "4-8", ">8"]

USER_AGENT = "Mozilla/5.0 (compatible; CareerCrawler/1.0; +https://example.com/bot)"
RATE_LIMIT_SECONDS = 1.0  # base delay between requests (add jitter)
REQUEST_TIMEOUT = 15  # seconds
STATE_FILE = Path("crawler_state.json")
OUTPUT_FILE = Path("career_analytics.json")

# -----------------------------
# Logging setup
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger("career-crawler")

# -----------------------------
# Requests session with retries
# -----------------------------
session = requests.Session()
session.verify = False
retries = Retry(total=3, backoff_factor=0.6, status_forcelist=(429, 500, 502, 503, 504))
session.mount("https://", HTTPAdapter(max_retries=retries))
session.headers.update({"User-Agent": USER_AGENT})

# -----------------------------
# Utilities
# -----------------------------
def polite_sleep():
    time.sleep(RATE_LIMIT_SECONDS + random.random() * 0.5)

def is_allowed_by_robots(url):
    # Simple robots check - uses /robots.txt
    parsed = urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    try:
        r = session.get(robots_url, timeout=REQUEST_TIMEOUT)
        robots_text = r.text.lower()
        # VERY simple: if "disallow: /" present for all user-agents, block; for more advanced, use urllib.robotparser
        if "disallow: /" in robots_text:
            logger.warning(f"robots.txt disallows crawling {parsed.netloc}")
            return False
        return True
    except Exception:
        # if robots unobtainable, be conservative but allow
        return True

def fetch_static(url):
    """Fetch page with requests + bs4. Returns (text, final_url)."""
    try:
        r = session.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.text, r.url
    except Exception as e:
        logger.debug(f"Static fetch failed {url}: {e}")
        return None, url

def fetch_dynamic(url, playwright_browser=None):
    """Render page with Playwright (sync). Returns (text, final_url)."""
    try:
        if playwright_browser is None:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(user_agent=USER_AGENT)
                page = context.new_page()
                page.goto(url, timeout=REQUEST_TIMEOUT * 1000)
                page.wait_for_load_state("networkidle", timeout=REQUEST_TIMEOUT * 1000)
                content = page.content()
                final = page.url
                browser.close()
                return content, final
        else:
            # if passed an open browser, use it (minor optimization)
            context = playwright_browser.new_context(user_agent=USER_AGENT)
            page = context.new_page()
            page.goto(url, timeout=REQUEST_TIMEOUT * 1000)
            page.wait_for_load_state("networkidle", timeout=REQUEST_TIMEOUT * 1000)
            content = page.content()
            final = page.url
            context.close()
            return content, final
    except Exception as e:
        logger.debug(f"Dynamic fetch failed: {e}")
        return None, url

# -----------------------------
# Heuristics to find job links on a careers page
# -----------------------------
COMMON_JOB_PATH_KEYWORDS = [
    "careers", "careers/jobs", "jobs", "careers/search", "openings", "positions", "vacancies", "opportunities"
]

def discover_job_links(root_url, html, max_links=200):
    """Return list of job-listing / job-detail links discovered on a career page HTML."""
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    anchors = soup.find_all("a", href=True)
    base = root_url
    links = set()
    for a in anchors:
        href = a["href"].strip()
        if href.startswith("#") or href.lower().startswith("mailto:"):
            continue
        # make absolute
        href_abs = urljoin(base, href)
        # filter by same domain (optional)
        if tldextract.extract(href_abs).registered_domain != tldextract.extract(root_url).registered_domain:
            continue
        # heuristic - include links with certain substrings or those likely to be job detail pages
        lower = href_abs.lower()
        if any(k in lower for k in COMMON_JOB_PATH_KEYWORDS) or re.search(r"/jobs?/", lower) or re.search(r"/job[s]?[-_/\w]+", lower):
            links.add(href_abs)
        # also capture /job/123 or /positions/slug
        if re.search(r"/(position|opening|job|career|vacancy|opportunity)s?[-_/]?\d+", lower) or len(links) < 5:
            links.add(href_abs)

        if len(links) >= max_links:
            break
    return list(links)

# -----------------------------
# Job parsing heuristics
# -----------------------------
def extract_text_from_soup(soup):
    """Extract main textual content from a job detail page (simple heuristic)."""
    # try common containers
    for sel in ["div.job-description", "div.job-desc", "div#job-description", "div.description", "section.job", "article"]:
        node = soup.select_one(sel)
        if node:
            return node.get_text(separator="\n").strip()
    # fallback: largest text block
    paragraphs = soup.find_all(["p", "div", "section"])
    best = ""
    for p in paragraphs:
        txt = p.get_text(separator=" ").strip()
        if len(txt) > len(best):
            best = txt
    return best

def find_experience(text):
    """Return normalized experience bucket from a job text (heuristic)."""
    text_low = text.lower()
    # patterns like "2+ years", "3-5 years", "0-1", "fresher", "entry level"
    m = re.search(r'(\d+)\s*\+\s*years?', text_low)
    if m:
        years = int(m.group(1))
        return bucket_from_years(years)
    m = re.search(r'(\d+)\s*-\s*(\d+)\s*years?', text_low)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        mid = (a + b) / 2.0
        return bucket_from_years(mid)
    m = re.search(r'(\d+)\s*years?', text_low)
    if m:
        yrs = int(m.group(1))
        return bucket_from_years(yrs)
    if "fresher" in text_low or "entry level" in text_low or "0 years" in text_low:
        return "0"
    return None

def bucket_from_years(years):
    if years < 1:
        return "0"
    if 1 <= years <= 2:
        return "1-2"
    if 2 < years <= 4:
        return "2-4"
    if 4 < years <= 8:
        return "4-8"
    return ">8"

def match_keywords(text, keywords):
    """Return set of keywords found (case-insensitive, whole word-ish)."""
    found = set()
    text_low = text.lower()
    # sort longer first to avoid partial overlap (e.g., "node" vs "node js")
    sorted_k = sorted(keywords, key=lambda x: -len(x))
    for kw in sorted_k:
        kw_low = kw.lower()
        # basic word boundary match
        pattern = r'\b' + re.escape(kw_low) + r'\b'
        if re.search(pattern, text_low):
            found.add(kw)
    return found

# -----------------------------
# Main crawl + parse worker
# -----------------------------
def parse_job_page(url, html):
    """Parse a job detail page and extract fields."""
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    # title heuristics
    title = None
    if soup.title and soup.title.string:
        title = soup.title.string.strip()
    h1 = soup.find("h1")
    if h1 and h1.get_text().strip():
        title = h1.get_text().strip()
    # location heuristics
    location = None
    for sel in ['.job-location', '.location', 'span.location', 'p.location']:
        node = soup.select_one(sel)
        if node:
            location = node.get_text().strip()
            break
    # description
    description = extract_text_from_soup(soup) or soup.get_text(separator="\n")
    # posted date attempt
    posted = None
    for attr in ['date-posted', 'posted', 'published', 'date']:
        node = soup.find(attrs={"class": re.compile(attr, re.I)})
        if node:
            try:
                posted = dateparser.parse(node.get_text(), fuzzy=True).isoformat()
                break
            except Exception:
                continue
    # experience
    exp_bucket = find_experience(description or "")
    # skills matching
    backend_found = match_keywords(description or "", BACKEND_TECH_STACK)
    frontend_found = match_keywords(description or "", FRONTEND_TECH_STACKS)
    languages_found = match_keywords(description or "", LANGUAGES)
    # remote / onsite
    remote_flag = None
    low = (title or "") + "\n" + (description or "")
    l = low.lower()
    if "remote" in l or "work from home" in l:
        remote_flag = "remote"
    elif "hybrid" in l:
        remote_flag = "hybrid"
    elif "on-site" in l or "onsite" in l or "in-office" in l:
        remote_flag = "onsite"

    return {
        "url": url,
        "title": title,
        "location": location,
        "posted": posted,
        "experience_bucket": exp_bucket,
        "backend": sorted(list(backend_found)),
        "frontend": sorted(list(frontend_found)),
        "languages": sorted(list(languages_found)),
        "remote_type": remote_flag,
        "raw_description_snippet": (description or "")[:2000]
    }

def crawl_careers_page(root_url, use_playwright=False, max_job_pages=200):
    """Crawl a career root and return list of job records."""
    logger.info(f"Crawling {root_url}")
    if not is_allowed_by_robots(root_url):
        logger.warning(f"Blocked by robots: {root_url}")
        return []

    html, final = fetch_static(root_url)
    polite_sleep()
    job_pages = discover_job_links(root_url, html)
    logger.info(f"Discovered {len(job_pages)} candidate job-related links from {root_url}")

    results = []
    # optionally initialize playwright browser once
    pw_browser = None
    if use_playwright:
        pw = sync_playwright().start()
        pw_browser = pw.chromium.launch(headless=True)
    try:
        for link in job_pages[:max_job_pages]:
            try:
                # prefer static fetch; if content seems minimal or JS heavy, fallback to dynamic
                page_html, final_url = fetch_static(link)
                polite_sleep()
                if not page_html or len(page_html) < 500:
                    logger.info(f"Static fetch small or failed for {link} -> trying Playwright")
                    page_html, final_url = fetch_dynamic(link, playwright_browser=pw_browser)
                    polite_sleep()
                # check if this looks like a list page or single job detail
                # if it's a list page, try to find job detail links inside and expand
                soup = BeautifulSoup(page_html or "", "html.parser")
                # heuristic: lots of links with job slugs -> expand
                inner_job_links = discover_job_links(final_url, page_html, max_links=50)
                # if page is likely a job detail (has h1 + description), parse it
                parsed = parse_job_page(final_url, page_html)
                if parsed and (parsed.get("title") or parsed.get("raw_description_snippet")):
                    results.append(parsed)
                # add discovered inner job links (parse them too)
                for job_link in inner_job_links:
                    if len(results) >= max_job_pages:
                        break
                    try:
                        page_html2, final2 = fetch_static(job_link)
                        polite_sleep()
                        if not page_html2 or len(page_html2) < 500:
                            page_html2, final2 = fetch_dynamic(job_link, playwright_browser=pw_browser)
                            polite_sleep()
                        parsed2 = parse_job_page(final2, page_html2)
                        if parsed2:
                            results.append(parsed2)
                    except Exception as ie:
                        logger.debug(f"failed to parse inner {job_link}: {ie}")
                if len(results) >= max_job_pages:
                    break
            except Exception as e:
                logger.warning(f"failed to crawl link {link}: {e}")
    finally:
        if pw_browser:
            pw_browser.close()
            pw.stop()

    logger.info(f"Crawled {len(results)} job pages under {root_url}")
    return results

# -----------------------------
# Analytics generation
# -----------------------------
def compute_analytics(all_jobs):
    stats = {
        "total_jobs": len(all_jobs),
        "by_backend": {},
        "by_frontend": {},
        "by_language": {},
        "by_experience_bucket": {},
        "by_remote_type": {},
        "by_location_top": {}
    }
    for job in all_jobs:
        # backend
        for b in job.get("backend", []):
            stats["by_backend"][b] = stats["by_backend"].get(b, 0) + 1
        # frontend
        for f in job.get("frontend", []):
            stats["by_frontend"][f] = stats["by_frontend"].get(f, 0) + 1
        # languages
        for l in job.get("languages", []):
            stats["by_language"][l] = stats["by_language"].get(l, 0) + 1
        # experience
        eb = job.get("experience_bucket") or "unspecified"
        stats["by_experience_bucket"][eb] = stats["by_experience_bucket"].get(eb, 0) + 1
        # remote
        rt = job.get("remote_type") or "unspecified"
        stats["by_remote_type"][rt] = stats["by_remote_type"].get(rt, 0) + 1
        # location
        loc = job.get("location") or "unspecified"
        stats["by_location_top"][loc] = stats["by_location_top"].get(loc, 0) + 1

    # optionally sort top locations
    stats["by_location_top"] = dict(sorted(stats["by_location_top"].items(), key=lambda kv: -kv[1])[:20])
    return stats

# -----------------------------
# Main orchestration
# -----------------------------
def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}

def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))

def main(careers_list, use_playwright=False):
    state = load_state()
    aggregated_jobs = []
    for root in careers_list:
        try:
            if root in state.get("completed", []):
                logger.info(f"Skipping already processed {root}")
                continue
            jobs = crawl_careers_page(root, use_playwright=use_playwright)
            # tag company domain
            for j in jobs:
                j["company_root"] = root
            aggregated_jobs.extend(jobs)
            # persist incremental
            state.setdefault("completed", []).append(root)
            state.setdefault("jobs", []).extend(jobs)
            save_state(state)
        except Exception as e:
            logger.exception(f"Error crawling {root}: {e}")

    analytics = compute_analytics(aggregated_jobs)
    OUTPUT_FILE.write_text(json.dumps({"jobs": aggregated_jobs, "analytics": analytics}, indent=2))
    logger.info(f"Wrote analytics to {OUTPUT_FILE}")
    return {"jobs": aggregated_jobs, "analytics": analytics}

# -----------------------------
# Run
# -----------------------------
if __name__ == "__main__":
    # Example run: set use_playwright=True if you expect many JS heavy pages.
    results = main(CAREERS_PAGES_LINKS, use_playwright=True)
    print("Total jobs:", len(results["jobs"]))
    print("Top backend hits:", results["analytics"]["by_backend"])
