"""
db/build_listing_recommendations.py — Generate recommended listing copy per ASIN.

Reads content_briefs (tiered keywords) + listings (current title) to produce
a listing_recommendations table with:
  - Recommended title (~200 chars, natural language)
  - 5 bullet points (each answering a distinct customer question)
  - Description (~2000 chars, 4 paragraphs)
  - Backend search terms (leftover unique words)
  - Q&A seed questions

Optimized for Amazon's COSMO/Rufus AI systems: natural language, named
entities, semantic coherence over keyword stuffing.

Adapted from DWC project for Niré Beauty (makeup brushes, PostgreSQL).

Usage:
    python -m db.build_listing_recommendations
"""
from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import psycopg2
import psycopg2.extras
from schema import get_conn

# =====================================================================
# Constants
# =====================================================================

TITLE_CHAR_LIMIT = 200
BULLET_CHAR_TARGET = 250
DESC_CHAR_TARGET = 2000

STOP_WORDS = {
    "a", "an", "the", "and", "or", "for", "of", "in", "to", "with", "by",
    "is", "it", "on", "at", "as", "from", "this", "that", "be", "are",
    "was", "were", "has", "have", "each", "every", "your", "our", "its",
    "set", "brush",  # too generic to count as differentiators
}

# Words to exclude from titles — competitor/noise terms
TITLE_EXCLUDE_WORDS = {
    # Competitor brand names
    "real", "techniques", "morphe", "sigma", "bh", "cosmetics", "elf",
    "jessup", "zoeva", "spectrum", "lamora", "bs-mall", "bestope",
    "docolor", "texamo", "beili",
    # Noise
    "amazon", "brand", "rated", "best", "seller", "top",
}

# =====================================================================
# Bullet theme definitions for makeup brushes
# =====================================================================

BULLET_THEMES = {
    "quality": {
        "triggers": {"professional", "high quality", "premium", "luxury",
                     "award", "vegan", "cruelty free", "synthetic",
                     "soft", "dense", "durable", "best"},
        "question": "What makes these brushes high quality?",
        "label": "Quality & Craftsmanship",
    },
    "completeness": {
        "triggers": {"set", "piece", "kit", "complete", "full",
                     "everything", "starter", "beginner", "all",
                     "collection", "assortment"},
        "question": "What's included in this brush set?",
        "label": "Complete Set Contents",
    },
    "application": {
        "triggers": {"foundation", "powder", "blush", "contour", "eyeshadow",
                     "blending", "concealer", "highlight", "bronzer", "lip",
                     "brow", "eyeliner", "kabuki", "fan", "stippling",
                     "buffing", "flat top"},
        "question": "What makeup looks can I create?",
        "label": "Versatile Application",
    },
    "gift_occasion": {
        "triggers": {"gift", "birthday", "christmas", "present", "women",
                     "teen", "girl", "mom", "her", "anniversary",
                     "valentine", "mother", "daughter", "stocking"},
        "question": "Is this a good gift?",
        "label": "Perfect Gift",
    },
    "value_extras": {
        "triggers": {"case", "bag", "pouch", "sponge", "blender", "cleaner",
                     "holder", "travel", "portable", "organizer", "storage",
                     "guide", "box", "stand"},
        "question": "What extras come with the set?",
        "label": "Accessories & Value",
    },
}


# =====================================================================
# Product attribute parser
# =====================================================================

def parse_product_attributes(product_name: str) -> dict:
    """Extract structured attributes from a Niré Beauty product name."""
    name = product_name or ""
    name_lower = name.lower()

    attrs = {
        "variant": None,        # color/style variant (Glitter, Pink, White, Artistry)
        "piece_count": None,    # e.g. "15piece"
        "is_award_winning": False,
        "is_vegan": False,
        "includes_case": False,
        "includes_sponge": False,
        "includes_cleaner": False,
        "includes_guide": False,
        "includes_gift_box": False,
        "includes_holder": False,
    }

    # Variant detection
    if "glitter" in name_lower:
        attrs["variant"] = "Glitter"
    elif "pink" in name_lower:
        attrs["variant"] = "Pink"
    elif "white" in name_lower:
        attrs["variant"] = "White"
    elif "artistry" in name_lower:
        attrs["variant"] = "Artistry"
    else:
        attrs["variant"] = "Classic"

    # Piece count
    m = re.search(r'(\d+)\s*piece', name_lower)
    if m:
        attrs["piece_count"] = m.group(1)

    # Features
    attrs["is_award_winning"] = "award" in name_lower
    attrs["is_vegan"] = "vegan" in name_lower
    attrs["includes_case"] = "case" in name_lower
    attrs["includes_sponge"] = "sponge" in name_lower or "blender" in name_lower or "beauty blender" in name_lower
    attrs["includes_cleaner"] = "cleaner" in name_lower
    attrs["includes_guide"] = "guide" in name_lower
    attrs["includes_gift_box"] = "gift box" in name_lower
    attrs["includes_holder"] = "holder" in name_lower

    return attrs


# =====================================================================
# Title generator
# =====================================================================

def _unique_words(text: str) -> set[str]:
    """Extract meaningful words from text."""
    return {w.lower() for w in re.findall(r'[a-z]+', text.lower())
            if len(w) > 2 and w.lower() not in STOP_WORDS}


def generate_title(attrs: dict, title_keywords: list[dict],
                   current_title: str) -> tuple[str, int]:
    """
    Generate a recommended product title from attributes and title-tier keywords.
    Returns (title_text, keywords_used_count).
    """
    brand = "Niré Beauty"
    pieces = attrs["piece_count"] or "15"
    variant = attrs["variant"] or ""

    # Build core descriptor
    core_parts = []
    if attrs["is_award_winning"]:
        core_parts.append("Award Winning")
    if variant and variant not in ("Classic", "Artistry"):
        core_parts.append(variant)
    if variant == "Artistry":
        core_parts.append("Artistry")
    core_parts.append(f"{pieces} Piece Professional Makeup Brush Set")

    # Extract unique concept words from title keywords
    used_words = _unique_words(brand + " " + " ".join(core_parts))
    kw_used = 0
    differentiators: list[str] = []

    for kw in title_keywords:
        query = kw["search_query"]
        new_words = _unique_words(query) - used_words - TITLE_EXCLUDE_WORDS
        if new_words:
            diff = _extract_differentiator(query, used_words)
            if diff:
                differentiators.append(diff)
                used_words |= new_words
                kw_used += 1

    # Assemble title
    core_desc = " ".join(core_parts)
    title = f"{brand} {core_desc}"

    # Add key differentiator (e.g. "Vegan", "with Case")
    extras = []
    if attrs["is_vegan"]:
        extras.append("Vegan Makeup Brushes")
    if attrs["includes_case"]:
        extras.append("Case")
    if attrs["includes_sponge"]:
        extras.append("Makeup Sponge")
    if attrs["includes_cleaner"]:
        extras.append("Brush Cleaner")
    if attrs["includes_guide"]:
        extras.append("Guide")
    if attrs["includes_gift_box"]:
        extras.append("Gift Box")
    if attrs["includes_holder"]:
        extras.append("Brush Holder")

    if extras:
        # Build "with X, Y & Z" suffix
        if attrs["is_vegan"]:
            vegan_part = extras.pop(0)
            if extras:
                extras_str = f"{vegan_part} with {', '.join(extras[:-1])}"
                if len(extras) > 1:
                    extras_str += f" & {extras[-1]}"
                elif len(extras) == 1:
                    extras_str = f"{vegan_part} with {extras[0]}"
            else:
                extras_str = vegan_part
        else:
            extras_str = "with " + ", ".join(extras[:-1])
            if len(extras) > 1:
                extras_str += f" & {extras[-1]}"
            elif len(extras) == 1:
                extras_str = f"with {extras[0]}"

        candidate = f"{title}: {extras_str}"
        if len(candidate) <= TITLE_CHAR_LIMIT:
            title = candidate

    # Add keyword differentiators if space allows
    for diff in differentiators[:2]:
        candidate = f"{title}, {diff}"
        if len(candidate) <= TITLE_CHAR_LIMIT:
            title = candidate
        else:
            break

    return title, kw_used


def _extract_differentiator(keyword: str, already_used: set[str]) -> str | None:
    """Extract the new concept from a keyword not already covered."""
    words = keyword.lower().split()
    new_words = [w for w in words
                 if w.lower() not in already_used
                 and w not in STOP_WORDS
                 and w not in TITLE_EXCLUDE_WORDS
                 and len(w) > 2
                 and not re.match(r'^b\d{2}', w)]
    if not new_words:
        return None
    phrase = " ".join(w.capitalize() for w in new_words)
    return phrase


# =====================================================================
# Bullet point generator
# =====================================================================

def _classify_keyword_theme(keyword: str) -> tuple[str, int]:
    """Classify a keyword into a theme."""
    kw_lower = keyword.lower()
    best_theme = "quality"
    best_score = 0

    for theme_name, theme_def in BULLET_THEMES.items():
        score = sum(1 for t in theme_def["triggers"] if t in kw_lower)
        if score > best_score:
            best_score = score
            best_theme = theme_name

    return best_theme, best_score


def generate_bullets(attrs: dict, bullet_keywords: list[dict],
                     title_keywords: list[dict]) -> list[dict]:
    """Generate 5 themed bullet points from bullet-tier keywords."""
    theme_buckets: dict[str, list[dict]] = {t: [] for t in BULLET_THEMES}

    for kw in bullet_keywords:
        theme, score = _classify_keyword_theme(kw["search_query"])
        theme_buckets[theme].append(kw)

    variant = attrs["variant"] or "Classic"
    pieces = attrs["piece_count"] or "15"
    brand = "Niré Beauty"

    bullets = []
    for theme_name, theme_def in BULLET_THEMES.items():
        kws = theme_buckets[theme_name]
        kw_phrases = [k["search_query"] for k in kws[:5]]

        text = _build_bullet_text(theme_name, attrs, kw_phrases, brand, variant, pieces)

        bullets.append({
            "theme": theme_name,
            "label": theme_def["label"],
            "question": theme_def["question"],
            "text": text,
            "keywords_used": kw_phrases,
        })

    return bullets


def _build_bullet_text(theme: str, attrs: dict, kw_phrases: list[str],
                       brand: str, variant: str, pieces: str) -> str:
    """Build a single bullet point text for a theme."""
    is_artistry = variant == "Artistry"

    if theme == "quality":
        if attrs["is_award_winning"]:
            text = (f"AWARD-WINNING QUALITY — {brand} brushes are crafted with ultra-soft, "
                    f"dense synthetic bristles that rival high-end natural hair brushes. "
                    f"{'100% vegan and cruelty-free, our ' if attrs['is_vegan'] else 'Our '}"
                    f"professional-grade brushes deliver flawless blending, precise application, "
                    f"and lasting durability that makeup artists and beauty enthusiasts trust.")
        else:
            text = (f"PROFESSIONAL QUALITY — Crafted with premium synthetic bristles that are "
                    f"incredibly soft yet firm enough for precise application. "
                    f"{'100% vegan and cruelty-free. ' if attrs['is_vegan'] else ''}"
                    f"Each {brand} brush is designed for effortless blending, smooth coverage, "
                    f"and long-lasting performance that elevates your makeup routine.")

    elif theme == "completeness":
        if is_artistry:
            text = (f"COMPLETE ARTISTRY SET — This curated brush collection includes every brush "
                    f"you need for a full face of makeup, from foundation and powder to eyeshadow "
                    f"and blending. Whether you're a beginner learning techniques or a professional "
                    f"building your kit, this set covers every step of your routine.")
        else:
            text = (f"COMPLETE {pieces}-PIECE SET — Everything you need for a flawless full-face "
                    f"look in one set. Includes brushes for foundation, powder, blush, contour, "
                    f"highlight, eyeshadow, blending, lip, and brow — plus specialty brushes for "
                    f"precision work. No gaps in your routine, no extra purchases needed.")

    elif theme == "application":
        text = (f"VERSATILE APPLICATION — From seamless foundation buffing to precise eyeshadow "
                f"blending, each brush is purpose-shaped for its task. The dense kabuki delivers "
                f"airbrushed powder coverage, the angled contour brush sculpts naturally, and the "
                f"tapered blending brushes create effortless gradient effects. Works beautifully "
                f"with powders, creams, and liquid formulas.")

    elif theme == "gift_occasion":
        if attrs["includes_gift_box"]:
            text = (f"PERFECT GIFT — Arrives in a beautiful gift box, ready to give. "
                    f"An ideal present for birthdays, Christmas, Mother's Day, Valentine's Day, "
                    f"or any special occasion. Whether she's a makeup beginner, a beauty enthusiast, "
                    f"or a professional artist, this {brand} set is a thoughtful gift "
                    f"she'll actually use every day.")
        else:
            text = (f"GREAT GIFT FOR HER — A thoughtful present for the makeup lover in your life. "
                    f"Perfect for birthdays, holidays, and special occasions. Whether she's just "
                    f"starting her beauty journey or upgrading her collection, this professional "
                    f"brush set delivers luxury quality at an accessible price point.")

    elif theme == "value_extras":
        extras = []
        if attrs["includes_case"]:
            extras.append("a premium brush case for organized storage and travel")
        if attrs["includes_holder"]:
            extras.append("a stylish brush holder to display your collection")
        if attrs["includes_sponge"]:
            extras.append("a beauty blender sponge for seamless foundation application")
        if attrs["includes_cleaner"]:
            extras.append("a brush cleaner to maintain bristle softness and hygiene")
        if attrs["includes_guide"]:
            extras.append("a makeup guide to help you get the most from each brush")

        if extras:
            extras_text = "; ".join(extras)
            text = (f"BONUS ACCESSORIES INCLUDED — This set comes with more than just brushes: "
                    f"{extras_text}. Everything you need to apply, store, clean, and master your "
                    f"makeup tools in one complete package.")
        else:
            text = (f"EXCEPTIONAL VALUE — More than just a brush set, this complete kit includes "
                    f"everything to start your professional makeup routine. Designed for easy "
                    f"storage and travel-friendly convenience, so your brushes stay protected "
                    f"and organized wherever you go.")

    else:
        text = (f"{brand} {variant} Makeup Brush Set — professional quality brushes "
                f"designed for flawless makeup application every day.")

    return text


# =====================================================================
# Description generator
# =====================================================================

def generate_description(attrs: dict, title_keywords: list[dict],
                         bullet_keywords: list[dict],
                         nth_keywords: list[dict]) -> str:
    """Generate a ~2000 char product description in 4 paragraphs."""
    brand = "Niré Beauty"
    variant = attrs["variant"] or "Classic"
    pieces = attrs["piece_count"] or "15"
    is_artistry = variant == "Artistry"

    # P1: Hero statement
    variant_desc = ""
    if variant == "Glitter":
        variant_desc = "stunning glitter-finish handles that add sparkle to your vanity"
    elif variant == "Pink":
        variant_desc = "elegant pink handles that bring a touch of feminine luxury to your routine"
    elif variant == "White":
        variant_desc = "sleek white handles for a clean, modern aesthetic"
    elif variant == "Artistry":
        variant_desc = "a curated selection of artistry-focused brushes with a professional brush holder"
    else:
        variant_desc = "classic elegant handles designed for the professional and everyday user alike"

    p1 = (f"The {brand} {variant} {pieces}-Piece Professional Makeup Brush Set features "
          f"{variant_desc}. "
          f"{'Award-winning and trusted by beauty enthusiasts worldwide, each ' if attrs['is_award_winning'] else 'Each '}"
          f"brush is crafted with ultra-soft, dense synthetic bristles that pick up product "
          f"effortlessly and blend seamlessly for a flawless, airbrushed finish. "
          f"{'100% vegan and cruelty-free — premium quality without compromise.' if attrs['is_vegan'] else ''}")

    # P2: Who it's for + application versatility
    p2 = (f"Whether you're a makeup beginner learning the basics or an experienced artist "
          f"perfecting your technique, this set has every brush you need. Foundation, concealer, "
          f"powder, blush, contour, highlight, eyeshadow, blending, brow, and lip — each brush "
          f"is purpose-shaped for its task, so you get professional results without the guesswork. "
          f"Works beautifully with powder, cream, and liquid formulas across all skin types.")

    # P3: What's included + quality story
    included_items = ["professional makeup brushes"]
    if attrs["includes_case"]:
        included_items.append("a premium brush case")
    if attrs["includes_holder"]:
        included_items.append("a brush holder")
    if attrs["includes_sponge"]:
        included_items.append("a beauty blender sponge")
    if attrs["includes_cleaner"]:
        included_items.append("a brush cleaner")
    if attrs["includes_guide"]:
        included_items.append("a step-by-step makeup guide")

    included_str = ", ".join(included_items[:-1]) + f", and {included_items[-1]}" if len(included_items) > 2 else " and ".join(included_items)

    p3 = (f"This complete set includes {included_str}. "
          f"The synthetic bristles are designed to be non-shedding, easy to clean, and maintain "
          f"their shape wash after wash. Unlike natural hair brushes, {brand}'s synthetic "
          f"bristles won't absorb excess product, so you use less makeup and achieve more "
          f"consistent coverage every application.")

    # P4: Gift + care + CTA
    if attrs["includes_gift_box"]:
        p4 = (f"Beautifully packaged in a gift-ready box, this set makes an unforgettable "
              f"present for birthdays, Christmas, Valentine's Day, Mother's Day, or any special "
              f"occasion. To keep your brushes performing their best, clean regularly with the "
              f"included brush cleaner or mild soap and warm water. Lay flat to dry. "
              f"Upgrade your makeup routine with {brand} — the brushes trusted by beauty lovers "
              f"around the world.")
    else:
        p4 = (f"Makes a wonderful gift for the beauty enthusiast in your life. To keep your "
              f"brushes performing their best, clean regularly with mild soap and warm water, "
              f"and lay flat to dry. Store in the included case to protect bristles and keep "
              f"your collection organized. Upgrade your makeup routine with {brand} — "
              f"professional quality brushes designed to make every look effortless.")

    paragraphs = [p1, p2, p3, p4]
    description = "\n\n".join(paragraphs)

    if len(description) > DESC_CHAR_TARGET:
        description = description[:DESC_CHAR_TARGET - 3] + "..."

    return description


# =====================================================================
# Backend search terms generator
# =====================================================================

def generate_backend_terms(rec_title: str, bullets: list[dict], description: str,
                           all_keywords: list[dict], branded_keywords: list[dict]) -> str:
    """Generate backend search terms from leftover keyword words."""
    used_words = _unique_words(rec_title)
    for b in bullets:
        used_words |= _unique_words(b["text"])
    used_words |= _unique_words(description)

    all_kw_words = set()
    for kw in all_keywords:
        all_kw_words |= _unique_words(kw["search_query"])

    branded_words = set()
    for kw in branded_keywords:
        for w in kw["search_query"].lower().split():
            if len(w) > 3 and w not in STOP_WORDS:
                branded_words.add(w)

    leftover = (all_kw_words - used_words) | (branded_words - used_words)
    leftover = {w for w in leftover if len(w) > 2}

    # Amazon backend limit: 249 bytes
    terms = sorted(leftover)
    result = " ".join(terms)
    if len(result.encode("utf-8")) > 249:
        trimmed = []
        size = 0
        for t in terms:
            t_size = len(t.encode("utf-8")) + 1
            if size + t_size <= 249:
                trimmed.append(t)
                size += t_size
            else:
                break
        result = " ".join(trimmed)

    return result


# =====================================================================
# Q&A seed generator
# =====================================================================

def generate_qa_seeds(attrs: dict, bullet_keywords: list[dict],
                      nth_keywords: list[dict]) -> list[dict]:
    """Generate Q&A seed questions based on product attributes."""
    brand = "Niré Beauty"
    variant = attrs["variant"] or ""
    pieces = attrs["piece_count"] or "15"

    all_kw_text = " ".join(k["search_query"].lower()
                           for k in bullet_keywords + nth_keywords)

    qa = []

    # Always include
    qa.append({
        "question": f"What brushes are included in the {brand} {variant} set?",
        "answer_hint": (f"The {pieces}-piece set includes brushes for foundation, powder, "
                       f"blush, contour, highlight, eyeshadow (multiple sizes), blending, "
                       f"lip, and brow application — everything needed for a complete "
                       f"makeup look."),
    })

    if attrs["is_vegan"]:
        qa.append({
            "question": f"Are {brand} brushes really vegan and cruelty-free?",
            "answer_hint": (f"Yes, all {brand} brushes use 100% synthetic bristles. "
                           f"No animal hair is used. The brushes are cruelty-free and "
                           f"perform as well or better than natural hair alternatives."),
        })

    qa.append({
        "question": f"Are these brushes good for beginners?",
        "answer_hint": (f"Absolutely. The set includes every brush you need for a full face "
                       f"of makeup, and {'the included guide helps you learn ' if attrs['includes_guide'] else ''}"
                       f"which brush to use for each step. The soft, forgiving bristles "
                       f"make blending easy even for first-time users."),
    })

    if attrs["includes_case"] or attrs["includes_holder"]:
        qa.append({
            "question": "Can I travel with these brushes?",
            "answer_hint": (f"Yes! The set comes with "
                           f"{'a compact brush case that keeps all brushes organized and protected during travel' if attrs['includes_case'] else 'a brush holder for convenient storage'}"
                           f". The case fits easily in a carry-on or makeup bag."),
        })

    qa.append({
        "question": "How do I clean these makeup brushes?",
        "answer_hint": (f"{'Use the included brush cleaner for quick spot cleaning. For deep cleaning, w' if attrs['includes_cleaner'] else 'W'}ash with mild soap or brush cleanser and warm water. "
                       f"Gently swirl bristles in your palm, rinse thoroughly, reshape, "
                       f"and lay flat to dry. Clean weekly for best results and hygiene."),
    })

    if "gift" in all_kw_text or "present" in all_kw_text or attrs["includes_gift_box"]:
        qa.append({
            "question": f"Is this a good gift for someone who loves makeup?",
            "answer_hint": (f"{'It arrives in a beautiful gift box, ready to give. ' if attrs['includes_gift_box'] else ''}"
                           f"This set is one of the most popular makeup brush gifts on Amazon "
                           f"— perfect for birthdays, Christmas, Mother's Day, or any occasion. "
                           f"Suitable for beginners and experienced makeup users alike."),
        })

    # Differentiation Q&A for multi-variant brand
    if variant and variant != "Classic":
        qa.append({
            "question": f"What's different about the {variant} set compared to other {brand} sets?",
            "answer_hint": _variant_diff_hint(variant, attrs),
        })

    qa.append({
        "question": "Do these brushes shed or lose their shape?",
        "answer_hint": (f"{brand} brushes are designed with densely packed synthetic bristles "
                       f"that resist shedding. With proper care — cleaning regularly and laying "
                       f"flat to dry — they maintain their shape and softness for years."),
    })

    return qa


def _variant_diff_hint(variant: str, attrs: dict) -> str:
    if variant == "Glitter":
        return ("The Glitter set features eye-catching glitter-finish handles that add sparkle "
                "to your collection. Same award-winning brush quality with a fun, glamorous look. "
                "Great for teens and anyone who loves a bit of sparkle.")
    elif variant == "Pink":
        return ("The Pink set features elegant pink handles for a feminine aesthetic. "
                "Same professional-grade bristles and brush selection as the Classic set, "
                "with a softer, more romantic look.")
    elif variant == "White":
        return ("The White set features sleek white handles for a clean, modern look. "
                "Same professional brush quality as the Classic set with a minimalist "
                "aesthetic that looks beautiful on any vanity.")
    elif variant == "Artistry":
        return ("The Artistry set is designed for more advanced application techniques. "
                "It includes a professional brush holder instead of a case, and the brush "
                "selection is curated for blending and artistic precision work.")
    return ""


# =====================================================================
# Main build function
# =====================================================================

def build_listing_recommendations(conn) -> int:
    """Build listing_recommendations from content_briefs + listings."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print("  [build_listing_recs] Loading data...", flush=True)

    # Get active ASINs with product names
    cur.execute("SELECT asin, product_name FROM listings WHERE status='Active'")
    listings = {r["asin"]: r["product_name"] for r in cur.fetchall()}

    # Get content briefs grouped by ASIN and tier
    cur.execute("""
        SELECT asin, search_query, content_brief_score, content_tier, tier_rank,
               search_volume, keyword_relevance, keyword_role, keyword_type, strategy
        FROM content_briefs
        ORDER BY asin, content_tier, tier_rank
    """)
    asin_briefs: dict[str, dict[str, list[dict]]] = {}
    for row in cur.fetchall():
        r = dict(row)
        asin_briefs.setdefault(r["asin"], {}).setdefault(r["content_tier"], []).append(r)

    print(f"  [build_listing_recs] {len(listings)} active ASINs, "
          f"{len(asin_briefs)} ASINs with content briefs", flush=True)

    output_rows = []

    for asin, product_name in listings.items():
        briefs = asin_briefs.get(asin, {})
        title_kws = briefs.get("title", [])
        bullet_kws = briefs.get("bullet", [])
        nth_kws = briefs.get("nice_to_have", [])
        branded_kws = briefs.get("branded", [])
        all_kws = title_kws + bullet_kws + nth_kws

        if not all_kws and not branded_kws:
            continue

        attrs = parse_product_attributes(product_name)
        rec_title, title_kw_used = generate_title(attrs, title_kws, product_name)
        bullets = generate_bullets(attrs, bullet_kws, title_kws)
        bullet_kw_used = sum(len(b["keywords_used"]) for b in bullets)
        description = generate_description(attrs, title_kws, bullet_kws, nth_kws)
        backend = generate_backend_terms(rec_title, bullets, description, all_kws, branded_kws)
        qa_seeds = generate_qa_seeds(attrs, bullet_kws, nth_kws)

        total_vol_covered = sum(k.get("search_volume") or 0 for k in all_kws)

        output_rows.append((
            asin,
            rec_title,
            len(rec_title),
            json.dumps(bullets),
            description,
            len(description),
            backend,
            json.dumps(qa_seeds),
            title_kw_used,
            len(title_kws),
            bullet_kw_used,
            len(bullet_kws),
            total_vol_covered,
            total_vol_covered,
            product_name,
            now,
        ))

    # Write
    print(f"  [build_listing_recs] Writing {len(output_rows)} recommendations...", flush=True)
    with conn.cursor() as w_cur:
        w_cur.execute("DELETE FROM listing_recommendations")
        for row in output_rows:
            w_cur.execute(
                """INSERT INTO listing_recommendations
                   (asin, rec_title, rec_title_chars, rec_bullets, rec_description,
                    rec_description_chars, rec_backend_terms, rec_qa_seeds,
                    title_keywords_used, title_keywords_total,
                    bullet_keywords_used, bullet_keywords_total,
                    total_volume_covered, total_volume_available,
                    current_title, built_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                row,
            )
    conn.commit()

    avg_title_len = (sum(r[2] for r in output_rows) / len(output_rows)) if output_rows else 0
    avg_desc_len = (sum(r[5] for r in output_rows) / len(output_rows)) if output_rows else 0
    print(f"  [build_listing_recs] Done — {len(output_rows)} rows written")
    print(f"  [build_listing_recs] Avg title: {avg_title_len:.0f} chars, avg desc: {avg_desc_len:.0f} chars")

    return len(output_rows)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from schema import init_db
    init_db()

    conn = get_conn()
    n = build_listing_recommendations(conn)
    conn.close()
    print(f"\nDone. {n} listing_recommendations rows written.")
