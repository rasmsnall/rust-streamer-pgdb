"""Generate Word (.docx) documents from the Markdown sources in ``docs/``.

The Markdown files are the single source of truth. Word output is generated, never
hand-edited, so the two cannot drift. Regenerate after any documentation change::

    python tools/md2docx.py

Formatting follows the South Korean university thesis conventions adopted for this
project: a serif body face, justified text with generous line spacing, chapter headings
in Roman numerals, table captions placed above their table, and figure captions placed
below their figure. Caption placement is a property of the Markdown source; this script
only styles what it finds.

Requires ``python-docx``. No other dependency.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

from docx import Document
from docx.enum.section import WD_SECTION
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Cm, Pt, RGBColor

# Body face for Latin text, and the East Asian face Word falls back to for Hangul.
BODY_FONT = "Times New Roman"
EASTASIA_FONT = "Batang"
MONO_FONT = "Consolas"

BODY_SIZE = Pt(11)
CODE_SIZE = Pt(9)
LINE_SPACING = 1.6

# Inline spans, matched in priority order. Code is matched first so that markup inside
# backticks is preserved literally.
INLINE = re.compile(
    r"(?P<code>`[^`]+`)"
    r"|(?P<link>\[[^\]]+\]\([^)]+\))"
    r"|(?P<bold>\*\*[^*]+\*\*)"
    r"|(?P<italic>\*[^*]+\*)"
)


def _set_fonts(run, mono: bool = False) -> None:
    """Apply the document face to ``run``, including the East Asian fallback.

    python-docx does not expose ``w:eastAsia``, so it is set on the underlying XML.
    Without it Word substitutes an arbitrary face for any Hangul in the source.
    """
    name = MONO_FONT if mono else BODY_FONT
    run.font.name = name
    run.font.size = CODE_SIZE if mono else BODY_SIZE
    rpr = run._element.get_or_add_rPr()
    rfonts = rpr.find(qn("w:rFonts"))
    if rfonts is None:
        rfonts = OxmlElement("w:rFonts")
        rpr.append(rfonts)
    rfonts.set(qn("w:ascii"), name)
    rfonts.set(qn("w:hAnsi"), name)
    rfonts.set(qn("w:eastAsia"), name if mono else EASTASIA_FONT)


def _add_inline(paragraph, text: str) -> None:
    """Append ``text`` to ``paragraph``, converting Markdown inline spans to runs.

    Handles code spans, links, bold and italic. Link targets are rendered as visible
    text in parentheses, because a printed thesis-style document must remain usable on
    paper where a hyperlink is invisible.
    """
    pos = 0
    for m in INLINE.finditer(text):
        if m.start() > pos:
            run = paragraph.add_run(text[pos : m.start()])
            _set_fonts(run)
        kind = m.lastgroup
        raw = m.group()
        if kind == "code":
            run = paragraph.add_run(raw[1:-1])
            _set_fonts(run, mono=True)
        elif kind == "link":
            label, target = re.match(r"\[([^\]]+)\]\(([^)]+)\)", raw).groups()
            run = paragraph.add_run(label)
            _set_fonts(run)
            run.font.color.rgb = RGBColor(0x00, 0x33, 0x99)
            tail = paragraph.add_run(f" ({target})")
            _set_fonts(tail)
            tail.font.size = Pt(9)
        elif kind == "bold":
            run = paragraph.add_run(raw[2:-2])
            _set_fonts(run)
            run.bold = True
        else:
            run = paragraph.add_run(raw[1:-1])
            _set_fonts(run)
            run.italic = True
        pos = m.end()
    if pos < len(text):
        run = paragraph.add_run(text[pos:])
        _set_fonts(run)


def _body_paragraph(doc, text: str, style: str | None = None):
    """Add a justified body paragraph carrying the document's spacing."""
    p = doc.add_paragraph(style=style)
    p.paragraph_format.line_spacing = LINE_SPACING
    p.paragraph_format.space_after = Pt(6)
    if style is None:
        p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
    _add_inline(p, text)
    return p


def _code_block(doc, lines: list[str]) -> None:
    """Render a fenced code block as a single monospaced, unjustified paragraph."""
    p = doc.add_paragraph()
    p.paragraph_format.left_indent = Cm(0.8)
    p.paragraph_format.line_spacing = 1.0
    p.paragraph_format.space_before = Pt(6)
    p.paragraph_format.space_after = Pt(6)
    run = p.add_run("\n".join(lines))
    _set_fonts(run, mono=True)


def _split_row(line: str) -> list[str]:
    """Split one Markdown table row into trimmed cell texts."""
    return [c.strip() for c in line.strip().strip("|").split("|")]


def _table(doc, rows: list[str]) -> None:
    """Render a Markdown pipe table, treating the first row as the header."""
    header = _split_row(rows[0])
    body = [_split_row(r) for r in rows[2:]]
    table = doc.add_table(rows=1, cols=len(header))
    table.style = "Table Grid"
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    for cell, text in zip(table.rows[0].cells, header):
        cell.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
        _add_inline(cell.paragraphs[0], text)
        for run in cell.paragraphs[0].runs:
            run.bold = True
    for record in body:
        cells = table.add_row().cells
        for cell, text in zip(cells, record):
            _add_inline(cell.paragraphs[0], text)
    doc.add_paragraph()


def _page_numbers(doc) -> None:
    """Add a centred page-number field to the footer of every section."""
    for section in doc.sections:
        p = section.footer.paragraphs[0]
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        fld = OxmlElement("w:fldSimple")
        fld.set(qn("w:instr"), "PAGE")
        p._p.append(fld)


def _configure(doc) -> None:
    """Apply page geometry and the default style for the whole document."""
    for section in doc.sections:
        section.top_margin = Cm(3.0)
        section.bottom_margin = Cm(3.0)
        section.left_margin = Cm(3.0)
        section.right_margin = Cm(2.0)
    style = doc.styles["Normal"]
    style.font.name = BODY_FONT
    style.font.size = BODY_SIZE
    style.element.rPr.rFonts.set(qn("w:eastAsia"), EASTASIA_FONT)


def convert(md_path: Path, docx_path: Path) -> None:
    """Convert one Markdown file to Word.

    Args:
        md_path: Markdown source to read.
        docx_path: Word file to write, overwritten if present.

    Raises:
        FileNotFoundError: if ``md_path`` does not exist.

    Does not panic on unsupported Markdown; unrecognised constructs fall through to
    plain body text rather than being dropped.
    """
    lines = md_path.read_text(encoding="utf-8").split("\n")
    doc = Document()
    _configure(doc)

    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        if stripped.startswith("```"):
            block: list[str] = []
            i += 1
            while i < len(lines) and not lines[i].strip().startswith("```"):
                block.append(lines[i])
                i += 1
            _code_block(doc, block)

        elif stripped.startswith("|") and i + 1 < len(lines) and set(
            lines[i + 1].strip()
        ) <= set("|-: "):
            rows = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                rows.append(lines[i])
                i += 1
            _table(doc, rows)
            continue

        elif stripped.startswith("#"):
            level = len(stripped) - len(stripped.lstrip("#"))
            text = stripped[level:].strip()
            if level == 1:
                doc.add_heading("", 0)
                p = doc.paragraphs[-1]
                p.alignment = WD_ALIGN_PARAGRAPH.CENTER
                _add_inline(p, text)
                for run in p.runs:
                    run.font.size = Pt(20)
                    run.bold = True
            else:
                p = doc.add_heading("", min(level - 1, 4))
                _add_inline(p, text)
                for run in p.runs:
                    run.font.color.rgb = RGBColor(0, 0, 0)
                    run.bold = True
                    run.font.size = Pt(15 - level)

        elif stripped == "---":
            doc.add_paragraph()

        elif re.match(r"^\s*[-*]\s+", line):
            _body_paragraph(doc, re.sub(r"^\s*[-*]\s+", "", line), style="List Bullet")

        elif re.match(r"^\s*\d+\.\s+", line):
            _body_paragraph(doc, re.sub(r"^\s*\d+\.\s+", "", line), style="List Number")

        elif stripped:
            # Re-join wrapped source lines into one paragraph.
            para = [stripped]
            i += 1
            while i < len(lines) and lines[i].strip() and not re.match(
                r"^\s*(#|[-*]\s|\d+\.\s|\||```|---)", lines[i]
            ):
                para.append(lines[i].strip())
                i += 1
            _body_paragraph(doc, " ".join(para))
            continue

        i += 1

    _page_numbers(doc)
    doc.save(str(docx_path))


def main() -> int:
    """Convert every ``docs/*.md`` file to a sibling ``.docx``."""
    docs = Path(__file__).resolve().parent.parent / "docs"
    sources = sorted(docs.glob("*.md"))
    if not sources:
        print(f"no markdown found in {docs}", file=sys.stderr)
        return 1
    for md in sources:
        out = md.with_suffix(".docx")
        convert(md, out)
        print(f"{md.name} -> {out.name} ({out.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
