#!/usr/bin/env python3
"""Render the proxy graceful-lifecycle sequence diagrams to SVG + PNG.

Source of truth for these diagrams is this file. It replaces the PlantUML
blocks in ../sequences.puml with a hand-tuned renderer, because the
fireworks-tech-graph skill's "sequence" template mode has no lifeline,
activation, or alt/par/loop frame primitives -- it only places node boxes and
connects them with arrows.

What this renderer emits, in style-1 (Flat Icon) tokens:
  - participant head boxes with dashed vertical lifelines (actors get a glyph)
  - blue solid request arrows / purple dashed return arrows
  - gray "internal step" boxes for PlantUML self-messages (x -> x)
  - dashed alt / par / loop frames with corner label badges and else dividers
  - folded-corner yellow notes and inline yellow annotation chips
  - italic "..." time-gap rules
  - a shared legend

Layout is fully automatic: column gaps derive from participant, self-box, and
adjacent-message-label widths, and canvas margins expand so a self-box on an
edge lifeline can never clip.

Usage
-----
    python3 generate_sequences.py              # all 6, SVG + PNG, into ./
    python3 generate_sequences.py 2 5          # only those diagram numbers
    python3 generate_sequences.py --no-png     # skip PNG export
    python3 generate_sequences.py --out DIR    # write elsewhere

Validate afterwards with the skill's checker (expects 0 errors):
    bash ~/.claude/skills/fireworks-tech-graph/scripts/validate-svg.sh <file>.svg

Note on glyphs: cairosvg's font fallback renders U+2192 and friends as tofu, so
sanitize() rewrites arrows / comparison operators / dashes to ASCII on write.
Section marks are fine and are kept verbatim in the subtitles.
"""

import argparse
import os
import sys

from xml.sax.saxutils import escape

# ---- style tokens (style-1 flat icon) ----
BG = "#ffffff"
BOX_FILL = "#ffffff"
BOX_STROKE = "#d1d5db"
TEXT_PRIMARY = "#111827"
TEXT_SECONDARY = "#6b7280"
LIFELINE = "#cbd5e1"
REQ = "#2563eb"
RET = "#9333ea"
SELF_FILL = "#f3f4f6"
SELF_STROKE = "#9ca3af"
SELF_TEXT = "#374151"
NOTE_FILL = "#fef9c3"
NOTE_STROKE = "#eab308"
NOTE_TEXT = "#92400e"
FRAME_STROKE = "#cbd5e1"
FRAME_FILL = "none"
ACTOR_FILL = "#eff6ff"
ACTOR_STROKE = "#bfdbfe"

FONT = ("'Helvetica Neue', Helvetica, Arial, 'PingFang SC', "
        "'Microsoft YaHei', sans-serif")

MSG_FS = 11.5
SELF_FS = 12.0
PART_FS = 13.0
NOTE_FS = 11.0
CHIP_FS = 10.5

LEFT_M = 30
RIGHT_M = 30
MIN_GAP = 175


GLYPH_FIX = {"→": "->", "←": "<-", "≥": ">=", "≤": "<=",
             "⇒": "=>", "’": "'", "—": "-", "–": "-"}


def sanitize(text):
    for bad, good in GLYPH_FIX.items():
        text = text.replace(bad, good)
    return text


def tw(text, fs):
    """Rough text width."""
    w = 0.0
    for ch in text:
        if ord(ch) > 0x2000:
            w += fs * 1.0
        elif ch in "iljI.,:;'|!()[]":
            w += fs * 0.31
        elif ch in "mMwW@":
            w += fs * 0.85
        elif ch.isupper():
            w += fs * 0.66
        else:
            w += fs * 0.545
    return w


def maxw(text, fs):
    return max(tw(l, fs) for l in text.split("\n"))


class Seq:
    def __init__(self, title, participants, rows, subtitle=None):
        self.title = title
        self.subtitle = subtitle
        self.parts = participants  # list of (id, label, kind)
        self.rows = rows
        self.order = [p[0] for p in participants]

    # ---------- layout ----------
    def layout(self):
        ids = self.order
        n = len(ids)
        # participant box widths
        pw = {}
        ph = {}
        for pid, label, kind in self.parts:
            lines = label.split("\n")
            w = maxw(label, PART_FS) + (46 if kind == "actor" else 30)
            pw[pid] = max(140, round(w))
            ph[pid] = 20 + 17 * len(lines)
        self.pw, self.ph = pw, ph
        self.part_h = max(ph.values())

        # self-box widths
        selfw = {pid: 0 for pid in ids}
        for r in self.rows:
            if r[0] == "self":
                selfw[r[1]] = max(selfw[r[1]], maxw(r[2], SELF_FS) + 30)
        self.selfw = {k: round(v) for k, v in selfw.items()}

        # adjacent-pair label widths
        pair_need = {}
        for r in self.rows:
            if r[0] == "msg":
                i, j = ids.index(r[1]), ids.index(r[2])
                if abs(i - j) == 1:
                    k = min(i, j)
                    pair_need[k] = max(pair_need.get(k, 0),
                                       maxw(r[3], MSG_FS) + 26)

        # left / right margins must fit the widest self-box on the edge lifelines
        self.left_m = max(LEFT_M, self.selfw[ids[0]] / 2 - pw[ids[0]] / 2 + 12)
        self.right_m = max(RIGHT_M,
                           self.selfw[ids[-1]] / 2 - pw[ids[-1]] / 2 + 12)

        gaps = []
        for i in range(n - 1):
            need = MIN_GAP
            need = max(need, (pw[ids[i]] + pw[ids[i + 1]]) / 2 + 26)
            need = max(need, (self.selfw[ids[i]] + self.selfw[ids[i + 1]]) / 2 + 26)
            need = max(need, pair_need.get(i, 0))
            gaps.append(int(round(need / 10.0) * 10))

        xs = {}
        x = self.left_m + pw[ids[0]] / 2
        xs[ids[0]] = x
        for i in range(n - 1):
            x += gaps[i]
            xs[ids[i + 1]] = x
        self.x = xs
        self.width = int(x + pw[ids[-1]] / 2 + self.right_m)
        self.content_x0 = min(LEFT_M, self.left_m) - 6
        self.content_x1 = self.width - min(RIGHT_M, self.right_m) + 6

        # vertical pass
        head_y = 74 if not self.subtitle else 92
        self.head_y = head_y
        cursor = head_y + self.part_h + 30
        items = []
        stack = []
        last_arrow = None
        depth = 0
        for r in self.rows:
            kind = r[0]
            if kind == "msg":
                lines = r[3].split("\n")
                cursor += 6 + 13 * len(lines)
                ay = cursor
                items.append(("msg", r[1], r[2], lines, r[4], ay))
                last_arrow = (self.x[r[1]], self.x[r[2]], ay)
                cursor += 16
            elif kind == "self":
                lines = r[2].split("\n")
                h = 14 + 15 * len(lines)
                items.append(("self", r[1], lines, cursor, h))
                last_arrow = (self.x[r[1]], self.x[r[1]], cursor + h / 2)
                cursor += h + 16
            elif kind == "chip":
                lines = r[1].split("\n")
                h = 8 + 13 * len(lines)
                cx = (last_arrow[0] + last_arrow[1]) / 2 if last_arrow else self.width / 2
                items.append(("chip", lines, cx, cursor, h))
                cursor += h + 10
            elif kind == "note":
                lines = r[2].split("\n")
                h = 16 + 15 * len(lines)
                items.append(("note", r[1], lines, cursor, h))
                cursor += h + 16
            elif kind == "gap":
                items.append(("gap", r[1], cursor + 8))
                cursor += 30
            elif kind == "frame":
                stack.append((r[1], cursor + 4, depth))
                depth += 1
                cursor += 28
            elif kind == "else":
                items.append(("else", r[1], cursor + 4, depth - 1))
                cursor += 28
            elif kind == "end":
                label, top, d = stack.pop()
                depth -= 1
                bottom = cursor + 8
                items.append(("frame", label, top, bottom, d))
                cursor = bottom + 16
        self.items = items
        self.body_bottom = cursor + 10
        self.legend_y = self.body_bottom + 26
        self.height = int(self.legend_y + 54)

    # ---------- render ----------
    def svg(self):
        self.layout()
        W, H = self.width, self.height
        L = []
        A = L.append
        A(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" '
          f'width="{W}" height="{H}">')
        A('  <defs>')
        for name, color in (("req", REQ), ("ret", RET), ("self", SELF_STROKE)):
            A(f'    <marker id="ar-{name}" markerWidth="9" markerHeight="7" '
              f'refX="8.5" refY="3.5" orient="auto">')
            A(f'      <polygon points="0 0, 9 3.5, 0 7" fill="{color}"/>')
            A('    </marker>')
        A('  </defs>')
        A('  <style>')
        A(f'    text {{ font-family: {FONT}; }}')
        A('  </style>')
        A(f'  <rect width="{W}" height="{H}" fill="{BG}"/>')

        # title
        A(f'  <text x="{LEFT_M}" y="40" font-size="20" font-weight="700" '
          f'fill="{TEXT_PRIMARY}">{escape(self.title)}</text>')
        if self.subtitle:
            A(f'  <text x="{LEFT_M}" y="62" font-size="12.5" '
              f'fill="{TEXT_SECONDARY}">{escape(self.subtitle)}</text>')
        rule_y = 56 if not self.subtitle else 74
        A(f'  <line x1="{LEFT_M}" y1="{rule_y}" x2="{W-RIGHT_M}" y2="{rule_y}" '
          f'stroke="#e5e7eb" stroke-width="1"/>')

        # frames (behind everything)
        for it in self.items:
            if it[0] == "frame":
                _, label, top, bottom, d = it
                x0 = self.content_x0 + d * 12
                x1 = self.content_x1 - d * 12
                A(f'  <rect x="{x0}" y="{top}" width="{x1-x0}" '
                  f'height="{bottom-top}" rx="6" fill="{FRAME_FILL}" '
                  f'stroke="{FRAME_STROKE}" stroke-width="1" '
                  f'stroke-dasharray="6,4"/>')

        # lifelines
        head_bottom = self.head_y + self.part_h
        for pid in self.order:
            x = self.x[pid]
            A(f'  <line x1="{x}" y1="{head_bottom}" x2="{x}" '
              f'y2="{self.body_bottom}" stroke="{LIFELINE}" '
              f'stroke-width="1.4" stroke-dasharray="5,4"/>')

        # frame labels + else dividers (above frame rect, below arrows)
        for it in self.items:
            if it[0] == "frame":
                _, label, top, bottom, d = it
                x0 = self.content_x0 + d * 12
                bw = tw(label, 11) + 18
                A(f'  <rect x="{x0}" y="{top}" width="{bw}" height="19" '
                  f'rx="4" fill="#e2e8f0" stroke="{FRAME_STROKE}" '
                  f'stroke-width="1"/>')
                A(f'  <text x="{x0+bw/2}" y="{top+13.5}" font-size="11" '
                  f'font-weight="600" text-anchor="middle" '
                  f'fill="#475569">{escape(label)}</text>')
            elif it[0] == "else":
                _, label, y, d = it
                x0 = self.content_x0 + d * 12
                x1 = self.content_x1 - d * 12
                A(f'  <line x1="{x0}" y1="{y}" x2="{x1}" y2="{y}" '
                  f'stroke="{FRAME_STROKE}" stroke-width="1" '
                  f'stroke-dasharray="6,4"/>')
                bw = tw(label, 11) + 18
                A(f'  <rect x="{x0+10}" y="{y+2}" width="{bw}" height="18" '
                  f'rx="4" fill="#ffffff" stroke="{FRAME_STROKE}" '
                  f'stroke-width="1"/>')
                A(f'  <text x="{x0+10+bw/2}" y="{y+14.5}" font-size="11" '
                  f'font-weight="600" text-anchor="middle" '
                  f'fill="#475569">{escape(label)}</text>')

        # body items
        for it in self.items:
            k = it[0]
            if k == "msg":
                _, src, dst, lines, mkind, ay = it
                x1, x2 = self.x[src], self.x[dst]
                color = REQ if mkind == "req" else RET
                marker = "ar-req" if mkind == "req" else "ar-ret"
                dash = '' if mkind == "req" else ' stroke-dasharray="6,4"'
                sx = x1 + (4 if x2 > x1 else -4)
                A(f'  <line x1="{sx}" y1="{ay}" x2="{x2}" y2="{ay}" '
                  f'stroke="{color}" stroke-width="1.6"{dash} '
                  f'marker-end="url(#{marker})"/>')
                mid = (x1 + x2) / 2
                lw = max(tw(l, MSG_FS) for l in lines)
                top = ay - 7 - 13 * len(lines)
                A(f'  <rect x="{mid-lw/2-5}" y="{top-1}" width="{lw+10}" '
                  f'height="{13*len(lines)+4}" fill="{BG}"/>')
                for i, ln in enumerate(lines):
                    A(f'  <text x="{mid}" y="{top+11+13*i}" font-size="{MSG_FS}" '
                      f'text-anchor="middle" fill="#334155">{escape(ln)}</text>')
            elif k == "self":
                _, pid, lines, y, h = it
                bw = max(maxw("\n".join(lines), SELF_FS) + 30, 120)
                x = self.x[pid] - bw / 2
                A(f'  <rect x="{x}" y="{y}" width="{bw}" height="{h}" rx="6" '
                  f'fill="{SELF_FILL}" stroke="{SELF_STROKE}" '
                  f'stroke-width="1.2"/>')
                A(f'  <rect x="{x}" y="{y}" width="3.5" height="{h}" '
                  f'rx="1.5" fill="{SELF_STROKE}"/>')
                for i, ln in enumerate(lines):
                    A(f'  <text x="{self.x[pid]+2}" y="{y+19+15*i}" '
                      f'font-size="{SELF_FS}" text-anchor="middle" '
                      f'fill="{SELF_TEXT}">{escape(ln)}</text>')
            elif k == "chip":
                _, lines, cx, y, h = it
                cw = max(tw(l, CHIP_FS) for l in lines) + 20
                x = min(max(cx - cw / 2, self.content_x0 + 6),
                        self.content_x1 - cw - 6)
                A(f'  <rect x="{x}" y="{y}" width="{cw}" height="{h}" rx="4" '
                  f'fill="{NOTE_FILL}" stroke="#fde68a" stroke-width="1"/>')
                for i, ln in enumerate(lines):
                    A(f'  <text x="{x+10}" y="{y+13+13*i}" '
                      f'font-size="{CHIP_FS}" fill="{NOTE_TEXT}">'
                      f'{escape(ln)}</text>')
            elif k == "note":
                _, over, lines, y, h = it
                nw = max(max(tw(l, NOTE_FS) for l in lines) + 34, 200)
                xs = [self.x[p] for p in over]
                cx = (min(xs) + max(xs)) / 2
                x = min(max(cx - nw / 2, self.content_x0 + 4),
                        self.content_x1 - nw - 4)
                fold = 12
                A(f'  <path d="M {x},{y} H {x+nw-fold} L {x+nw},{y+fold} '
                  f'V {y+h} H {x} Z" fill="{NOTE_FILL}" stroke="{NOTE_STROKE}" '
                  f'stroke-width="1.2"/>')
                A(f'  <path d="M {x+nw-fold},{y} V {y+fold} H {x+nw}" '
                  f'fill="none" stroke="{NOTE_STROKE}" stroke-width="1.2"/>')
                for i, ln in enumerate(lines):
                    A(f'  <text x="{x+14}" y="{y+21+15*i}" font-size="{NOTE_FS}" '
                      f'fill="{NOTE_TEXT}">{escape(ln)}</text>')
            elif k == "gap":
                _, text, y = it
                lw = tw(text, 11) + 24
                cx = (self.content_x0 + self.content_x1) / 2
                A(f'  <line x1="{self.content_x0+10}" y1="{y}" '
                  f'x2="{cx-lw/2}" y2="{y}" stroke="#e5e7eb" '
                  f'stroke-width="1" stroke-dasharray="4,4"/>')
                A(f'  <line x1="{cx+lw/2}" y1="{y}" '
                  f'x2="{self.content_x1-10}" y2="{y}" stroke="#e5e7eb" '
                  f'stroke-width="1" stroke-dasharray="4,4"/>')
                A(f'  <text x="{cx}" y="{y+4}" font-size="11" '
                  f'font-style="italic" text-anchor="middle" '
                  f'fill="{TEXT_SECONDARY}">{escape(text)}</text>')

        # participant heads (drawn last so lifelines tuck under)
        for pid, label, kind in self.parts:
            lines = label.split("\n")
            w, h = self.pw[pid], self.part_h
            x = self.x[pid] - w / 2
            fill = ACTOR_FILL if kind == "actor" else BOX_FILL
            stroke = ACTOR_STROKE if kind == "actor" else BOX_STROKE
            A(f'  <rect x="{x}" y="{self.head_y}" width="{w}" height="{h}" '
              f'rx="8" fill="{fill}" stroke="{stroke}" stroke-width="1.5"/>')
            tx = self.x[pid]
            if kind == "actor":
                gx, gy = x + 17, self.head_y + h / 2
                A(f'  <circle cx="{gx}" cy="{gy-7}" r="4" fill="none" '
                  f'stroke="#3b82f6" stroke-width="1.4"/>')
                A(f'  <path d="M {gx-5},{gy+7} v -4 a 5,5 0 0 1 10,0 v 4" '
                  f'fill="none" stroke="#3b82f6" stroke-width="1.4"/>')
                tx = self.x[pid] + 10
            base = self.head_y + h / 2 - (len(lines) - 1) * 8.5 + 5
            for i, ln in enumerate(lines):
                fs = PART_FS if i == 0 else 11.5
                fw = "600" if i == 0 else "400"
                fc = TEXT_PRIMARY if i == 0 else TEXT_SECONDARY
                A(f'  <text x="{tx}" y="{base+17*i}" font-size="{fs}" '
                  f'font-weight="{fw}" text-anchor="middle" fill="{fc}">'
                  f'{escape(ln)}</text>')

        # legend
        ly = self.legend_y
        A(f'  <line x1="{LEFT_M}" y1="{ly-14}" x2="{W-RIGHT_M}" y2="{ly-14}" '
          f'stroke="#e5e7eb" stroke-width="1"/>')
        lx = LEFT_M
        entries = [("line", REQ, "ar-req", None, "request / forward call"),
                   ("line", RET, "ar-ret", "6,4", "response / return"),
                   ("box", SELF_FILL, SELF_STROKE, None, "internal step"),
                   ("box", NOTE_FILL, NOTE_STROKE, None, "note / constraint")]
        for etype, c1, c2, dash, text in entries:
            if etype == "line":
                d = f' stroke-dasharray="{dash}"' if dash else ''
                A(f'  <line x1="{lx}" y1="{ly+8}" x2="{lx+28}" y2="{ly+8}" '
                  f'stroke="{c1}" stroke-width="1.6"{d} '
                  f'marker-end="url(#{c2})"/>')
            else:
                A(f'  <rect x="{lx}" y="{ly+2}" width="26" height="13" rx="3" '
                  f'fill="{c1}" stroke="{c2}" stroke-width="1.1"/>')
            A(f'  <text x="{lx+36}" y="{ly+12}" font-size="11" '
              f'fill="{TEXT_SECONDARY}">{escape(text)}</text>')
            lx += 36 + tw(text, 11) + 30
        A('</svg>')
        return "\n".join(L)


def write(spec, path):
    svg = sanitize(spec.svg())
    with open(path, "w") as f:
        f.write(svg)
    return spec.width, spec.height


# --------------------------------------------------------------------------
# Diagram specs -- one entry per @startuml block in ../sequences.puml
# --------------------------------------------------------------------------

DIAGRAMS = [
    ("seq-1-prestop-drain", Seq(
        title="PreStop drain full chain",
        subtitle="spec §3.6 + §3.2  ·  kubelet PreStop hook drives the coordinator through QUIESCING → DRAINED",
        participants=[
            ("kubelet", "kubelet", "actor"),
            ("cli", "mqproxyctl\n(PreStop)", "box"),
            ("admin", "ProxyAdminServer\n8082, loopback", "box"),
            ("coord", "ProxyLifecycle\nCoordinator", "box"),
            ("lb", "NLB /\nEndpointSlice", "box"),
        ],
        rows=[
            ("msg", "kubelet", "cli", "exec PreStop\n./mqproxyctl drain --wait --timeout 480s", "req"),
            ("msg", "cli", "admin", "POST /drain", "req"),
            ("msg", "admin", "coord", "beginDrain(PRESTOP)", "req"),
            ("msg", "coord", "admin", "DrainRun (drainId, cutoffs)", "ret"),
            ("msg", "admin", "cli", "202 { drainId, cutoffs }", "ret"),
            ("frame", "par"),
            ("self", "coord", "T0 → QUIESCING\nreadiness fails"),
            ("msg", "coord", "lb", "(observed) EndpointSlice / target\nderegistration begins", "req"),
            ("else", "else"),
            ("msg", "cli", "admin", "poll GET /drain/{drainId}", "req"),
            ("msg", "admin", "cli", "200 { phase: QUIESCING, ... }", "ret"),
            ("end",),
            ("self", "coord", "lbCutoff → MIGRATING\n(reject / count late transport)"),
            ("self", "coord", "migrationCutoff → DRAINING\n(close admission, freeze intake)"),
            ("self", "coord", "wait accepted_inflight_zero\n(dual-terminal accounting)"),
            ("frame", "alt  reached before hardDeadline"),
            ("self", "coord", "DRAINED"),
            ("else", "else  hardDeadline exceeded"),
            ("self", "coord", "FORCE_DRAINING (forced close)"),
            ("end",),
            ("msg", "cli", "admin", "poll GET /drain/{drainId}", "req"),
            ("msg", "admin", "cli", "200 { phase: DRAINED | FORCE_DRAINING }", "ret"),
            ("msg", "cli", "kubelet", "exit 0 (DRAINED)  /  non-zero (FORCE_DRAINING)", "ret"),
            ("note", ["cli", "lb"],
             "NLB 450s deregistration / connection-drain runs in parallel with the Proxy clock\n"
             "from the moment readiness / target is removed — never serialized after podGrace."),
        ],
    )),
    ("seq-2-grpc-double-goaway", Seq(
        title="gRPC double-GOAWAY drain",
        subtitle="spec §9.2 + §3.4  ·  two GOAWAY frames fence the highest stream id the server actually accepted",
        participants=[
            ("coord", "ProxyLifecycle\nCoordinator", "box"),
            ("adapter", "GrpcDrainAdapter", "box"),
            ("gsrv", "GrpcServer", "box"),
            ("server", "io.grpc.Server\ngrpc-java built-in", "box"),
            ("reg", "GrpcActiveCall\nRegistry", "box"),
            ("client", "Client\n5.0.7 / 5.2.1 / 5.3.2+", "box"),
        ],
        rows=[
            ("msg", "coord", "adapter", "enter DRAINING\nstopAcceptingNewRpcs()", "req"),
            ("msg", "adapter", "gsrv", "initiateServerDrain()", "req"),
            ("msg", "gsrv", "server", "shutdown()\n(once-only CAS, non-blocking)", "req"),
            ("msg", "server", "client", "GOAWAY(lastStreamId=MAX, NO_ERROR)", "req"),
            ("self", "client", "stop creating new streams\non this transport"),
            ("msg", "server", "client", "PING(0x97ACEF001)", "req"),
            ("msg", "client", "server", "PING_ACK", "ret"),
            ("chip", "or 10s fallback if no ACK"),
            ("msg", "server", "client", "GOAWAY(lastStreamCreated, NO_ERROR)", "req"),
            ("chip", "freezes the highest stream id the server actually accepted"),
            ("self", "adapter", "wait no_new_work_reached\n(this protocol's evidence)"),
            ("self", "coord", "last protocol reaches no_new_work_reached\n-> SendDrainGate.closeAdmission()"),
            ("self", "adapter", "wait grpcOpenSendRpcs == 0"),
            ("msg", "adapter", "reg", "closeAll(GrpcDrainStatusPolicy)", "req"),
            ("msg", "reg", "client", "Telemetry stream -> Status.OK (normal completion)", "req"),
            ("msg", "reg", "client", "ReceiveMessage / other streaming\n-> UNAVAILABLE \"[PROXY_DRAINING] reconnect\"", "req"),
            ("self", "client", "Telemetry: 1s observer renewal,\nresend Settings (no error log)"),
            ("self", "client", "ReceiveMessage: reconnect elsewhere\nvia stable NLB address"),
            ("msg", "adapter", "reg", "wait grpcOpenDrainableCalls == 0", "req"),
            ("msg", "adapter", "gsrv", "awaitServerTermination(effectiveDeadline)", "req"),
            ("msg", "gsrv", "server", "awaitTermination(remaining)", "req"),
            ("frame", "alt  terminated in time"),
            ("msg", "gsrv", "adapter", "true", "ret"),
            ("msg", "adapter", "coord", "normal DrainResult", "ret"),
            ("else", "else  timeout / interrupted"),
            ("msg", "gsrv", "server", "shutdownNow() (once-only)", "req"),
            ("msg", "gsrv", "adapter", "forced=true, cause", "ret"),
            ("msg", "adapter", "coord", "forced DrainResult", "ret"),
            ("end",),
        ],
    )),
    ("seq-3-remoting-lease-migration", Seq(
        title="Remoting lease-driven GO_AWAY migration",
        subtitle="spec §3.3 + §9.4  ·  an expired channel lease buffers exactly one client-side migration",
        participants=[
            ("client", "Client\nRemoting >V5_3_1", "box"),
            ("nlb", "Stable NLB\naddress", "box"),
            ("server", "NettyRemoting\nServer", "box"),
            ("listener", "RemotingSend\nLifecycleListener", "box"),
        ],
        rows=[
            ("msg", "client", "nlb", "connect", "req"),
            ("msg", "nlb", "server", "new Channel", "req"),
            ("self", "server", "onChannelActive(): assign lease 270-330s\n(jitter, monotonic deadline)"),
            ("msg", "client", "server", "SEND_MESSAGE (acknowledged, non-oneway)", "req"),
            ("msg", "server", "listener", "beforeEnqueue(channel, request)", "req"),
            ("msg", "listener", "server", "accepted (lease not expired, gate OPEN)", "ret"),
            ("msg", "server", "client", "normal response", "ret"),
            ("gap", "lease deadline reached, channel otherwise idle or next request arrives"),
            ("msg", "client", "server", "SEND_MESSAGE", "req"),
            ("msg", "server", "listener", "beforeEnqueue(channel, request)", "req"),
            ("msg", "listener", "server", "GO_AWAY (lease expired, clientVersion > V5_3_1,\nrequest NOT dispatched to Broker)", "ret"),
            ("msg", "server", "client", "GO_AWAY response", "ret"),
            ("self", "client", "transport-layer reconnect\n(one replay only)"),
            ("msg", "client", "nlb", "reconnect", "req"),
            ("msg", "nlb", "server", "may land on same or different Pod", "req"),
            ("msg", "client", "server", "replay SEND_MESSAGE (attempt 2)", "req"),
            ("msg", "server", "listener", "beforeEnqueue (new channel, fresh lease)", "req"),
            ("msg", "listener", "server", "accepted", "ret"),
            ("msg", "server", "client", "normal response", "ret"),
            ("note", ["client", "server"],
             "A second GO_AWAY on the replayed attempt WOULD fail the client -\n"
             "this path buffers exactly one migration, never correctness."),
            ("note", ["nlb", "listener"],
             "Idle channel with zero inflight and expired lease is closed directly.\n"
             "Legacy clients (<= V5_2_0) get SERVE_UNTIL_CUTOFF: no GO_AWAY\n"
             "capability, served until forced close."),
        ],
    )),
    ("seq-4-sigterm-preempts-prestop", Seq(
        title="Direct SIGTERM preempts PreStop",
        subtitle="spec §8.3  ·  TERM fast-forwards the state machine instead of waiting for lbCutoff / migrationCutoff",
        participants=[
            ("term", "SIGTERM\nJVM shutdown hook", "box"),
            ("runtime", "ProxyRuntime", "box"),
            ("coord", "ProxyLifecycle\nCoordinator", "box"),
            ("adapters", "Grpc / Remoting\nDrainAdapter", "box"),
            ("gate", "SendDrainGate", "box"),
        ],
        rows=[
            ("msg", "term", "runtime", "shutdown(SIGTERM_FALLBACK)", "req"),
            ("msg", "runtime", "coord", "beginDrain(SIGTERM_FALLBACK)\n(join existing DrainRun if present)", "req"),
            ("msg", "coord", "runtime", "DrainRun (session, drainFuture)", "ret"),
            ("self", "runtime", "CAS AtomicReference<StopRun>\n(first winner constructs StopRun)"),
            ("self", "runtime", "stopDeadline = min(now+jvmTimeout,\n(preStopDeadline+jvmTimeout)-now)"),
            ("msg", "runtime", "coord", "escalateForStop(stopDeadline)", "req"),
            ("self", "coord", "cancel pending lb / lease scheduled timers\n(phase generation)"),
            ("self", "coord", "fast-forward state machine\n(READY/QUIESCING/MIGRATING -> DRAINING,\nor STARTING -> FORCE_DRAINING)"),
            ("msg", "coord", "gate", "closeAdmission() immediately\n(no 60s / 420s wait)", "req"),
            ("msg", "coord", "adapters", "freeze both protocols\n(same fan-out as normal drain)", "req"),
            ("msg", "coord", "runtime", "escalateForStop future completes (freeze issued)", "ret"),
            ("self", "runtime", "await accepted_inflight_zero\nwithin min(drainDeadline, stopDeadline)"),
            ("frame", "alt  drained before stopDeadline"),
            ("self", "runtime", "StopRun succeeds normally"),
            ("else", "else  stopDeadline exceeded"),
            ("msg", "runtime", "adapters", "force(stopDeadline)", "req"),
            ("msg", "adapters", "runtime", "forced DrainResult", "ret"),
            ("end",),
            ("self", "runtime", "STOPPING -> shut down Remoting -> gRPC Server\n-> TLS listener -> EventLoopGroups -> executors -> STOPPED"),
            ("msg", "runtime", "term", "stopFuture completes, hook join() returns", "ret"),
            ("self", "term", "JVM exits"),
            ("note", ["runtime", "adapters"],
             "If TERM bypasses or interrupts an in-progress PreStop drain, it must NOT wait for\n"
             "the 60s lbCutoff or 420s migrationCutoff - it closes admission and freezes both\n"
             "protocols on the very first scheduler tick."),
        ],
    )),
    ("seq-5-runtime-startup-order", Seq(
        title="Runtime construction and startup order",
        subtitle="spec §8.2  ·  ConstructionScope owns every leaf so a mid-build failure rolls back in reverse order",
        participants=[
            ("main", "ProxyStartup.main", "box"),
            ("factory", "ProxyRuntimeFactory", "box"),
            ("scope", "ConstructionScope", "box"),
            ("runtime", "ProxyRuntime", "box"),
        ],
        rows=[
            ("self", "main", "parseCommandLineArgument(args)"),
            ("self", "main", "initConfiguration(argument)\n-> validateGracefulLifecycle()"),
            ("msg", "main", "factory", "create()", "req"),
            ("msg", "factory", "scope", "new ConstructionScope()", "req"),
            ("msg", "factory", "scope", "own(\"server-executor\", newServerExecutor())", "req"),
            ("msg", "factory", "scope", "own(\"messaging-processor\", buildMessagingProcessorSafely())", "req"),
            ("msg", "factory", "scope", "own(\"grpc-server-bundle\", buildGrpcServerSafely(executor))", "req"),
            ("msg", "factory", "scope", "own(\"remoting-server\", buildRemotingSafely(processor))", "req"),
            ("msg", "factory", "scope", "own(\"admin-server\", buildAdmin())", "req"),
            ("frame", "alt  any leaf construction throws"),
            ("self", "scope", "rollback already-owned leaves\nin reverse order"),
            ("msg", "scope", "factory", "rethrow with rollback close failures as suppressed", "ret"),
            ("msg", "factory", "main", "exception (runtime still null)", "ret"),
            ("self", "main", "forceStop(startupFailure).join()\nSystem.exit(1)"),
            ("else", "else  all leaves succeed"),
            ("msg", "factory", "runtime", "new ProxyRuntime(leaves...)", "req"),
            ("msg", "factory", "scope", "commit()\n(ownership transferred, scope won't close on exit)", "req"),
            ("msg", "factory", "main", "runtime instance", "ret"),
            ("end",),
            ("self", "main", "Runtime.getRuntime().addShutdownHook(\n() -> runtime.shutdown(SIGTERM).join())"),
            ("msg", "main", "runtime", "runtime.start()", "req"),
            ("self", "runtime", "admin bind -> metrics -> TLS -> MessagingProcessor\n-> gRPC application -> gRPC listener -> Remoting listener\n-> listener contributors complete -> route warmup -> READY"),
            ("note", ["scope", "runtime"],
             "Local feature-off mode keeps the legacy BrokerController-wrapper-first\n"
             "ordering instead of this Cluster sequence."),
        ],
    )),
    ("seq-6-rollout-supervisor", Seq(
        title="Rollout supervisor - single-Pod serialized drain",
        subtitle="spec §4.4 + §12.7  ·  the supervisor gates on a persisted DrainResult, never on a metrics scrape alone",
        participants=[
            ("op", "Operator", "actor"),
            ("sup", "proxy_rollout.py\nsupervisor", "box"),
            ("k8s", "kubectl / helm", "box"),
            ("old", "Pod N\n(old)", "box"),
            ("new", "Pod N+1\n(new)", "box"),
            ("store", "Drain result store\nPod Condition / CRD", "box"),
        ],
        rows=[
            ("msg", "op", "sup", "run rollout(target chart/values)", "req"),
            ("msg", "sup", "k8s", "helm template/lint + schema check", "req"),
            ("msg", "sup", "k8s", "kubectl get hpa\n(assert no live HPA on target workload)", "req"),
            ("frame", "alt  live HPA found"),
            ("msg", "sup", "op", "fail fast, abort\n(no freeze / take-over attempted)", "ret"),
            ("end",),
            ("msg", "sup", "k8s", "verify all Pods READY, no in-progress drain,\nprovider health-port evidence OK", "req"),
            ("msg", "sup", "k8s", "helm upgrade --wait (no --atomic)", "req"),
            ("msg", "k8s", "new", "schedule + start\n(surge, maxUnavailable=0)", "req"),
            ("msg", "k8s", "old", "send SIGTERM after new Pod\nreaches Ready + minReadySeconds", "req"),
            ("frame", "loop  for each old Pod being replaced"),
            ("msg", "sup", "k8s", "assert at most one old Pod Terminating", "req"),
            ("self", "old", "drain via PreStop\n(see PreStop sequence diagram)"),
            ("msg", "old", "store", "write persisted DrainResult\n(structured termination message / Pod Condition)", "req"),
            ("msg", "sup", "store", "read confirmed drain result\n(NOT the last metrics scrape alone)", "req"),
            ("frame", "alt  store read/write fails, stale, or query timeout"),
            ("self", "sup", "fail-closed: pause rollout"),
            ("else", "else  forced == true AND accepted_sends > 0"),
            ("msg", "sup", "k8s", "kubectl rollout pause (possible real send loss)", "req"),
            ("else", "else  forced == true AND accepted_sends == 0"),
            ("self", "sup", "warn + record, continue rollout"),
            ("else", "else  DRAINED cleanly"),
            ("self", "sup", "proceed to next Pod"),
            ("end",),
            ("end",),
            ("msg", "sup", "k8s", "post-rollout check: all Pods READY, NLB targets healthy,\nno forced / late-connection increase", "req"),
            ("msg", "sup", "op", "rollout complete or paused with reason", "ret"),
        ],
    )),
]


def render(slug, spec, out_dir, png=True, scale=1.6):
    svg_path = os.path.join(out_dir, slug + ".svg")
    width, height = write(spec, svg_path)
    result = [f"{slug}.svg  {width}x{height}"]
    if png:
        try:
            import cairosvg
        except ImportError:
            result.append("(cairosvg not installed, PNG skipped)")
        else:
            png_path = os.path.join(out_dir, slug + ".png")
            cairosvg.svg2png(url=svg_path, write_to=png_path, scale=scale)
            result.append(f"+ {slug}.png @{scale}x")
    return "  ".join(result)


def main():
    parser = argparse.ArgumentParser(
        description="Render proxy graceful-lifecycle sequence diagrams.")
    parser.add_argument("numbers", nargs="*", type=int,
                        help="diagram numbers to render (default: all)")
    parser.add_argument("--out", default=os.path.dirname(
        os.path.abspath(__file__)), help="output directory")
    parser.add_argument("--no-png", action="store_true",
                        help="emit SVG only")
    parser.add_argument("--scale", type=float, default=1.6,
                        help="PNG scale factor (default 1.6)")
    args = parser.parse_args()

    selected = DIAGRAMS
    if args.numbers:
        bad = [n for n in args.numbers if not 1 <= n <= len(DIAGRAMS)]
        if bad:
            parser.error(f"no such diagram(s): {bad} (valid: 1-{len(DIAGRAMS)})")
        selected = [DIAGRAMS[n - 1] for n in args.numbers]

    os.makedirs(args.out, exist_ok=True)
    for slug, spec in selected:
        print(render(slug, spec, args.out, png=not args.no_png,
                     scale=args.scale))


if __name__ == "__main__":
    main()
