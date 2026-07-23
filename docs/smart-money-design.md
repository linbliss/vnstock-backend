# Smart Money Analysis Platform — Review & Design Proposal

> Tài liệu sống. Ghi lại đánh giá kiến trúc Order Flow Analyzer hiện tại và lộ trình
> nâng cấp thành nền tảng phân tích dòng tiền thông minh cho TTCK Việt Nam.
> Cập nhật trạng thái ở mục [Tiến độ](#tiến-độ).

## Mục tiêu

Không phải thêm chỉ báo. Mục tiêu là hệ có khả năng: **phân tích → hiểu ngữ cảnh →
diễn giải hành vi dòng tiền → chấm điểm → giải thích kết luận → backtest → benchmark**,
dễ mở rộng và bảo trì.

Hai case chuẩn để nghiệm thu thiết kế:

- **HDB (Accumulation)**: sideway, MA50≈MA100, volume giảm, khối ngoại mua ròng, buy
  aggressive > sell aggressive, có hấp thụ, chưa breakout → kỳ vọng *Accumulation, chưa
  vào Markup*.
- **STB (Institutional Absorption)**: uptrend, MA20>MA50>MA100, volume giảm, khối ngoại
  BÁN ròng rất mạnh, buy aggressive >> sell aggressive, nhiều large order tại kháng cự,
  giá gần như không giảm tương ứng lượng bán ngoại → kỳ vọng *Institutional Absorption,
  uptrend giữ, chưa Distribution*.

Cả hai **bắt buộc** cần Context Engine (trend + location + foreign flow) — hệ hiện tại
chưa giải được.

---

## PHẦN A — Review kiến trúc hiện tại

### A.1 Đang có HAI bộ não song song

| | Engine 1 — Shark Score | Engine 2 — Order Flow |
|---|---|---|
| File | `shark_monitor._metrics` → `_behavior` | `order_flow.analyze` |
| Output | 1 số `[-100,100]` + nhãn | 6 khối dữ liệu (CVD/VP/VWAP/large/absorption/iceberg) |
| Absorption | `net<-0.08 AND price_chg>=0` (toàn tape) | `window=40 tick, imb<=-0.6 AND chg>=0` |
| Divergence | lớn/nhỏ (`big_dir−small_dir`) | giá/CVD (đuôi 30%) |
| Ai tiêu thụ | cột Mã + badge | tab Order Flow (chỉ hiển thị) |

Hai engine tính lại cùng khái niệm bằng công thức + ngưỡng khác nhau → mâu thuẫn hiển
thị, nợ kỹ thuật kép. `order_flow.analyze()` chỉ **hiển thị**, không nối vào quyết định
→ đúng nghĩa *Indicator Engine chứ chưa Decision Engine*.

**Việc số 1 của refactor: hợp nhất hai engine trên một Event schema chung.**

### A.2 Điểm mạnh (giữ lại)

1. `order_flow.py` sạch, thuần, O(n), trung thực về giới hạn → nền lý tưởng, chỉ mở rộng.
2. Adaptive threshold (percentile) đã làm đúng ở 2 nơi.
3. Score v2 đã chống đa cộng tuyến bài bản (đo `corr(big_dir,imbalance)=+0.92` rồi gộp
   `flow`, nhường trọng số cho phân kỳ + hấp thụ).
4. `_detect_manip` đã thoát tautology (đo trên nhánh ép, ngưỡng dip thích ứng per-exchange).
5. **`shark_backtest.py` đã đúng phương pháp**: forward return T+h **trừ baseline**,
   t-stat, `edge_vs_base`. Tổng quát hoá, đừng vứt.
6. `SCORE_VERSION` đã có → sẵn cho A/B.
7. Nguyên liệu Smart Money đã có nhưng chưa nối: `shark_history` (foreign/dealer/putthrough
   theo ngày), DNSE WS (orderbook L1).

### A.3 Điểm yếu

| # | Vấn đề | Vị trí |
|---|---|---|
| W1 | Indicator chứ chưa Decision | `analyze()` không có tầng tiêu thụ |
| W2 | Absorption hard-rule (giá giảm nhẹ vẫn là hấp thụ nhưng bị loại) | `order_flow.py:217`, `shark_monitor.py:937` |
| W3 | Window cố định theo tick (40 tick = 2' với STB nhưng = cả phiên với mã thấp) | `absorption_events(window=40)`, `_divergence(0.30)`, `_detect_manip(follow=30)` |
| W4 | Large order thiếu context (chỉ có vs_vwap) | `large_orders` |
| W5 | Không có Context Engine (không trend/MA/foreign/session-phase intraday) | toàn bộ intraday |
| W6 | Pattern yếu — chỉ là `List[str]`, không strength/confidence/context, không backtest được | `_behavior["flags"]` |
| **W7** | **Chất lượng phân loại B/S là nền móng thầm lặng** — mọi tín hiệu phụ thuộc `side` | ingest |
| **W8** | **Không persist ở mức EVENT** → không backtest được từng pattern/context | chỉ có `shark_score` |
| **W9** | Hai định nghĩa absorption cho 2 số khác nhau người dùng thấy được | A.1 |

---

## PHẦN B — Kiến trúc đề xuất (tận dụng tối đa, không viết lại)

```
DATA (đã có): tape_store(tick) · orderbook L1 · ohlcv(ngày) · shark_history(foreign/dealer/pt)
      │
      ├───────────────┬────────────────────────────┐
      ▼               ▼                            ▼
 LAYER 0           LAYER 1                     (session meta)
 Context Engine    Raw Metrics (order_flow.py mở rộng)
 market_context    • Delta/CVD/rolling-delta · VWAP+bands
 • trend MA20/50/100(ohlcv)     • Volume Profile/POC/VA
 • location vs VWAP/POC/VA      • Large orders (thô)
 • S/R (VP peaks + swing ohlcv) • aggressive buy/sell ratio
 • session phase ATO/cont/ATC   • tick statistics
 • foreign_dir/dealer_dir(ngày)
      └──────┬────────┘
             ▼
 LAYER 2  Pattern Detectors  (patterns/*.py)
 mỗi detector: (metrics, context) -> Event{type, ts, strength[-1..1], confidence[0..1],
                                            context, evidence[]}
 Absorption · Distribution · Institution Cluster · Passive Buy/Sell · Exhaustion ·
 Delta/CVD Divergence · VWAP Accept/Reject · POC Rotation · Breakout Prep
             ▼
 LAYER 3  Decision Engine  (decision.py)
 aggregate(events, context) -> SmartMoneyState {
   accumulation_score, distribution_score, breakout_score, trend_quality,
   market_control, institution_activity, smart_money_confidence,
   wyckoff_phase, conclusion(text), evidence_chain[] }
             ▼
 REPORT / VIZ (SmartMoneyReport + footprint heatmap + timelines + dashboard)
```

### B.1 Trái tim — Event schema có Context bắt buộc

```python
@dataclass
class Context:
    trend: str          # uptrend|downtrend|sideway
    ma_state: str       # "MA20>MA50>MA100"
    location: str       # support|resistance|inside_va|at_poc|breakout|mid
    vwap_side: str      # above|below|at
    session_phase: str  # ato|continuous|atc|plo
    foreign_dir: float  # -1..1 (ngày, từ shark_history)
    dealer_dir: float   # -1..1 (tự doanh)

@dataclass
class Event:
    type: str; ts: str
    strength: float     # -1..1 (dương=bullish)
    confidence: float   # 0..1
    context: Context
    evidence: list[str]
```

`Event` chính là *"Large Buy @ Resistance, VWAP above, Foreign selling, 14:20 →
Institutional Absorption, conf 82%"* ở dạng cấu trúc.

### B.2 Layer 2 — Detector graded (giải W2/W3)

Chuyển hard-rule sang composite chấm điểm liên tục; window **time/volume-based** tự co
giãn theo thanh khoản. Ví dụ Absorption:

```
Kỳ vọng: lực bán ròng đẩy giá xuống ~ β·sell_imbalance·σ_local.
strength = clamp((giá_thực_giữ − giá_kỳ_vọng_giảm) / σ_local)
confidence ↑ theo |imbalance|, số large order đối ứng, độ dài giữ giá.
```

Detector = hàm thuần `(metrics, context) -> Optional[Event]` → test/version/backtest
riêng lẻ được. `_detect_manip`, absorption/reversal, `_divergence` hiện có migrate vào.

### B.3 Layer 3 — Decision Engine minh bạch (tự giải thích)

Rule/score cộng trọng số trên Event, mỗi sub-score kèm `evidence_chain`. ML chỉ vào sau
như re-ranker khi đã đủ event có nhãn. Bắt đầu bằng minh bạch (người dùng VN cần hiểu vì
sao).

```
accumulation_score = Σ w_i · event_i.strength⁺ · event_i.confidence
distribution_score = tương tự (bearish)
breakout_score     = f(volume_expansion, poc_rotation_up, gần R, delta⁺)
institution_activity = f(large_order_density, cluster, |foreign_dir|+|dealer_dir|)
wyckoff_phase = state machine {Accumulation, Markup, Distribution, Markdown}
```

**Nghiệm 2 case:** HDB → accumulation cao, phase=Accumulation, "chưa Markup". STB →
institution cao, absorption detected, distribution KHÔNG bật (giá giữ, CVD⁺), "uptrend
giữ". ✓

---

## PHẦN C — Backtest (tổng quát hoá cái đang có)

Lợi thế: **tape đã persist** → replay thuật toán mới trên lịch sử cũ, deterministic,
không nhiễu market drift.

1. **Persist events** — bảng `smart_money_events(ticker, date, ts, type, strength,
   confidence, context_json, algo_version)` (mảnh còn thiếu — W8).
2. **Event-study tổng quát** — nâng `shark_backtest`: group theo `event.type` ×
   context-bucket (vd `absorption × uptrend × resistance`) → forward return T+h trừ
   baseline, t-stat, edge, win-rate. Giữ phương pháp trừ nền hiện có.
3. **Decision-level** — bucket theo decile `accumulation/distribution_score`; kiểm tra
   **tính đơn điệu** (score tốt thì return tăng đều theo decile — test mạnh hơn t-stat).

Chuẩn hoá output: căn event tại t=0, vẽ lợi suất tích luỹ trung bình ± SE theo horizon.

---

## PHẦN D — Benchmark giữa các version

- Mọi event/score mang `algo_version` → backtest lọc theo version.
- **A/B bằng replay**: chạy cùng tập tape lịch sử qua v_old & v_new offline → so cặp,
  loại market drift.
- **Headline metrics**: `edge_vs_base@T+3` (top bucket) · `t_stat, n` · **monotonicity**
  across deciles · `precision@k` (distribution→drawdown) · `coverage`.
- **Kỷ luật chống over-engineering**: mỗi pattern mới phải tự chứng minh bằng event-study
  (`edge>0 & |t|≥2 & coverage đủ`) — không thì loại.

---

## PHẦN E — Trực quan hoá

1. **Smart Money Report card** — render Decision output (rating sao) + nút "vì sao?" xổ
   `evidence_chain`.
2. **Footprint Heatmap** *(giá trị mới cao nhất)* — lưới `giá × bucket thời gian`, màu =
   net delta.
3. **Smart Money Timeline** — ribbon event dọc phiên, màu bull/bear, cao theo confidence.
4. **Large Order Timeline** — scatter (x=time, y=giá, size=value, màu=chiều, ký hiệu=context).
5. **Volume Profile** + overlay POC/VA/VWAP/giá + đường S/R.
6. **Composite Dashboard** — gauge accumulation/distribution/breakout/trend-quality/institution.

---

## Lộ trình

| Phase | Nội dung | Kết quả |
|---|---|---|
| **0** | Audit trường `side` (nguồn/độ tin) | Biết nền có vững |
| **A** | Event schema + `market_context.py` (trend/location/session/foreign) + persist events | Context Engine |
| **B** | Migrate detectors → graded, window thích ứng; hợp nhất absorption | Layer 2 |
| **C** | `decision.py` + Smart Money Report | Decision Engine |
| **D** | Tổng quát hoá backtest sang event-level + benchmark version | Đo từng pattern |
| **E** | Footprint heatmap + timelines + dashboard | Trực quan |

---

## Tiến độ

### ✅ Phase 0 — Audit trường `side` (xong)

**Verdict: nền móng VỮNG.** `side` là aggressor do vendor cấp (vnstock `match_type`, DNSE
`tick_extra.side`), **không suy bằng tick-rule trong code**.

Bằng chứng (phiên 23/07, dữ liệu thật KBS):

| Mã | n | %U | tick-rule đồng thuận | KBS∩VCI đồng thuận side |
|---|---|---|---|---|
| STB | 1.643 | 0.12% | 80.6% | 100% |
| HDB | 1.956 | 0.10% | 95.7% | 98% |
| VIC | 2.763 | 0.07% | 84.4% | 100% |

- %U ≈ 0.1% (chỉ ATO/ATC). Tick-rule 80–96% (không phải ~50%) ⇒ `side` mang thông tin
  aggressor thật. KBS vs VCI đồng thuận 98–100%.
- Aggregation chỉ gộp cùng-chiều + cùng-nguồn (`_append_agg:670`) ⇒ không hỏng side.
- Insight: STB tick-rule chỉ 80% (thấp hơn HDB 96%) — đúng chữ ký hấp thụ (bán chủ động
  mà giá không downtick).

Công cụ: `tape_health(ticks)` + `GET /api/shark/health/{ticker}` (commit `23b9bdc`).

Phát hiện phụ:
- VCI fallback HỎNG (`Quote(source="vci").intraday()` → RetryError) + cắt cứng 100 khớp
  → KBS là nguồn vnstock duy nhất thực dụng. Không khẩn cấp.
- Auction (ATO/ATC) = U, loại khỏi CVD (đúng), nhưng `volume_profile` chia đôi 50/50 →
  cần xử lý auction nhất quán ở Layer 1.
- Mang sang Phase A: gắn `side_source` (dnse/kbs) vào tick/event để chấm confidence &
  backtest phân tách theo nguồn.

### ✅ Phase A — Event schema + Context Engine (xong)

Module `market_context.py` (Layer 0) + persist scaffold:

- **Schema**: `Context` (trend, ma_state, location, vwap_side, session_phase, foreign_dir,
  dealer_dir + tham chiếu định lượng price/MA/S-R/POC/VA/VWAP) và `Event` (type, ts,
  strength, confidence, context, evidence, algo_version) — dùng chung cho Layer 2/3.
- **`build_context(ticker, ticks, of, with_foreign)`** nối 3 nguồn ĐÃ CÓ:
  - trend + MA20/50/100 + hỗ trợ/kháng cự + breakout từ `ohlcv_store` (nến ngày).
  - VWAP + POC/Value Area từ `order_flow` (tái dùng cache order flow).
  - foreign_dir/dealer_dir từ `shark_history` (dòng tiền ngày, cache 15').
  - session_phase (ATO/liên tục/ATC/PLO) theo giờ tick cuối — deterministic cho replay.
  - Phòng thủ: thiếu nguồn nào → field trung tính, không ném lỗi. Đơn vị kVND cả 2 phía.
- **Persist**: bảng `smart_money_events` + `save_events/load_events` (idempotent theo
  algo_version) — kho cho detector Phase B & backtest Phase D.
- **Endpoint**: `GET /api/shark/context/{ticker}`; wrapper `shark_monitor.get_context`.

Nghiệm thu trên dữ liệu THẬT (phiên 23/07):

| Mã | trend | location | MA20/50/100 | Khớp kỳ vọng |
|---|---|---|---|---|
| STB | **uptrend** [MA20>MA50>MA100] | **resistance** (72.0 sát R 72.3) | 71.8/71.0/68.3 | ✓ "uptrend, large order tại kháng cự" |
| HDB | **sideway** [MA50≈MA100] | support | 26.69/26.10/26.05 | ✓ "sideway, MA50≈MA100" |
| VIC | uptrend | resistance | 219/213/191 | ✓ |

Context giờ nắm đúng thông tin engine cũ mù → nền để Layer 2/3 diễn giải 2 case.

**Còn nợ sang Phase B**: gắn `side_source` vào tick; xử lý auction (ATO/ATC) nhất quán ở
Layer 1; detector emit `Event` rồi `save_events`.

### ✅ Phase B — Detector graded Layer 2 (xong)

Module `patterns.py` — mỗi detector là hàm thuần `(ticks, context, ...) -> List[Event]`:

- **Cửa sổ THEO THỜI GIAN** (`time_windows`, mặc định 5') thay tick cố định (giải W3) —
  số cửa sổ co theo độ dài phiên, không theo mật độ khớp.
- **`detect_absorption` — impact-residual, graded, thích ứng theo mã** (giải W2/W9):
  ước lượng độ nhạy giá–imbalance `k` của CHÍNH mã trong phiên → residual = chg−k·imb →
  z-score. Bán áp đảo mà giá giữ hơn dự báo → `absorption` (bullish); mua áp đảo mà giá
  không lên → `supply_absorption` (bearish). Giá giảm NHẸ hơn mô hình vẫn tính hấp thụ.
- **`detect_divergence`** — phân kỳ giá/CVD phần cuối phiên bằng **tương quan hạng**
  (Spearman, bền hơn max/min của bản cũ).
- **`detect_institution_cluster`** — chỉ báo cụm lệnh lớn **BẤT THƯỜNG** (≥ P75 số lệnh
  lớn/cửa sổ), confidence theo cường độ tương đối (không 'always-on' như đòi ≥3 tuyệt đối).
- Mỗi event mang **context TẠI THỜI ĐIỂM** (`_ctx_at`: location tính theo giá lúc đó —
  một lệnh sáng ở support khác chiều ở resistance) + strength + confidence + evidence.
- `detect_all` orchestrator; `shark_monitor.get_events` (build context → detect → persist
  `smart_money_events`); `GET /api/shark/events/{ticker}`.

Nghiệm thu (dữ liệu 23/07, `net = Σ strength·confidence`):

| Mã | net | Đọc | Khớp kỳ vọng |
|---|---|---|---|
| STB | **+3.4** | buy-absorption + cluster mua tại uptrend | ✓ Institutional Absorption (bullish) |
| HDB | **+0.5** | trung tính, cụm mua nhẹ tại support | ✓ Accumulation chưa hướng |
| VIC | +1.6 | cluster mua mạnh + supply_absorption cùng lúc | ✓ cung gặp cầu tại kháng cự |

**Còn nợ**: detector Distribution/Exhaustion/VWAP-accept (Phase B.2 hoặc gộp vào Layer 3);
per-event context hiện chỉ đổi `location` (trend/S-R giữ mức phiên — chấp nhận được vì
trend theo ngày).

### ✅ Phase C — Decision Engine + Smart Money Report (xong)

Module `decision.py` (Layer 3) — minh bạch (rule/score, kèm `components` truy vết):

- **Sub-scores**: accumulation / distribution / breakout / trend_quality /
  institution_activity / bull_strength / bear_strength / market_control /
  smart_money_confidence.
- **MẤU CHỐT**: distribution GATE đóng góp của khối ngoại/cluster bởi mức **absorption** —
  khối ngoại BÁN mà giá được hấp thụ (STB) thì `foreign_dist = max(0,-foreign)·(1−absorp)`
  → ~0 → distribution KHÔNG tăng oan. Đây là thứ phân biệt "phân phối" thật với "hấp thụ".
- **Wyckoff MỞ RỘNG**: Accumulation · Spring · Markup · Buying Climax · Distribution ·
  Markdown · Trung tính — cây quyết định theo trend + điểm + vị trí + phân kỳ.
- **Smart Money Report**: sao (0–5) cho 5 chiều + cờ (absorption/distribution/vwap/poc/
  delta/cvd) + **kết luận tiếng Việt** ghép từ trạng thái + `evidence_chain` (truy vết).
- `shark_monitor.get_decision` tự dựng extras (vol_trend ngày, dịch POC nửa phiên, delta
  gần đây); `GET /api/shark/decision/{ticker}`.

Nghiệm thu (23/07):

| Mã | Wyckoff | absorption | distribution | Kết luận rút gọn | Khớp |
|---|---|---|---|---|---|
| STB | **Markup** | Detected | Not detected (17) | "được hấp thụ, giá giữ, chưa phân phối" | ✓ đúng Institutional Absorption |
| HDB | Trung tính | Not detected | 0 | đọc trung thực phiên (tape bán nhiều, CVD âm) | ✓ honest, không ép theo brief |
| VIC | Markup | — | 21 | "cung tại vùng cao — theo dõi phân phối" | ✓ sắc thái đúng |

**Ghi chú calibrate**: ngưỡng phase (acc/dist ≥55) và trọng số sub-score là **tham số**,
sẽ hiệu chỉnh bằng backtest Phase D. Foreign_dir=0 khi thiếu token FireAnt (local) → trên
server sẽ đủ tín hiệu khối ngoại.

### ✅ Phase E — Trực quan hoá (frontend, xong build)

Tab **Smart Money** trong Shark Action (frontend `SharkPage.tsx` + hook `useSharkDecision`):

- **Header**: mã + badge Wyckoff phase (màu theo pha) + market_control + Bull/Bear + độ tin.
- **Rating sao** (0–5) × 5 chiều: Xu hướng · Tổ chức · Gom · Xả · Breakout.
- **Thanh điểm** (0–100): accumulation/distribution/breakout/institution/trend_quality.
- **Cờ nhanh** màu: Hấp thụ · Phân phối · VWAP · POC · Delta · CVD.
- **Kết luận** tiếng Việt (render **đậm**).
- **Timeline sự kiện**: vạch theo giờ 09:00–14:45, trên/dưới trục = gom/xả, cao theo
  |strength|, đậm theo confidence; kèm danh sách 8 event mạnh nhất + evidence.
- **Ngữ cảnh** (trend/MA/S-R/VWAP/POC/ngoại-tự doanh) + **evidence chain**.

Gọi 1 endpoint `GET /api/shark/decision/{ticker}` (đã kèm `events` để vẽ timeline).
Type-check + build frontend PASS. Cần deploy backend để có dữ liệu.

### ✅ Phase E.2 — Footprint heatmap + dashboard gauge (xong)

- Backend: `order_flow.footprint(ticks, price_bins=20, time_buckets=30)` → lưới net delta
  (giá×thời gian) + tô POC/VWAP; đưa vào `analyze()`. Kiểm: tổng lưới = CVD cuối phiên.
- Frontend: `FootprintHeatmap` trong tab Order Flow (ô xanh gom/đỏ xả, đậm theo |delta|,
  nhãn giá POC/VWAP + trục thời gian). `SMGauge` bán nguyệt "Cán cân dòng tiền" (Xả◄►Gom
  = accumulation−distribution) trong tab Smart Money.

---

## Smart Money v2 — Explainable AI (từ "hiển thị kết quả" → "giải thích quyết định")

Mục tiêu: mỗi kết luận trả lời được "TẠI SAO?". Không thêm indicator. Lộ trình:

| Phase | Nội dung | Trạng thái |
|---|---|---|
| **F1** | **Contribution Ledger** — mỗi điểm = danh sách +/− (nguồn, nhãn, điểm, reliability) + confidence per-score | ✅ |
| F2 | Evidence Engine — ✓/✗ ngôn ngữ người cho mỗi kết luận | ✅ |
| F3 | **Decision = Rule-based Inference**: Score+Context+Regime+Evidence+Conflict+Memory → Hypotheses + State/Risk/Action(định tính+vùng giá)/Reason | ✅ |

### Kiến trúc suy luận (Signal → Evidence → Hypothesis → Decision)

Nâng mục tiêu từ "cộng điểm" → hệ suy luận. Các thành phần là MỞ RỘNG, không refactor lớn
(mối nối đã có sẵn):

| Thành phần | Mối nối sẵn có | Trạng thái |
|---|---|---|
| Regime (market state) | Context có trend/MA → thêm `Context.regime` | ✅ seam (weights F3) |
| Context→inference (reliability theo vị trí) | `_mk(rel_mult)` — hấp thụ tại hỗ trợ rel↑, tại kháng cự rel↓ | ✅ |
| Conflict Resolution | Ledger tách pro/con → conflict = mức đối kháng, phạt confidence | ✅ |
| Hypothesis Engine | Ledger đã nhóm theo giả thuyết (gom/xả/breakout) → chuẩn hoá xác suất | ⏳ F3 |
| Smart Money Memory | **`smart_money_events` đã persist mỗi phiên** → chỉ thêm hàm đọc/tổng hợp | ⏳ (interface) |
| Decision Inference | `decide()` là orchestrator → thêm input có default | ⏳ F3 |

**Hợp đồng dữ liệu (định nghĩa nay, impl F3/sau):**

```python
# Hypothesis Engine (F3)
@dataclass
class Hypothesis: name: str; probability: float; drivers: list[str]; regime_fit: float
# decide() → {"hypotheses": [Hypothesis...], "primary": name}  (softmax trên điểm nhóm)

# Smart Money Memory (sau) — đọc từ smart_money_events + shark_score đã lưu
def recent_summary(ticker, sessions=5) -> {
  "absorption_buy": int, "cluster": int, "poc_trend": "up|down|flat",
  "foreign_streak": int, "phase_history": [str], ...}
# → input cho Hypothesis (tín hiệu đơn lẻ vs quá trình kéo dài) + Conflict giữa phiên
```

### ✅ F1.5 — Reasoning seams (#3 #4 #5 xong)

- **#4 Regime**: `Context.regime ∈ {trending_up, trending_down, sideway}` (Layer 0, từ trend/MA).
- **#5 Context-reliability**: `_mk(rel_mult)` — hấp thụ tại hỗ trợ ×1.18, tại kháng cự ×0.72;
  cung tại kháng cự ×1.18; cụm mua tại hỗ trợ ×1.12. Nghiệm thu: hấp thụ HDB@support rel
  **0.95** vs STB@resistance rel **0.59**.
- **#3 Conflict**: từ 6 tín hiệu hướng (cvd/flow/absorp−supply/foreign/cluster/diverg) →
  `conflict = 2·min(pos,neg)/(pos+neg)`; phạt `confidence ×(1−0.35·conflict)`. Nghiệm thu:
  HDB conflict 94(Cao)→conf 58%; STB conflict 0(Thấp)→conf 84%. Frontend: badge regime +
  "Mâu thuẫn {mức}" ở header Smart Money.
| F4 | Story Engine — kể chuyện dòng tiền theo thời gian + Smart Money Story | ⏳ |
| F5 | Large Order aggregates (#9) | ⏳ |
| F6 | Presentation — hover breakdown, dashboard mới, typography | ⏳ (một phần: bấm điểm số xem ledger) |

### ✅ F1 — Contribution Ledger (xong)

`decision.py`: chấm điểm chuyển từ tổng-ẩn sang **sổ cái đóng góp**.
- `Contribution{source, label, points(+/−), polarity, reliability}`; `RELIABILITY` per-detector
  (cluster 0.85 > absorption 0.82 > … > poc 0.55 > vol 0.50) — #6 Confidence Engine.
- Mỗi điểm (accumulation/distribution/breakout/institution/trend): `score = clamp(Σ points)`,
  `confidence = Σ|points|·reliability / Σ|points|`. Thêm số hạng PHẠT (foreign bán, POC
  xuống, cung chặn, trend yếu) → điểm có cả (−), thực tế hơn.
- Output thêm `ledgers` + `score_confidence`. Frontend: bấm mỗi thanh điểm ở tab Smart
  Money → xổ "Vì sao?" (danh sách +/− + reliability).
- Nghiệm thu 23/07: STB Acc=48 (+14.7 cụm tổ chức mua, +14.5 hấp thụ mua, −4.6 cung chặn),
  Dist=5 (−9.3 đang được hấp thụ). Điểm = Σ points (đã kiểm).

### ✅ F2 — Evidence Engine (xong)

`decision._evidence_engine`: gom sổ cái của HƯỚNG CHI PHỐI (lean gom/xả/trung tính) thành
✓ thuận / ✗ nghịch bằng ngôn ngữ người + bằng chứng ngữ cảnh bổ sung (VWAP acceptance,
breakout chưa xác nhận), kèm confidence (đã phạt conflict). Bỏ đóng góp "nền" (offset cấu
trúc). Output `evidence{conclusion, lean, supporting[], contradicting[], confidence}`.
Frontend: card "Bằng chứng" ✓/✗ ở tab Smart Money.

Nghiệm thu 23/07: STB Markup — ✓ 9 cụm tổ chức mua, ✓ 2 hấp thụ mua, ✗ cung chặn, ✗ giá
dưới VWAP (conf 74%). HDB Trung tính — ✓ nền tích luỹ tại hỗ trợ, ✓ hấp thụ, ✓ giá giữ
trên VWAP, ✗ breakout chưa xác nhận (conf 60%).

### ✅ F3 — Decision = Rule-based Inference (xong)

`decision._hypotheses` + `_decision` — KHÔNG chỉ cộng điểm, mà suy luận từ nhiều đầu vào:

- **Hypothesis Engine**: sinh giả thuyết SONG SONG {Tích luỹ, Phân phối, Markup, Markdown,
  Rũ hàng, Cao trào mua, Chưa rõ} → raw score (regime chi phối trọng số: absorption trong
  sideway ≠ uptrend) → **softmax (T=18)** → xác suất. `memory` là seam (None = bỏ qua).
  "Chưa rõ" tăng theo conflict → tín hiệu mâu thuẫn thì giả thuyết phân tán.
- **Decision (inference)**: từ giả thuyết ưu thế + regime + conflict + confidence + context
  → `state · institution · trend · risk_level · action · reference_zones · reason`.
  **Action ĐỊNH TÍNH** (Theo dõi tích luỹ / Theo dõi xu hướng tăng / Cảnh giác rủi ro /
  Quan sát thêm) + **vùng giá tham chiếu** (vượt R xác nhận, giữ trên S an toàn) — KHÔNG
  lệnh mua/bán. Reason ghép từ bằng chứng mạnh nhất + mốc giá + cảnh báo mâu thuẫn.
- Output `hypotheses` + `decision`. Frontend: **thẻ Quyết định** (headline, đọc 30s) ở đầu
  tab Smart Money — Action lớn + chip State/Institution/Trend + Risk + thanh giả thuyết + lý do.

Nghiệm thu 23/07: STB → Markup 90%, "Theo dõi xu hướng tăng", Risk Thấp, mốc vượt 72.30 /
giữ 71.60. HDB (conflict cao) → "Chưa rõ 62%", "Quan sát thêm", Risk Trung bình. VIC →
Markup 83%.

**Còn lại**: F4 Story Engine · F5 Large Order aggregates · F6 Presentation. Memory (đọc
`smart_money_events` nhiều phiên) = seam đã có, impl khi cần.

### 🔨 Phase D — sau (backtest event-level + benchmark version)

Tổng quát hoá `shark_backtest`: persist đã có (`smart_money_events`) → group theo
`event.type × context-bucket` → forward return T+h trừ baseline, t-stat, edge, win-rate,
monotonicity. Benchmark version bằng replay tape lịch sử. Đây là bước hiệu chỉnh trọng số
Layer 3 dựa trên bằng chứng thay vì cảm tính. **Cần tích luỹ event qua nhiều phiên mới
đọc được kết quả** (giống `shark_backtest` hiện có).
