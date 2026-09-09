# **Data Quality Scoring Guidelines**

This document describes the criterion that will be used to judge the data quality of a specific map or set of maps for [Registry](https://github.com/Subway-Builder-Modded/registry).

The intent for this system is to both measure the objective quality of the raw data bounding the demand modeled within a map (for internal review) and then distill that quality into a single, usable metric for non-technical users (players)

## **1. Criterion**

A Subway Builder map is a closed-system model of how residents within a city normally commute. The provenance of the underlying census / official data bounds how realistic that model can be, and the quality of that data is what this rubric is intended to measure.

Countries around the world publish these statistics in disparate formats and with varying methodologies; this rubric is **NOT** intended to be an exhaustive reference of what can be used to model a Subway Builder map (or macro commute flows in general), but rather to provide a reasonable framework for comparing the relative quality of different data sources and the maps they produce.

This rubric thus scores data quality along **three pillars**, weighted by how much each shapes modeled demand:

| Pillar        | Weight | Captures                                        |
| :------------ | :----: | :---------------------------------------------- |
| **Workplace** |  0.50  | the destination side — where the jobs are       |
| **Resident**  |  0.35  | the origin side — where people live             |
| **O/D**       |  0.15  | the pairing — who commutes from where, to where |

**Why these weights?**

- **Workplace — 0.50.** _Workplace data is scored the highest. Workplaces are more concentrated than resident demand, and the nodes they produce within a modeled map are therefore the most load bearing. A concentrated set of destination points is more sensitive to misplacement than origin points, which are more diffuse. And during simulation, destination nodes provide a user with clear structure to work around._
  - _In addition, workplace data is often more difficult to obtain than resident data, and is more likely to be suppressed or estimated. A map that can accurately model workplace demand is therefore more likely to be a high-quality map overall._

- **Resident — 0.35.** _Resident data is scored the second highest; it represents the origin side of every commute and is generally more uniformly distributed than workplace data. While less load-bearing than workplace nodes, residential density provides a solid foundation for modeling commute flows._

- **O/D — 0.15.** _O/D data is scored the lowest; a gravity model can provide approximate (if unrealistic) flows from the origin/destination sides alone, so a measured O/D refines rather than creates demand. However, O/D data is still valuable for macro-flow fidelity which a gravity model would otherwise smooth over._

Within the Workplace and Resident pillars, quality further splits into a **count** (the ground-truth magnitude and the grain it is measured at) and a **dasymetric** (how that count is placed in space within the ground truth grain), as discussed in the following sections.

### **Relationship to the self-reported quality tag**

[Registry](https://github.com/Subway-Builder-Modded/registry) currently records data quality as a single author-declared `source_quality` tag — `high` / `medium` / `low`. That tag is a useful first cut to distinguish map quality, but is limited in several ways this rubric is built to address:

- **Single-axis.** It grades only whether the _source_ is official; it does not ask what was measured, at what grain, how it was placed, or whether any commute flow data was observed. Two `high` maps can differ enormously in actual data quality: a government total at provincial (ADM1) grain and a government block-level (ADM5) census are both "official," yet are wholly different levels of data quality.
- **Self-reported.** Authors grade their own work, so the tag is not comparable across authors and biases the value upwards. This rubric applies objectively the same named tiers to every pipeline, so a map's score is auditable and comparable.
- **Coarse.** The three buckets encompass the full range of scores; this rubric resolves a continuous [0, 1] score for internal use, which can then be mapped to a coarse tier system as needed.
- **Opaque.** A tag says _how good_, never _why_. This rubric is especially userful for map reviewers; the author would now be required to report more granular detail on their source data.

The metric should then be used as a refinement over the existing tag. A `high` / `medium` / `low` split is useful for the end-user (the person using the simulation), and they likely do not need to or want to know the exact methodology or source data the map draws from.

The continuous rubric score resolves into **six named tiers** — very high, high, medium, low, very low, absent (aliased **A**–**F**) — which are themselves a refinement of the registry's three: **very high** and **high** collapse to `high`, **medium** to `medium`, and **low**, **very low** and **absent** to `low`. After review, the rubric-assigned tier therefore **supersedes** `source_quality` rather than rewriting it: existing self-reported tags are frozen for legacy compatibility, and new registry listings initialize the legacy tag from the reviewed tier (via the collapse above) at creation only.

## **2. Granularity**

The quality of the metric being measured by the census/official statistics is secondary to the granularity at which the metric is measured. The finer the measurement granularity, the higher fidelity and detail to which a map can be modeled. Within this document, the following definitions are used:

| Metric Granularity | Description                                                                                                                                            | Examples                                                           |
| :----------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------- | :----------------------------------------------------------------- |
| Mesh               | Data is presented in a mesh grid, with each present cell carrying the measured value of the metric for the establishments / buildings within that cell | INSPIRE Grid 1km × 1km, 国勢調査 250m native grid, etc.            |
| ADM5               | Data is measured per micro / block-level admin unit — the finest measured admin grain (census block / measured-population statistical block)           | US census block, TW 最小統計區 (MSA) sub-里, PE manzana            |
| ADM4               | Data is measured per sub-municipal admin unit                                                                                                          | US census block group, JP 町丁目, CZ ZSJ-díl, PL rejon, LV pagasts |
| ADM3               | Data is measured per municipal admin unit                                                                                                              | US CDP, JP 市区町村, CZ obec, PL gmina, UA громада                 |
| ADM2               | Data is measured per regional admin unit                                                                                                               | US county, CZ okres, PL powiat, UA район                           |
| ADM1               | Data is measured per provincial admin unit                                                                                                             | US state, JP 都道府県, CZ kraj, PL województwo, UA область         |
| None               | No census anchor                                                                                                                                       | N/A                                                                |

### **Data Suppression**

When reporting these values, please use the highest granularity data that you have available that does NOT suffer from significant (>5%) privacy-based suppression.

_Some countries will refuse to provide data points that contain only a handful of residents; therefore it is important to cross-check the total metric counts for a particular administrative boundary level against a known unsuppressed ground truth total of a coarser granularity. If the totals mismatch greatly, it is likely the artifact of data suppression._

### **Choosing the grain for hybrid or ambiguous sources**

Granularity scores the grain at which a count's **magnitude** is measured — not the finer grain it may ultimately be placed at (that finer placement is credited in the [Dasymetric](#3-dasymetric) section). Some sources blur the two: a total measured at a coarse unit is redistributed to finer units that carry their own partial signal. To resolve which grain to score:

1. **Find where the magnitude is actually measured.** A total published per municipality and then spread across buildings by a model is _measured_ at the municipality — score that grain; the finer placement earns its credit only in the dasymetric (R × I).
2. **Determine whether the finer units carry independent magnitude information of their own.** If they do (e.g. establishment employee-size bands, per-building dwelling counts), the finer grain is partly measured, not pure interpolation, and the effective grain sits **between** the coarse anchor and the fine unit; for example:
   - Exact per-unit counts (e.g. block-level headcounts) => score the **fine** grain.
   - A partial per-unit signal (e.g. size _bands_ constrained to a coarse total) => score a **middle** grain.
   - No independent finer signal (a coarse total spread by geometry alone) => score the **coarse** grain.
3. **When it remains ambiguous, take the middle and record the bounds** so the call is auditable. _Example:_ Mexico's workplace magnitude is measurable at varying granularities: ~ADM2 (municipal) as a floor, with some industry counts available at ~ADM3/4 level. In addition, the establishment directory carries real employee-size bands at address level (ADM5). Within the rubric, it is thus scored at **~ADM4**, between the coarser ADM2 (total-only resolution floor) and the finer ADM5 (treat the establishments as measured units).
4. **A geocoded sample is graded by the grain its sample size statistically supports** — typically the grain where its expansion factors are calibrated — not by the precision of its geocoding. A survey that records exact addresses for two percent of households does not carry block-grain magnitudes: at fine grain each cell holds a handful of observations, and the counts only stabilize at the unit where the survey was designed to close.

### **Multi-nation maps**

A map spanning national borders draws each pillar from more than one statistical system, and those systems rarely match in quality. **Each scored factor takes the floor across the nations covered** — the weakest count anchor, the coarsest measured grain, the weakest placement — regardless of each nation's population share. The tier is a single promise about the whole map: on its weaker side the map behaves like the weaker pipeline, and a blended or best-side score would overstate exactly the areas a player cannot distinguish.

Two refinements:

- **O/D is scored on what actually constrains the flows.** A real cross-border commuter series that bounds a border map's defining flows can earn the structured-marginals rung even when one nation's internal flows are synthetic — the weak side's gravity model is why the pillar is not scored higher, not a reason to zero it.
- **Record the per-nation bounds in the review notes** (same spirit as the grain rule above), so the discount is auditable and a data upgrade on the weak side can be re-scored without re-deriving the strong side.

_Example: suppose a map spans the Øresund strait. The Swedish side publishes measured 100 m population and workplace grids; the Danish side (say) publishes workplace totals only per municipality, placed by building size. Every workplace factor is scored at the Danish rungs — municipal grain, size-only placement — even if most of the map's residents live on the Swedish side. A real cross-strait commuter series still earns the O/D structured-marginals rung, and the review notes record both sides' rungs so a Danish data upgrade can later be re-scored without re-deriving the Swedish half._

## **3. Dasymetric**

A **dasymetric** distributes a count in space _below_ the grain at which it is measured ([Granularity](#2-granularity)). It is shared by the Workplace and Resident pillars — scored identically for both — as the product of two independent factors:

`dasymetric_score = R × I`

- **Spatial Resolution (R)** — how finely mass can be placed (the geometry / coverage available).
- **Intensity Fidelity (I)** — how well the mass is apportioned among those candidate locations.

Splitting them lets the rubric distinguish a method that fits per-type densities to the area's ground truth total from one that spreads building mass with a flat prior, even when both draw on the same building layer. The tiers are identical for workplace and resident; only the example sources differ (noted inline).

A dasymetric is important when the base ground truth grain is coarser than the expected demand-point grain of the map. A map with points only at ADM3 (municipal) grain will be far less usable as a model (and far less engaging to simulate) than one with points at ADM5 (sub-municipal) grain, even if both are drawn from the same ADM3 ground truth total. The dasymetric is what allows the model to place demand nodes at a finer grain than the base ground truth, and a strongly constrained dasymetric enables maps with otherwise poor resolution to place well-scaled nodes at realistic locations.

When properly applied, a dasymetric can also help mitigate the effects of data suppression. If a census mesh cell is suppressed, but the dasymetric has a building footprint overlay, an approximate mass can still be placed in the "correct" location.

### **Spatial Resolution (R) — where mass can land**

| Resolution                                              | Weight | Description                                                                                                                                                                                                                        | Examples                                                                   |
| :------------------------------------------------------ | :----- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------- |
| Exact building footprints                               | 1.0    | An authoritative national cadastre / building register places mass to the individual structure                                                                                                                                     | CZ RÚIAN, PL BDOT10k, TW NLSC, LT GRPK, LV VZD, EE ETAK/EHR                |
| Census mesh ≤ 125 m (uniform) OR measured-pop ADM5 unit | 0.9    | A uniform ≤125 m grid, OR an official ADM5 census unit (census block / sub-里) carrying a **measured** per-cell count — census-block scale (~120 m urban), near-building resolution                                                | CZ 100m grid, PL 125m urban, LV 100m urban, TW MSA sub-里, US census block |
| Census mesh ≤ 250 m                                     | 0.85   | A fine, complete published grid cell (building-footprint refinement within the cell lifts toward 1.0)                                                                                                                              | JP 250m (residents) / 500m→100m FA (workers), LT 250m                      |
| Census mesh ≤ 500 m                                     | 0.75   | A medium published grid cell — finer than 1 km but too coarse to approximate building placement; a building-footprint refinement within the cell lifts toward the ≤250 m tier                                                      | Unrefined 500m meshes                                                      |
| ML / hybrid footprints (near-complete)                  | 0.7    | ML / satellite or multi-source footprints (GHS-OBAT, Google/MS Open Buildings, Overture = OSM∪Google∪MS∪Esri) — near-complete coverage, so mass can land wherever buildings exist; geometry inferred, no native use-tags           | UA Overture + GHS-OBAT + GHS-BUILT-V                                       |
| OSM-only footprints (coverage gaps)                     | 0.6    | Crowd-sourced footprints alone — accurate where present but **incomplete and uneven**: coverage is sparse in rural areas and, in many countries, spotty even in cities, so demand in uncovered cells falls back to worse placement | Minimal OSM-only pipeline                                                  |
| Census mesh ≤ 1 km                                      | 0.5    | A coarse published grid cell                                                                                                                                                                                                       | EE INSPIRE 1km (before building refinement)                                |
| Admin polygon only                                      | 0.3    | No sub-admin geometry; mass spread across the whole unit                                                                                                                                                                           | —                                                                          |

### **Intensity Fidelity (I) — how the total is shared among those locations**

| Fidelity                                                    | Weight | Description                                                                                                                                                                                                                                                                                                                    | Illustration                                                                                                                                                                | Example Sources                                                                                       |
| :---------------------------------------------------------- | :----- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :---------------------------------------------------------------------------------------------------- |
| Directly measured (per cell / building)                     | 1.0    | The cell or building already carries its own measured count, so nothing is apportioned — the data states exactly how much demand sits where (so a source is not penalized for lacking a building overlay when its unit is already at demand-point grain)                                                                       | A census grid whose every cell reports its own worker or resident count                                                                                                     | JP 経済センサス worker mesh, US LODES; JP / PL / TW population meshes; EE + CZ per-building dwellings |
| Fine building-type weights (NACE class), locally calibrated | 0.85   | The total is split among buildings by a **fine, multi-type use taxonomy** (NACE classes: office, factory, shop, warehouse, hotel, clinic, …), with the per-type weights tuned to match the country's own published employment or housing figures                                                                               | A factory, an office and a corner shop each get a distinct worker density, in proportions checked against the national census                                               | CZ SLDB-NACE, PL Tikhonov, TW bounded-Tikhonov, LT measured density, LV / EE mesh500 NNLS             |
| Fine building-type weights (NACE class), generic            | 0.6    | The same fine, multi-type split, but the per-type weights come from generic or international rules of thumb rather than the country's own data — plausible, but not locally verified                                                                                                                                           | Buildings are still ranked office > factory > shop > warehouse, but from a standard reference table, not one fit to the country                                             | UA (literature + PL analogue) based priors + floor caps; GHS-POP residential                          |
| Coarse sector split (broad productive types)                | 0.5    | The total is split by only a **handful of broad productive types** — a primary / secondary / tertiary sector grouping, or an equivalent office / industrial / retail set. It separates the big density groups (worker-dense services from sparse industry) but resolves no fine types; within a sector, mass is spread by size | Offices and factories are told apart as "tertiary vs secondary," but a warehouse and a corner shop in the same sector are weighted alike by floor area                      | Primary / secondary / tertiary sector dasymetrics                                                     |
| Binary worker / resident split                              | 0.4    | Exactly **two types** — is a building a workplace or a home? Mass is routed onto the correct side (workers off residential, residents off commercial) and spread by size within it, but no productive types are distinguished (a warehouse and an office weigh alike)                                                          | Workers are kept out of apartment blocks and placed on commercial / industrial buildings by volume, but a shed and an office tower of equal volume receive the same workers | CN (commercial / residential volume split)                                                            |
| Building size only (no type)                                | 0.25   | No distinction between building types at all — demand is spread by raw building size (floor area or footprint) across **every** building, so workers can land in homes and a warehouse and an apartment block of equal size receive the same amount                                                                            | A large industrial shed and a large residential tower each receive demand in proportion to their floor area, with no sense of which is a workplace                          | Native-cadastre floor-area split (no use classification)                                              |
| Uniform                                                     | 0.1    | Demand is spread evenly, ignoring even building size and location — the maximum-entropy placement. Its near-zero contribution comes through the low **R** it is usually paired with (admin polygon); kept just above 0 so that "mass on buildings" (a higher R) is not erased by the R × I product                             | Every part of the area receives the same demand density                                                                                                                     | Admin polygon only                                                                                    |

## **4. Workplace**

_The most important pillar, accounting for half of the weighted score. Scored as **count** (below) × **dasymetric** (the shared [Dasymetric](#3-dasymetric) R × I ladders)._

### **Workplace Count Ground Truth**

The physical/registered split is not binary. Census place-of-work questions and establishment censuses **measure** physical work location; some registers have the statistics office **infer** it; others take the firm's **self-declared** address as the workplace location. The following table describes the different workplace-count anchors and their relative weights.

| Workplace Count                     | Weight | Description                                                                                                                                                                                                 | Illustration                                                                                                                                                           | Example Sources                                   |
| :---------------------------------- | :----- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------ |
| Physical — instrument-measured      | 1.0    | A census place-of-work question or establishment census measures where each employee physically works                                                                                                       | A worker in a rural logistics facility is counted in the rural area where they physically work; each area's count is the sum of everyone who works within it           | JP 経済センサス / 従業地, CZ dojížďka, TW 工商業  |
| Physical — inferred                 | 0.85   | A register that the statistics office has cross-referenced to physical work location (not the raw registered address)                                                                                       | The same rural-facility worker still lands in the rural area, but the location is reconstructed from linked administrative records rather than asked directly          | LV NPV020 (VID + VSAA + OCMA physical inference)  |
| Registered — self-declared location | 0.7    | The count is taken at the firm's declared establishment / HQ address; a multi-site firm collapses onto one node                                                                                             | A worker in a rural facility is counted in the city where their employer is registered, not where they actually work                                                   | EE TÖR + Ariregister (firm-declared asukoht)      |
| Institutional Worker Sizes          | 0.5    | The census reports counts of firms binned by employee-count band per area — a size distribution, not a headcount at a location                                                                              | An area is described only as "250 firms with under 10 staff, 50 firms with 10–25 staff, …", with no fixed location for the jobs                                        | Business-register size-band tabulations           |
| Estimated (census-anchored proxy)   | 0.3    | No direct workplace count; the magnitude is bootstrapped from a real published figure (often a residence-side employment total and/or a coarse grain) then redistributed by an independent workplace signal | A regional employed-population figure is scaled to local areas by population share, then placed onto buildings — grounded in a real number, but not workplace-measured | UA oblast зайняте населення × pop-share → hromada |
| None                                | 0.0    | No census anchor at all, or a residence employment count used verbatim as workplace (workers left where people live, with no redistribution)                                                                | Workers are simply pinned where residents are, or invented with no anchor                                                                                              | —                                                 |

### **Expanded sample surveys**

A household or travel survey that geocodes each respondent's workplace **measures** physical work location — but for a sample, and an expansion-weighted sample is not an enumeration. The top rung requires counting where **each** employee works: however faithfully every surveyed workplace was recorded, the per-area magnitudes of an expanded sample carry sampling error and fieldwork-vintage drift that no re-expansion removes. Score such counts at the **registered (0.7)** rung — matching the reliability class, not the label. The symmetry is deliberate: a register counts **everyone** at the wrong kind of location, an expanded sample measures the **right** kind of location for a fraction of everyone; both sit one rung below full-coverage inferred registers (0.85). _Imagine_ a national travel survey that geocodes home and workplace for two percent of a metro's households and is re-expanded to census population totals: its job magnitudes are estimates however exact each geocode, and the grain credited should likewise be the grain the sample statistically supports (see [Granularity](#2-granularity) rule 4), typically where its expansion is calibrated.

### **Workplace Dasymetric**

Scored via the shared **[Dasymetric](#3-dasymetric)** ladders (R × I). Workplace-specific: intensity is a per-NACE-class **worker** density fit to the workplace ground truth total — see the _Workplace_ rows in [Dasymetric](#3-dasymetric).

## **5. Resident**

_Second most important pillar, with a weight of 0.35 — same **count** × **dasymetric** ([Dasymetric](#3-dasymetric)) structure as Workplace._

### **Resident Count Ground Truth**

| Anchored Resident Count | Weight | Description                                                                     | Illustration                                                                                                                                                                                                                                                                                                                                                                    | Example Sources                                                       |
| :---------------------- | :----- | :------------------------------------------------------------------------------ | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :-------------------------------------------------------------------- |
| Employed Residents      | 1.0    | Counts people who have a job, at their place of **residence**                   | Someone who lives in a suburb and works downtown is counted as one employed resident of the suburb — exactly the origin set that generates commutes                                                                                                                                                                                                                             | JP 国勢調査 employed, PL NSP pracujący, CZ SLDB, LV NPV010, LT S3R532 |
| Working Age Residents   | 0.7    | Counts everyone of working age (e.g. 15–64) living in the area, employed or not | Every 15–64-year-old in the suburb is counted, including the non-commuting — approximate to the commuter pool. The absolute total is somewhat inflated, though relative magnitudes should remain relatively consistent with the actual employed population distribution.                                                                                                        | TW t012 × working-age proxy, EE Eurostat GISCO EMP (15-64)            |
| Total Population        | 0.4    | Counts every resident — children, students, and retirees included               | The whole suburb is counted, so the origin total is materially larger than the actual set of commuters. The absolute total is often two or more times greater than the true employed population count, and the distribution is highly skewed by demographically homogeneous areas (dormitories, retirement communities, etc.) versus the true employed population distribution. | UA                                                                    |
| None                    | 0      | No residence-side count is used                                                 | Residents are inferred with no census anchor                                                                                                                                                                                                                                                                                                                                    | —                                                                     |

### **Resident Dasymetric**

Scored via the shared **[Dasymetric](#3-dasymetric)** ladders (R × I). Resident-specific: intensity is per-building **dwelling** counts or residential floor area (not per-class NACE) — see the _Resident_ rows in [Dasymetric](#3-dasymetric).

## **6. O/D Shaping**

_The lightest pillar (weight 0.15) — see [Criterion](#1-criterion) for why._

Origin-Destination pairs, or O/D matrices of any granularity help shape the map’s fidelity to real-world commute flows.

In the absence of O/D data pairs, a pure gravity-bound model can be used, but this will smooth over macro-level commute flows.

| O/D Metric                        | Weight | Description                                                                                                                                                                                                                                                                               | Illustration                                                                                                                                                                                                                                                                                                                                                                   | Example Sources                                                                                                     |
| :-------------------------------- | :----- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------ |
| Full Matrix (Direct O/D)          | 1.0    | Every home-area → work-area commute flow is counted directly                                                                                                                                                                                                                              | The census reports exactly how many people commute from each town to each other town — every home-town → work-town pair is counted directly, so no flow ever has to be guessed                                                                                                                                                                                                 | CZ ZSJ-díl, PL gmina, JP municipality, LV pagasts (registry-corrected)                                              |
| Structured Marginals              | 0.75   | Not a full matrix, but each area's total out- and in-commuters are known, **plus** how far trips tend to go (same-area / same-region / cross-region shares), or pins to specific destinations (e.g. major cities within the country)                                                      | The census informs that 40% of a town's workers stay local, 40% go elsewhere in the county and 20% leave the county — and that 5,000 specifically commute to the capital — enough to rebuild realistic flows without naming every pair                                                                                                                                         | TW (distance bins + county pins), EE (distance bins + Tallinn/Tartu pins), HU (county×county OD + containment bins) |
| Marginal O/D                      | 0.5    | Only each area's total out- and in-commuters are known — no sense of distance or direction                                                                                                                                                                                                | The census informs that a town sends 10,000 commuters out and takes 3,000 in, but not where any of them go or how far                                                                                                                                                                                                                                                          | —                                                                                                                   |
| Synthetic from measured marginals | 0.25   | No commute flow data, but both marginals are real **and a synthetic matrix can be doubly-constrained** (IPF-bounded), so the flows reproduce the measured origin and destination totals — the marginals genuinely constrain the output                                                    | The census gives each town's real job count and its real number of working residents, but no flows. A gravity model guesses the flows, then nudges them until each town's guessed departures add back up to its working-resident total and its guessed arrivals add back up to its job total — every individual flow is invented, but the town totals they sum to are all real | LT (measured workplace + employed-resident totals)                                                                  |
| Prior-informed synthetic          | 0.1    | No commute flow data and the marginals themselves are synthetic, but the flows are **IPF-bounded to those targets with an informed prior** — a distance-decay and containment kernel calibrated from real commute data in analogue countries — not an unconstrained gravity default model | Here even each town's job and resident totals are estimated, not measured. A gravity model guesses the flows, tuned with real commute patterns from similar countries — how fast commuting falls off with distance, and how many people work in their own town — so the invented flows behave realistically, and are still nudged to add up to those estimated town totals     | UA (synthetic-GIPF; priors calibrated on PL / JP / TW measured OD)                                                  |
| None / unbounded gravity          | 0.0    | No commute flow data and no bounding — a pure, unconstrained gravity model whose flows are bound by no marginal (even when the input masses are real), or flows from a coarse estimate, or nothing at all                                                                                 | No commute information is available at all. A plain gravity model assumes only that more jobs and shorter distance mean more commuters; the guessed flows are never checked against any town's real worker or job total, so a town's flows can add up to more (or fewer) people than actually live or work there                                                               | MA (no marginal)                                                                                                    |

_The Full Matrix rung is reserved for flows **counted by enumeration**. A **sampled** O-D matrix — a travel survey that records each respondent's origin and destination — observes flow structure but does not count it: however complete its pairwise coverage, it scores **Structured Marginals** at the granularity its sample statistically supports (the same principle as [Expanded sample surveys](#4-workplace) on the count axis). Where sampled structure covers only part of a map and the rest is constructed from derived marginals, the blend drops a further rung (Marginal O/D) — the reliable information the whole map shares is the marginal totals._

_Note that a pipeline at the **0.0 rung that has measured marginals** (real resident + job totals per area) is 0.0 **as-built, but 0.25-capable**. With an IPF / doubly-constrained pass over its existing marginals, the pipeline can be lifted to 0.25 with no new data, meaning the gap is a modeling choice, not a data limitation. A pipeline whose marginals are **fabricated** (such as UA) cannot reach 0.25 without acquiring real marginals; one with **no marginals at all** (e.g. MA) cannot reach 0.1 without acquiring a prior._

## **7. Example Countries**

_Granularity columns record the finest grain at which the count metric is **measured** (not the grain it is modeled onto)._

| CC            | Author   | w_count                                     | w_dasymetric                                                     | w_granularity                             | r_count                                    | r_dasymetric                                       | r_granularity                              | od_metric                                           | od_granularity      |
| :------------ | :------- | :------------------------------------------ | :--------------------------------------------------------------- | :---------------------------------------- | :----------------------------------------- | :------------------------------------------------- | :----------------------------------------- | :-------------------------------------------------- | :------------------ |
| CZ            | Yukina-  | physical                                    | native building use + NACE fit                                   | ADM4 (ZSJ-díl)                            | employed_resident                          | census mesh 100m + dwelling                        | ADM3 (obec)                                | Full                                                | ADM4 (ZSJ-díl)      |
| EE            | Yukina-  | register (self-declared)                    | native/exact building use + NNLS                                 | ADM3 (omavalitsus)                        | employed_resident (15-64)                  | exact building (dwelling units)                    | Mesh (1km)                                 | Structured marginals                                | ADM3 (omavalitsus)  |
| HU            | Yukina-  | physical (KSH census place-of-work)         | Overture / GULU building use + NACE fit                          | ADM3 (település)                          | employed_resident (KSH + cadastre)         | cadastre parcels + Overture                        | ADM3 (település)                           | Structured marginals (county OD + containment bins) | ADM3 (település)    |
| JP            | Yukina-  | physical                                    | census mesh (500m → 100m FA)                                     | Mesh (500m)                               | employed_resident                          | census mesh (250m)                                 | ADM4 (町丁目)                                 | Full                                                | ADM3 (市区町村)         |
| LT            | Yukina-  | physical                                    | native building use + measured density                           | ADM3 (savivaldybė)                        | employed_resident (census ACT_employees)   | census mesh (250m)                                 | ~ADM4 (seniūnija; miesto cores whole-city) | Synthetic (measured margins)                        | N/A                 |
| LV            | Yukina-  | physical (register-derived)                 | native building use + DPA                                        | ADM4 (pagasts)                            | employed_resident                          | mesh (100m urban / 200m rural) + DPA               | ADM4 (pagasts)                             | Full (registry-corrected)                           | ADM4 (pagasts)      |
| PL            | Yukina-  | physical                                    | native building use + Tikhonov fit                               | ADM3 (gmina)                              | employed_resident (anchored to ADM3 gmina) | mesh (125m urban / 250m)                           | ADM3 (gmina)                               | Full                                                | ADM3 (gmina)        |
| SK            | Yukina-  | physical (SODB census place-of-work)        | native building cadastre (ZBGIS) + NNLS fit                      | ADM3 (obec)                               | employed_resident (SODB)                   | native ZSJ (measured pop)                          | ADM3 (obec)                                | Full (obec×obec census)                             | ADM3 (obec)         |
| TW            | Yukina-  | physical                                    | native building use + Tikhonov fit                               | ADM3 (鄉鎮市區)                               | working_age (anchored to ADM3 employed)    | census mesh (MSA sub-里, ADM5 measured pop)         | ADM4 (里)                                   | Structured marginals                                | ADM3 (鄉鎮市區)         |
| UA            | Yukina-  | estimated (census proxy)                    | Overture / GULU building use + prior estimated workplace density | ADM1 (область)                            | total_population                           | Overture / GULU bottom-up ~100m raster             | ADM3 (громада)                             | Prior-informed (GIPF)                               | N/A                 |
| PE            | kai      | registry (self-declared)                    | manzana registry (urban) + Overture land-use/POI hex (rural)     | ~ADM4 (manzana urban / zona censal rural) | total_population                           | measured per manzana (block)                       | ADM5 (manzana)                             | None (unbounded gravity)                            | N/A                 |
| CN            | Kronifer | physical (5th Economic Census 从业人员)         | commercial/residential volume over OSM + Overture + 3D-GloBFP    | ADM4 (街道)                                 | total_population                           | residential volume over OSM + Overture + 3D-GloBFP | ADM4 (街道)                                  | None (unbounded gravity)                            | N/A                 |
| ES            | Max02    | estimated (census proxy)                    | OSM/Overture hybrid footprints + generic type weights            | ADM3 (municipio)                          | working_age                                | OSM/Overture hybrid footprints + generic weights   | ADM3 (municipio)                           | None (single-pass capacity gravity)                 | N/A                 |
| MX            | slurry   | physical (DENUE estab. + CE totals, banded) | establishment points + size-band split                           | ~ADM4 (municipio total → establishments)  | working_age                                | measured block population                          | ADM5 (Manzana)                             | None (unbounded gravity)                            | N/A                 |
| NO            | slurry   | physical                                    | measured in-unit                                                 | Mesh (250m)                               | total_population                           | measured in-unit                                   | Mesh (250m)                                | None (unbounded gravity)                            | N/A                 |
| PR            | slurry   | registry (self-declared)                    | Overture footprints + calibrated 8-class job density             | ADM2 (municipio)                          | employed_resident                          | ADM5 working-age blocks + Overture                 | ~ADM4 (ADM2 employed × ADM5 working-age)   | None (unbounded gravity)                            | N/A                 |
| US            | slurry   | physical (inferred, LODES WAC)              | measured in-unit (block)                                         | ADM5 (census block)                       | employed_resident (LODES RAC)              | measured in-unit (block)                           | ADM5 (census block)                        | Full (LODES OD)                                     | ADM5 (census block) |
| OSM (patcher) | —        | none (OSM floor-area heuristic, no census)  | OSM footprints × levels (uncalibrated sqft/job)                  | None                                      | none (OSM floor-area heuristic, no census) | OSM footprints × levels (uncalibrated sqft/person) | None                                       | None (synthetic jobs% × pop)                        | N/A                 |

## **8. Composite Score**

Every pipeline resolves to a single **weighted score** in [0, 1] and a named quality tier; the rest of this section defines how that score is built.

| Tier          | Grade | Weighted Score |
| :------------ | :---: | :------------- |
| **Very high** |   A   | ≥ 0.75         |
| **High**      |   B   | 0.60 – 0.75    |
| **Medium**    |   C   | 0.45 – 0.60    |
| **Low**       |   D   | 0.30 – 0.45    |
| **Very low**  |   E   | 0.15 – 0.30    |
| **Absent**    |   F   | < 0.15         |
| **Unknown**   |   U   | N/A            |

_Tiers apply to the **weighted** (player-facing) score, and refine the current three-level tag system: **very high** and **high** map to `high`, **medium** to `medium`, and **low** / **very low** / **absent** to `low`._

_The **absent** tier (grade F) marks pipelines whose data does not effectively anchor the model. Most arrive with no usable census anchor at all — the granularity multiplier floors their score at ~0 by construction — but a pipeline with a real yet minimal anchor (a single region-wide total, an assumed employment ratio) can also fall below the very-low floor on score alone. Absent means the official data does not meaningfully constrain the model — not that none was consulted._

_The **unknown** tier (grade U) is reserved for pipelines that have not been scored yet, to enable backwards compatibility for pipelines/maps that have yet to be scored._

_Tiers are heterogeneous — a given tier can arise from different strength/weakness profiles across the three pillars. The **grade** letter alias (A–F) is kept for brevity._

The **raw composite score** combines the three scored pillars from the tables above. Each pillar multiplies its **count ground truth**, its **dasymetric** (`R × I`, [Dasymetric](#3-dasymetric)), and a **granularity multiplier** `G` for the grain at which that pillar's count is measured. The pillars are then weighted with **workplace > resident > O/D** as described in [Criterion](#1-criterion). The raw score is a number in [0, 1].

```text
workplace_raw = count_w × (R_w × I_w) × G(gran_w)
resident_raw  = count_r × (R_r × I_r) × G(gran_r)
od_raw       = score_od           × G(gran_od)

raw_score = 0.50 · workplace_raw  +  0.35 · resident_raw  +  0.15 · od_raw
```

Given that factors are in [0, 1], the _raw score_ is in [0, 1]. The product form inside each pillar is **weakest-link**: a strong count spread by a weak dasymetric — or any pillar anchored at a coarse grain — is penalized, which is the intent for an internal-review metric.

A second score, the **weighted composite score**, replaces the product inside each pillar with a half-and-half average — `pillar_weighted = (0.5 · count + 0.5 · R·I) × G` — which is more forgiving of a single weak sub-factor and yields a more spread-out, player-facing scale. Note that O/D has no split, so it stays `score_od × G` in both.

Both composite scores combine pillars with the same `0.50 · workplace + 0.35 · resident + 0.15 · od` weights; the table reports both.

**Granularity multiplier (G)** — read off the Granularity ladder:

| Grain                                          | G    |
| :--------------------------------------------- | :--- |
| Mesh — very-fine, uniform (≤125 m)             | 1.00 |
| Mesh — fine (≤250 m)                           | 0.95 |
| Mesh — medium (≤500 m)                         | 0.90 |
| Mesh — coarse (≤1 km)                          | 0.85 |
| Mesh — very-coarse (>1 km)                     | 0.65 |
| Measured-pop ADM5 unit (census block / sub-里) | 0.95 |
| Sub-municipal (ADM4)                           | 0.90 |
| Municipal (ADM3)                               | 0.70 |
| Regional (ADM2)                                | 0.50 |
| Provincial (ADM1)                              | 0.30 |
| None                                           | 0.00 |

_Mesh tiers are graded by **cell size in meters** — a uniform grid gives consistent resolution everywhere (especially in rural areas). Admin tiers are rated by their **typical** size, which varies widely and is usually far larger than the tier name implies: gminy are often 100+ km², obce / city-districts routinely > 1 km². So a uniform 1 km grid (0.85) outscores a municipality (ADM3, 0.70), and the two ladders are **not** equivalent. A measured-population ADM5 unit (census block, sub-里) is the exception: it is small and measured, so it sits with the fine-mesh tier._

**Sample scores from known pipelines:**

| CC            | Author   | Workplace | Resident | O/D  | Raw Composite | Weighted Composite | Tier      |
| :------------ | :------- | :-------- | :------- | :--- | :------------ | :----------------- | :-------- |
| US            | slurry   | 0.73      | 0.86     | 0.95 | 0.81          | **0.87**           | Very high |
| LV            | Yukina-  | 0.65      | 0.81     | 0.90 | 0.74          | **0.82**           | Very high |
| JP            | Yukina-  | 0.77      | 0.77     | 0.70 | 0.76          | **0.81**           | Very high |
| CZ            | Yukina-  | 0.77      | 0.63     | 0.90 | 0.74          | **0.78**           | Very high |
| PL            | Yukina-  | 0.60      | 0.63     | 0.70 | 0.62          | **0.66**           | High      |
| SK            | Yukina-  | 0.60      | 0.63     | 0.70 | 0.62          | **0.66**           | High      |
| LT            | Yukina-  | 0.60      | 0.77     | 0.25 | 0.60          | **0.65**           | High      |
| TW            | Yukina-  | 0.60      | 0.57     | 0.53 | 0.57          | **0.65**           | High      |
| EE            | Yukina-  | 0.42      | 0.85     | 0.53 | 0.58          | **0.65**           | High      |
| NO            | slurry   | 0.81      | 0.32     | 0.00 | 0.52          | **0.65**           | High      |
| MX            | slurry   | 0.63      | 0.60     | 0.00 | 0.52          | **0.65**           | High      |
| HU            | Yukina-  | 0.42      | 0.49     | 0.53 | 0.46          | **0.57**           | Medium    |
| PE            | kai      | 0.34      | 0.34     | 0.00 | 0.29          | **0.50**           | Medium    |
| PR            | slurry   | 0.21      | 0.49     | 0.00 | 0.27          | **0.40**           | Low       |
| CN            | Kronifer | 0.25      | 0.10     | 0.00 | 0.16          | **0.40**           | Low       |
| ES            | Max02    | 0.09      | 0.21     | 0.00 | 0.12          | **0.26**           | Very low  |
| UA            | Yukina-  | 0.04      | 0.12     | 0.10 | 0.08          | **0.17**           | Very low  |
| OSM (patcher) | —        | 0.00      | 0.00     | 0.00 | 0.00          | **0.00**           | Absent    |
| _Unscored_    | —        | —         | —        | —    | —             | N/A                | Unknown   |

The raw (weakest-link product) column is retained as the sterner internal view.

_OSM (patcher): the `Subway-Builder-Modded/map-manager` pipeline (an early community OSM patcher), scored as the true floor of the scale. It uses **no census or official statistics at all** — resident and job counts are derived entirely from OSM building footprints × `building:levels`, multiplied by hardcoded square-feet-per-person / square-feet-per-job constants (the source comments call it "all vibes"). Neighborhoods are Voronoi cells around OSM place-nodes, and the O/D "matrix" is a synthetic jobs-share × population product with no distance decay. Because no count magnitude is ever **measured**, the granularity multiplier is **None (G = 0)**, which zeroes every pillar regardless of how the OSM footprints are placed — so it scores **0.00**. It is the concrete illustration of what "no census bounding" costs: with no real magnitude to place, spatial detail is worthless._

_Initially, all pipelines are **unscored** (grade U) until a reviewer has worked with teh submitter to define the three pillars and the O/D metric. The unscored tier is a placeholder and is not at all a judgment of the map's data quality._

### **Worked examples — one per tier**

Each country is shown with both composites: **raw** (weakest-link `count × (R·I) × G`) and **weighted** (half-and-half `(0.5·count + 0.5·R·I) × G`, the graded number). O/D has no split — it is `score_od × G` in both. Pillars combine as `0.50 · workplace + 0.35 · resident + 0.15 · od`.

- **Very high — JP.** Physical 経済センサス worker mesh (500 m) · employed residents (250 m mesh, ADM4) · full municipal O/D matrix (ADM3).
  - _Raw_ — workplace `1.0 × (0.85×1.0) × 0.90 = 0.765`, resident `= 0.765`, od `1.0 × 0.70 = 0.70` → `0.50·0.765 + 0.35·0.765 + 0.15·0.70` = **0.76**.
  - _Weighted_ — workplace `0.925 × 0.90 = 0.83`, resident `= 0.83`, od `0.70` → `0.50·0.83 + 0.35·0.83 + 0.15·0.70` = **0.81**.
- **High — TW.** Physical count on NLSC cadastre + calibrated fit (municipal) · working-age on MSA sub-里 measured-pop blocks · structured marginals (ADM3).
  - _Raw_ — workplace `1.0 × (1.0×0.85) × 0.70 = 0.595`, resident `0.7 × (0.9×1.0) × 0.90 = 0.567`, od `0.75 × 0.70 = 0.525` → **0.57**.
  - _Weighted_ — workplace `0.925 × 0.70 = 0.648`, resident `0.80 × 0.90 = 0.720`, od `0.525` → **0.65**.
- **Medium — HU.** KSH census place-of-work on Overture/GULU footprints + NACE fit (ADM3) · employed residents on cadastre parcels + Overture (ADM3) · structured marginals (county O/D + containment bins, ADM3).
  - _Raw_ — workplace `1.0 × (0.7×0.85) × 0.70 = 0.417`, resident `1.0 × (0.7×1.0) × 0.70 = 0.49`, od `0.75 × 0.70 = 0.525` → **0.46**.
  - _Weighted_ — workplace `0.798 × 0.70 = 0.558`, resident `0.85 × 0.70 = 0.595`, od `0.525` → **0.57**.
  - _(LT, the previous Medium exemplar, moved to High after its 2021-census seniūnija rescore: resident grain ~ADM4 lifted its weighted composite 0.59 → 0.65.)_
- **Low — CN.** Physical 5th Economic Census count on ML footprints (binary commercial/residential split, 街道 / ADM4) · total population (same footprints & grain) · no O/D (unbounded gravity).
  - _Raw_ — workplace `1.0 × (0.7×0.4) × 0.90 = 0.252`, resident `0.4 × (0.7×0.4) × 0.90 = 0.101`, od `0.00` → **0.16**.
  - _Weighted_ — workplace `0.64 × 0.90 = 0.576`, resident `0.34 × 0.90 = 0.306`, od `0.00` → **0.40**.
- **Very low — UA.** Estimated proxy on ML footprints (oblast grain) · total population (hromada grain) · prior-informed synthetic O/D (GIPF calibrated on analogue flows).
  - _Raw_ — workplace `0.3 × (0.7×0.6) × 0.30 = 0.038`, resident `0.4 × (0.7×0.6) × 0.70 = 0.118`, od `0.10` → **0.08**.
  - _Weighted_ — workplace `0.36 × 0.30 = 0.108`, resident `0.41 × 0.70 = 0.287`, od `0.10` → **0.17**.
- **Absent — OSM patcher.** No census anchor anywhere, so `G = 0` zeroes every pillar regardless of footprint placement.
  - _Raw_ — workplace `0.0 × (0.6×0.2) × 0 = 0.00`, resident `= 0.00`, od `0.00` → **0.00**.
  - _Weighted_ — workplace `(0.5·0.0 + 0.5·0.13) × 0 = 0.00`, resident `= 0.00`, od `0.00` → **0.00**.

## **9. Unscored Metrics**

These considerations are not part of the score, but they meaningfully affect how much a map can be trusted and should be mentioned wherever they apply. They are listed in rough descending order of impact — from those that can materially shift modeled demand, through a bias worth flagging, down to a common point of confusion that is not a quality signal at all.

### **Absolute Anchor**

Each Subway Builder map is a closed-system model of a real-world open system. Maps within the simulation cannot render any points outside of a certain boundary, whereas in real life any map bounds would most certainly be porous.

The absolute anchor of the map is which total (employed residents / workplaces) the map is scaled towards, and whether or not the map models those workers who live outside of the boundary, or those residents who work outside of the boundary.

Choosing to model only those persons who both live and work within the map boundary is a design choice, but it should be noted that this will reduce the relative magnitude of modeled commute flows and may skew against nodes located at the periphery.

### **Special Demand — a separate layer, outside the normal modeled flow**

The three scored pillars cover **commute** demand — the normal home → work flow the map models. Most pipelines also inject **special-demand** points that are deliberately **not** part of that normal modeled flow: attractions (museums, temples, stadiums, ski resorts, etc.), hospitals, airports, ports and other generators of unique trips.

These are generally modeled using a visitor or throughput magnitude as an analogue to commuter traffic. Because it is a different kind of demand, and is net-optional for a submitted map, this layer is deliberately **not** scored.

A map with a well-cited special-demand layer is materially better than one that invents attraction weights, even when the two score identically on the commute pillars, but that distinction is not captured in this rubric.

### **Data Vintage & Temporal Coherence**

Some census / official data is of COVID-vintage, wherein O/D is lower than normal, and WFH (work from home) rates are higher. This rubric does not attempt to penalize COVID-vintage data, as even though it may be less representative of the pre-COVID world, it is still a real-world measurement.

Separately, a single map can be assembled from layers of **different vintages**. For example, a reasonable methodology may include a residence census, a workplace / economic census usually published a year or more later, a current building-footprint snapshot, and a special-demand layer whose figures may span many years. This modest, systematic lag between the residence and workplace censuses is normal and accepted (the two are rarely published concurrently).

The looser risk is the special-demand layer, which is left primarily to the discretion of the mapper and may lack any vintage constraint whatsoever; a single map can blend a pre-pandemic tourism peak, a recent municipal survey, and an undated estimate side by side. Special demand does not inherently factor into this rubric's final score, so this risk is noted rather than scored.

### **Informal Employment**

Some census data attempts to capture the full breadth of employment within a country by estimating informal employment (or garnering such data via survey). Herein lies a potential source of bias in the data, as informal employment is often more prevalent in certain sectors (e.g. construction, agriculture) and may be more spatially diffuse than formal employment, and certain countries will exhibit much higher rates of informal employment than others.

This rubric does not attempt to score informal employment, as modeling it explicitly is a design choice, but it is worth noting that a map that does not model informal employment may be less realistic than one that does.

### **Point-Placement Detail — distinct from input quality**

How finely a map ultimately places demand points is a performance / UX choice, **not** a measure of input-data quality, and the two must not be conflated. A generator may agglomerate, cull, or merge fine-grained input into fewer rendered points for engine performance without lowering its score as the ground-truth input is unchanged.

Two examples of this include the US demand generator's agglomeration mechanic, wherein multiple LODES RAC points are merged into a single point for the purposes of in-simulation rendered quality/performance, and the shared EU/JP/TW pipeline's merging of runt/residual points below a certain population threshold in the densest areas of each map.

## **10. Credits**

This scoring system was developed by _Yukina-_ for the Subway Builder Modded organization, and is a living document. Input from the community will be incorporated into this system as the map ecosystem and modeling practices evolve.
