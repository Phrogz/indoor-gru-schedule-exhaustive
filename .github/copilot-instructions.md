# Tournament Schedule Generator - AI Assistant Guide

## Project Overview

Generates optimal tournament schedules for N teams over multiple weeks, minimizing "double byes" (gaps of 3 slots between games) and "five-slot spans" (games spread across 5+ slots). Uses exhaustive enumeration with worker threads for parallelization.

## Key Concepts

### Data Encoding

- **Teams**: Indices 0 to N-1, displayed as letters A-P (max 16 teams)
- **Matchups**: Encoded as indices 0 to N×(N-1)/2-1, representing all unique team pairs
  - For 6 teams: 0=AvB, 1=AvC, 2=AvD, 3=AvE, 4=AvF, 5=BvC, 6=BvD, 7=BvE, 8=BvF, 9=CvD, 10=CvE, 11=CvF, 12=DvE, 13=DvF, 14=EvF
- **Slots**: Time slots in a week; N_SLOTS = (N_TEAMS × 3) / 2 (each team plays 3 games)
- **Patterns**: Valid 3-slot combinations within span constraints (shapes 4 or 5)

### Scoring

Schedules scored by `[doubleByes, fiveSlotTeams]` tuple (lower is better):

- `doubleByes`: Count of 3-slot gaps between consecutive games for a team
- `fiveSlotTeams`: Count of teams whose games span 5+ slots

### Round-Robin Rules

A "round" consists of all N×(N-1)/2 matchups (each pair plays exactly once). For 6 teams, that's 15 matchups.

**Within any week:**

1. Each team plays exactly 3 games
2. Each team's games must span at most 5 slots
3. No matchup can occur more than once in the same week (same two teams can't play twice)

**Cross-week round tracking:**

- Weeks may span across rounds (e.g., 6 teams × 5 weeks = 45 games = 3 complete rounds)
- The tournament typically ends with an incomplete round (this is normal)
- `excludeMatchups`: Matchups already used in the current round cannot be reused until the round completes
- `requiredMatchups`: When remaining games in round ≤ N_SLOTS, those unused matchups must appear somewhere in the week

**⚠️ CRITICAL: Slot order is NOT constrained by round boundaries.**
When a week straddles two rounds, games from round N+1 may appear in ANY slot—including BEFORE games that complete round N. The only requirement is that all `requiredMatchups` appear somewhere in the week (any slot position). There is NO `roundBoundarySlot` concept—do not partition slots by round membership.

**Example: 6 teams, week 1 with interleaved rounds:**

- Week 0 used matchups: {0,2,3,6,8,9,10,11,14} (9 of 15 in round 0)
- Required to complete round 0: {1,4,5,7,12,13} (6 matchups, must appear in week 1)
- Valid week 1: `[1,0,5,4,9,7,13,14,12]`
  - Matchups 0,9,14 at slots 1,4,7 are round 1 games (reusing week 0's matchups)
  - Matchups 1,5,4,7,13,12 complete round 0
  - Round 1 games appear BEFORE some round 0 games—**this is valid!**

**Multi-week example: 6 teams, 5 weeks (45 games = 3 rounds)**

- Week 0: 9 games toward round 0
- Week 1: 6 games complete round 0, 3 games start round 1 (interleaved in any order)
- Week 2: 9 games toward round 1
- Week 3: 6 games complete round 1, 3 games start round 2 (interleaved in any order)
- Week 4: 9 games toward round 2

### Output Formats

All results use **streaming text format** (`.txt`) for memory efficiency:

- **1-week files** (`results/{N}teams-1week.txt`): One schedule per line, comma-separated matchup indices
- **Multi-week files** (`results/{N}teams-{M}weeks.txt`): Tab-indented depth-first tree traversal

**Reading results:**

```javascript
import { createReadStream } from 'fs';
import { createInterface } from 'readline';

// Stream line-by-line (memory efficient for large files)
const rl = createInterface({ input: createReadStream('results/6teams-1week.txt') });
for await (const line of rl) {
  const schedule = line.split(',').map(Number);
  // process schedule...
}

// Or read entire file (small files only)
import { readFileSync } from 'fs';
const schedules = readFileSync('results/6teams-1week.txt', 'utf8')
  .trim().split('\n')
  .map(line => line.split(',').map(Number));
```

## Reference Schedule: 6 Teams, 5 Weeks

Validated schedule from [best-results](https://github.com/Phrogz/PerfectGRUIndoorSchedule/blob/main/best-results/6teams-3games-5weeks-5slotsmax-options.txt):

```txt
Week 0: [4,2,13,3,8,9,7,10,5]    # AvF AvD DvF AvE BvF CvD BvE CvE BvC
Week 1: [1,4,11,0,9,14,6,7,12]   # AvC AvF CvF AvB CvD EvF BvD BvE DvE
Week 2: [13,6,8,2,14,5,3,1,10]   # DvF BvD BvF AvD EvF BvC AvE AvC CvE
Week 3: [7,3,0,12,5,4,9,13,11]   # BvE AvE AvB DvE BvC AvF CvD DvF CvF
Week 4: [14,11,10,8,12,1,6,0,2]  # EvF CvF CvE BvF DvE AvC BvD AvB AvD
```

Grid view (Week 0):

```txt
     0 1 2 3 4 5 6 7 8
  A: F D . E . . . . .
  B: . . . . F . E E C
  C: . . . . . D . E B
  D: . A F . . C . . .
  E: . . . . . . B C .
  F: A . D . B . . . .
```

## Entry Points

| Script | Purpose | Example |
|--------|---------|---------|
| `calculate.mjs` | Schedule enumeration (1+ weeks) | `node calculate.mjs --teams=6 --weeks=3` |
| `display.mjs` | View saved results | `node display.mjs results/8teams-2weeks.txt --limit 5` |

### CLI Flags

- `--teams=N`: Number of teams (must be even, max 16)
- `--weeks=N`: Number of weeks to generate (parallel version requires ≥2)
- `--workers=N`: Number of worker threads (defaults to CPU count)
- `--validate`: Run without reading/writing files (test enumeration only)
- `--debug`: Enable verbose logging

## Architecture

### Backtracking Enumeration (`enumerateSchedules`)

Core algorithm in `calculate.mjs`:

1. Fills slots left-to-right, trying all valid matchups
2. Uses BigInt bitmasks for pattern validity (`validMasks[]`)
3. Prunes when teams can't complete 3 games within span constraints
4. Tracks `requiredMatchups` to ensure round-robin completion

### Round-Robin Constraints (`getRoundConstraints` + `updateRoundMatchups`)

When spanning multiple weeks, ensures each pair plays exactly once per round:

- Uses `roundMatchups: Map<roundNum, Set<matchupIdx>>` to track which matchups belong to which round
- `excludeMatchups`: Bitmask of matchups used in current round (only applied when entire week fits in one round)
- `requiredMatchups`: Matchups that must appear (when remaining games in round ≤ N_SLOTS)
- `updateRoundMatchups()`: After placing a week, assigns matchups to correct rounds (required matchups complete current round, extras start new round)

### Parallelization Strategy

- Main thread generates all optimal week-0 schedules

- Chunks distributed to workers; each explores all continuations
- Workers report `optimal` results incrementally via `parentPort.postMessage`
- Final results aggregated and saved with tree structure

## Constraints & Gotchas

1. **Team limit**: TEAMS string only has 16 letters; N_TEAMS > 16 fails
2. **Week count**: `calculate.mjs` requires `N_WEEKS >= 1`
3. **Memory**: Multi-week results can be huge; use streaming `.txt` format for large runs
4. **Pattern masks**: Use BigInt (not Number) since pattern count can exceed 32

## Development Workflow

```bash
# Generate and save 1-week schedules (creates results/{N}teams-1week.txt)
node calculate.mjs --teams=6 --weeks=1

# Generate multi-week (loads prior results, saves {N}teams-{M}weeks.txt)
node calculate.mjs --teams=8 --weeks=2

# Validate without file I/O
node calculate.mjs --teams=8 --weeks=2 --validate

# View results with grid display
node display.mjs results/8teams-2weeks.txt --limit 3
```
