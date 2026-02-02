# Ideal GRU Week Schedules

Pre-computed optimal tournament schedules for indoor Ultimate leagues, stored as
exhaustively-explored results. Exists because the
[PerfectGRUIndoorSchedule](https://github.com/Phrogz/PerfectGRUIndoorSchedule)
generator uses a round-robin algorithm that excludes certain matchups, and—for
8-team schedules—forces schedules to allow 5-slot spans in order to find _any_
legal options before scoring them.

This repository instead explores the full space of legal schedules **without**
those constraints, finding schedules with better scores (fewer double-byes and
5-slot spans).

## Running

```bash
# Generate schedules for N teams over M weeks
node calculate.mjs --teams=8 --weeks=3

# View saved results
node display.mjs results/8teams-3weeks.txt --limit=5
```

### Pragmatic Generation for Large Search Spaces

For 8 teams, the search space grows exponentially with weeks:

- 1 week: ~1 second
- 2 weeks: ~10 seconds  
- 3 weeks: ~6 minutes
- 4 weeks: **~40 weeks** (estimated)

A pragmatic approach for week 4+: stop 3-weeks early after accumulating "enough"
optimal schedules, then start week 4 searching through those partial results.
The generator automatically resumes from prior results, so you can interrupt and
restart as needed.

## Display Output

The `display.mjs` script shows schedules in a readable grid format:

```
$ node display.mjs results/8teams-4weeks.txt --limit=1

File: results/8teams-4weeks.txt
Teams: 8 (ABCDEFGH), Slots: 12
Schedules in file: 149192

=== Schedule 1 of 149192 ===
Week 0: AvB AvE BvE AvG BvG DvE CvG DvH DvF CvH CvF FvH
  A: B E . G . . . . . . . .
  B: A . E . G . . . . . . .
  C: . . . . . . G . . H F .
  D: . . . . . E . H F . . .
  E: . A B . . D . . . . . .
  F: . . . . . . . . D . C H
  G: . . . A B . C . . . . .
  H: . . . . . . . D . C . F
...
```

Options:
- `--limit=N` — Show at most N schedules (default: 10, 0 for all)
- `--matchups` — Show matchup indices instead of team letters
- `--no-grid` — Hide the slot grid, show only matchup summary

## Motivation

While the original PerfectGRUIndoorSchedule provides good schedules, its
pre-filtering approach (see its `options/` directory) eliminates some potentially
optimal configurations:

1. **Round-robin constraints** force matchups into specific weeks based on a
   fixed rotation algorithm
2. **5-slot span allowance** was required for 8 teams to find _any_ legal
   schedules within those constraints

By exploring from scratch without these constraints, we can find schedules that:
- Have fewer total double-byes (3-slot gaps between a team's games)
- Have fewer teams with 5-slot spans (games spread across 5+ time slots)

### Scoring

Schedules are scored by `[doubleByes, fiveSlotTeams]` tuples (lower is better):
- **doubleByes**: Total count of 3-slot gaps between consecutive games across
  all teams in all weeks
- **fiveSlotTeams**: Total count of team-weeks where a team's games span 5 slots

## Results File Format

Results are stored in `results/` as tab-indented text files representing a
depth-first tree traversal. Each complete path through the tree represents one
multi-week schedule.

### Header

```
# teams=8 weeks=4 score=8,16 count=149184 (partial)
```

- `teams`: Number of teams
- `weeks`: Number of weeks in each schedule
- `score`: Best score found `[doubleByes,fiveSlotTeams]`
- `count`: Number of complete schedules
- `(partial)`: Present if enumeration was interrupted

### Body Structure

```
0,3,9,5,11,18,16,21,19,17,15,26          # Week 0 schedule A
	23,27,24,25,22,6,10,1,2,7,8,13        # Week 1 following A
		14,1,22,3,15,4,8,12,20,11,21,27   # Week 2 following A→B
			0,1,7,2,8,17,20,24,26,23,25,22 # Week 3 (complete path 1)
			0,1,7,2,8,17,20,26,24,25,23,22 # Week 3 (complete path 2)
```

Each line is a comma-separated list of **matchup indices** representing the
games played in order during that week. Tab indentation indicates week depth
(0 tabs = week 0, 1 tab = week 1, etc.).

### Matchup Encoding

Each number represents a game between two teams. The indices are assigned by
iterating through all unique team pairs in order:

```
 0    1    2    3    4    5    6
AvB AvC AvD AvE AvF AvG AvH
     7    8    9   10   11   12
    BvC BvD BvE BvF BvG BvH
        13   14   15   16   17
        CvD CvE CvF CvG CvH
            18   19   20   21
            DvE DvF DvG DvH
                22   23   24
                EvF EvG EvH
                    25   26
                    FvG FvH
                        27
                        GvH
```

So a week schedule like `0,3,9,5,11,18,16,21,19,17,15,26` means:
- Slot 0: matchup 0 (AvB)
- Slot 1: matchup 3 (AvE)
- Slot 2: matchup 9 (BvE)
- ...and so on

### Incomplete Markers

When generation is interrupted, incomplete branches are marked with `…`:

```
0,3,9,5,11,18,16,21,19,17,15,26
	23,27,24,25,22,6,10,1,2,7,8,13
		…                                 # Enumeration interrupted here
```

The generator can resume from these markers.
