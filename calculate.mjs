// Parallel Tournament Schedule Generator
// Uses worker threads to parallelize multi-week enumeration

import { Worker, isMainThread, parentPort, workerData } from 'worker_threads';
import { cpus } from 'os';
import { fileURLToPath } from 'url';
import { writeFileSync, readFileSync, existsSync, mkdirSync, unlinkSync } from 'fs';
import { createReadStream } from 'fs';
import { createInterface } from 'readline';
import { TreeWriter, TreeReader, parseHeader } from './lib/tree-format.mjs';

// Parse command-line args (only used in main thread)
function parseArgs() {
  const args = process.argv.slice(2);

  // Check for help flag first (only in main thread)
  if (isMainThread && (args.includes('--help') || args.includes('-h'))) {
    const defaultWorkers = Math.max(1, cpus().length - 2);
    console.log(`
Tournament Schedule Generator - Parallel Enumeration

Usage: node schedule-parallel.mjs [options]

Options:
  --teams=N     Number of teams (must be even, 4-16, default: 8)
  --weeks=N     Number of weeks to generate (default: 2)
  --workers=N   Number of worker threads (default: ${defaultWorkers}, which is cpus-2)
  --validate    Run without reading/writing files (test enumeration only)
  --debug       Enable verbose logging
  --help, -h    Show this help message

Examples:
  node schedule-parallel.mjs --teams=6 --weeks=3
  node schedule-parallel.mjs --teams=8 --weeks=4 --workers=8
  node schedule-parallel.mjs --teams=6 --weeks=5 --validate

Output files are saved to results/{N}teams-{W}week(s).txt
Prior results are automatically loaded to resume from the best available checkpoint.
`);
    process.exit(0);
  }

  // Default to leaving 2 cores free for OS responsiveness (minimum 1 worker)
  const defaultWorkers = Math.max(1, cpus().length - 2);
  let teams = 8, weeks = 2, debug = false, workers = defaultWorkers, validate = false;
  for (const arg of args) {
    if (arg.startsWith('--teams=')) teams = parseInt(arg.slice(8));
    else if (arg.startsWith('--weeks=')) weeks = parseInt(arg.slice(8));
    else if (arg.startsWith('--workers=')) workers = parseInt(arg.slice(10));
    else if (arg === '--debug') debug = true;
    else if (arg === '--validate') validate = true;
  }
  return { teams, weeks, debug, workers, validate };
}

// Get config from workerData in worker threads, parseArgs in main thread
const CONFIG = isMainThread ? parseArgs() : {
  teams: workerData.nTeams,
  weeks: workerData.numWeeks,
  debug: false,
  workers: 1,
  validate: false
};

// Teams encoded as indices 0 to N-1
const N_TEAMS = CONFIG.teams;
const N_WEEKS = CONFIG.weeks;
const DEBUG = CONFIG.debug;
const N_WORKERS = CONFIG.workers;
const VALIDATE = CONFIG.validate;
const TEAMS = 'ABCDEFGHIJKLMNOP'.slice(0, N_TEAMS).split('');
const N_SLOTS = (N_TEAMS * 3) / 2;
const N_MATCHUPS = (N_TEAMS * (N_TEAMS - 1)) / 2;

// Precompute matchup encoding/decoding
const matchupToTeams = new Uint8Array(N_MATCHUPS * 2);
const teamsToMatchup = new Uint8Array(N_TEAMS * N_TEAMS);

let idx = 0;
for (let ti1 = 0; ti1 < N_TEAMS - 1; ti1++) {
  for (let ti2 = ti1 + 1; ti2 < N_TEAMS; ti2++) {
    matchupToTeams[idx * 2] = ti1;
    matchupToTeams[idx * 2 + 1] = ti2;
    teamsToMatchup[ti1 * N_TEAMS + ti2] = idx;
    teamsToMatchup[ti2 * N_TEAMS + ti1] = idx;
    idx++;
  }
}

function encodeMatchup(ti1, ti2) {
  return teamsToMatchup[ti1 * N_TEAMS + ti2];
}

function decodeMatchup(matchupIdx) {
  return [matchupToTeams[matchupIdx * 2], matchupToTeams[matchupIdx * 2 + 1]];
}

function matchupToString(matchupIdx) {
  const [ti1, ti2] = decodeMatchup(matchupIdx);
  return `${TEAMS[ti1]}v${TEAMS[ti2]}`;
}

// Pattern shapes
const SHAPES_4 = [[0, 1, 3], [0, 2, 3]];
const SHAPES_5 = [[0, 1, 4], [0, 2, 4], [0, 3, 4]];

function buildPatterns(shapes, nSlots) {
  const patterns = [];
  for (const s of shapes) {
    const span = s[2] + 1;
    for (let start = 0; start <= nSlots - span; start++) {
      patterns.push([s[0] + start, s[1] + start, s[2] + start]);
    }
  }
  return patterns;
}

const PATTERNS = buildPatterns([...SHAPES_4, ...SHAPES_5], N_SLOTS);
const N_PATTERNS = PATTERNS.length;

// Precompute slot masks using BigInt
const slotMasks = new Array(N_SLOTS);
for (let slot = 0; slot < N_SLOTS; slot++) {
  let mask = 0n;
  for (let pi = 0; pi < N_PATTERNS; pi++) {
    if (PATTERNS[pi].includes(slot)) {
      mask |= (1n << BigInt(pi));
    }
  }
  slotMasks[slot] = mask;
}

const ALL_PATTERNS_MASK = (1n << BigInt(N_PATTERNS)) - 1n;

function scoreSchedule(schedule, nTeams = N_TEAMS, nSlots = N_SLOTS) {
  const slots = [];
  for (let ti = 0; ti < nTeams; ti++) slots.push([]);

  for (let s = 0; s < nSlots; s++) {
    const m = schedule[s];
    const t1 = matchupToTeams[m * 2];
    const t2 = matchupToTeams[m * 2 + 1];
    slots[t1].push(s);
    slots[t2].push(s);
  }

  let doubleByes = 0;
  let fiveSlotTeams = 0;

  for (let ti = 0; ti < nTeams; ti++) {
    const ts = slots[ti];
    if (ts.length !== 3) continue;

    const span = ts[2] - ts[0] + 1;
    if (span === 5) fiveSlotTeams++;

    for (let i = 0; i < 2; i++) {
      if (ts[i + 1] - ts[i] === 3) doubleByes++;
    }
  }

  return { doubleByes, fiveSlotTeams };
}

function enumerateSchedules(options) {
  const { excludeMatchups = 0, requiredMatchups = null, firstGame = null, onSchedule } = options;

  const games = new Uint8Array(N_SLOTS);
  const slotCounts = new Uint8Array(N_TEAMS);
  const validMasks = new Array(N_TEAMS).fill(0n);
  let usedMatchups = 0;
  let count = 0;

  // NOTE: excludeMatchups applies uniformly to all slots in the week.
  // When a week straddles rounds, games from different rounds can be
  // interleaved in ANY order. Required matchups just need to appear
  // somewhere in the week, not in specific slot positions.

  const requiredOpponents = new Array(N_TEAMS);
  for (let ti = 0; ti < N_TEAMS; ti++) {
    requiredOpponents[ti] = new Set();
  }
  if (requiredMatchups) {
    for (const m of requiredMatchups) {
      const [t1, t2] = decodeMatchup(m);
      requiredOpponents[t1].add(t2);
      requiredOpponents[t2].add(t1);
    }
  }

  function backtrack(slot) {
    if (slot === N_SLOTS) {
      // Verify all required matchups were placed
      for (let ti = 0; ti < N_TEAMS; ti++) {
        if (requiredOpponents[ti].size > 0) return;  // Required matchup missing
      }
      count++;
      if (onSchedule) onSchedule(games);
      return;
    }

    const sm = slotMasks[slot];
    const candidates = [];
    for (let ti = 0; ti < N_TEAMS; ti++) {
      if (slotCounts[ti] < 3 && (validMasks[ti] & sm)) {
        candidates.push(ti);
      }
    }

    if (candidates.length < 2) return;

    for (let i = 0; i < candidates.length - 1; i++) {
      const ti1 = candidates[i];
      for (let j = i + 1; j < candidates.length; j++) {
        const ti2 = candidates[j];
        const matchup = encodeMatchup(ti1, ti2);
        const matchupBit = 1 << matchup;

        if ((usedMatchups & matchupBit) || (excludeMatchups & matchupBit)) continue;

        games[slot] = matchup;
        usedMatchups |= matchupBit;
        slotCounts[ti1]++;
        slotCounts[ti2]++;
        const old1 = validMasks[ti1];
        const old2 = validMasks[ti2];
        validMasks[ti1] &= sm;
        validMasks[ti2] &= sm;

        const wasRequired1 = requiredOpponents[ti1].delete(ti2);
        const wasRequired2 = requiredOpponents[ti2].delete(ti1);

        let ok = (validMasks[ti1] !== 0n) && (validMasks[ti2] !== 0n);
        for (let ti = 0; ti < N_TEAMS && ok; ti++) {
          if (slotCounts[ti] < 3 && validMasks[ti] === 0n) ok = false;
        }

        if (ok) {
          for (let ti = 0; ti < N_TEAMS && ok; ti++) {
            const gamesLeft = 3 - slotCounts[ti];
            if (requiredOpponents[ti].size > gamesLeft) {
              ok = false;
            }
          }
        }

        if (ok) {
          const remaining = N_SLOTS - slot - 1;
          let needed = 0;
          for (let ti = 0; ti < N_TEAMS; ti++) {
            needed += 3 - slotCounts[ti];
          }
          if (needed / 2 <= remaining) {
            backtrack(slot + 1);
          }
        }

        if (wasRequired1) requiredOpponents[ti1].add(ti2);
        if (wasRequired2) requiredOpponents[ti2].add(ti1);

        validMasks[ti1] = old1;
        validMasks[ti2] = old2;
        slotCounts[ti1]--;
        slotCounts[ti2]--;
        usedMatchups &= ~matchupBit;
      }
    }
  }

  for (let ti = 0; ti < N_TEAMS; ti++) validMasks[ti] = ALL_PATTERNS_MASK;

  if (firstGame) {
    const [ti1, ti2] = firstGame;
    const matchup = encodeMatchup(ti1, ti2);
    games[0] = matchup;
    usedMatchups = 1 << matchup;
    slotCounts[ti1] = 1;
    slotCounts[ti2] = 1;

    const patternsWithSlot0 = [];
    for (let pi = 0; pi < N_PATTERNS; pi++) {
      if (PATTERNS[pi].includes(0)) {
        patternsWithSlot0.push(pi);
      }
    }

    for (const pa of patternsWithSlot0) {
      for (const pb of patternsWithSlot0) {
        const slotsA = new Set(PATTERNS[pa]);
        const slotsB = new Set(PATTERNS[pb]);
        let shared = 0;
        for (const s of slotsA) if (slotsB.has(s)) shared++;
        if (shared !== 1) continue;

        for (let ti = 0; ti < N_TEAMS; ti++) validMasks[ti] = ALL_PATTERNS_MASK;
        validMasks[ti1] = 1n << BigInt(pa);
        validMasks[ti2] = 1n << BigInt(pb);

        backtrack(1);
      }
    }
  } else {
    backtrack(0);
  }

  return count;
}

// ╔══════════════════════════════════════════════════════════════════════════════╗
// ║  ⚠️  CRITICAL: ROUND-ROBIN CROSS-WEEK RULES  ⚠️                              ║
// ║                                                                              ║
// ║  When a week STRADDLES TWO ROUNDS, games from the NEW round may appear in    ║
// ║  ANY slot - INCLUDING BEFORE games that complete the current round!          ║
// ║                                                                              ║
// ║  Example: 6 teams, week 1 with interleaved rounds:                           ║
// ║  - Week 0 used matchups: {0,2,3,6,8,9,10,11,14} (9 of 15 in round 0)         ║
// ║  - Required to complete round 0: {1,4,5,7,12,13} (6 matchups)                ║
// ║  - Valid week 1: [1,0,5,4,9,7,13,14,12]                                      ║
// ║    - Matchups 0,9,14 are round 1 games (reusing week 0's matchups)           ║
// ║    - These appear BEFORE some round 0 games — THIS IS VALID!                 ║
// ║                                                                              ║
// ║  The ONLY requirement: all requiredMatchups appear SOMEWHERE in the week.    ║
// ║  There is NO slot-order constraint based on round membership.                ║
// ╚══════════════════════════════════════════════════════════════════════════════╝

// Track which matchups have been used in each round
// roundMatchups[roundNum] = Set of matchup indices used in that round
function getRoundConstraints(weekStartGame, roundMatchups) {
  const startRound = Math.floor(weekStartGame / N_MATCHUPS);
  const usedInRound = roundMatchups.get(startRound) || new Set();
  const gamesPlayedInRound = usedInRound.size;
  const gamesRemainingInRound = N_MATCHUPS - gamesPlayedInRound;

  const requiredMatchups = new Set();

  // If remaining games in round fit in one week, all unused matchups are required
  // These must appear SOMEWHERE in the week (any slot), not in specific positions
  if (gamesRemainingInRound <= N_SLOTS && gamesRemainingInRound > 0) {
    for (let m = 0; m < N_MATCHUPS; m++) {
      if (!usedInRound.has(m)) {
        requiredMatchups.add(m);
      }
    }
  }

  // excludeMatchups: matchups used in current round cannot be reused WITHIN THAT ROUND
  // CRITICAL: When week straddles rounds (gamesRemainingInRound < N_SLOTS), the week
  // will complete the current round AND start a new one. The new-round matchups can be
  // ANY matchup. Since games can be interleaved in any order, we only apply exclusions
  // when the ENTIRE week fits within the current round.
  let excludeMatchups = 0;
  if (gamesRemainingInRound >= N_SLOTS) {
    // Entire week is within current round - exclude all matchups used so far in this round
    for (const m of usedInRound) {
      excludeMatchups |= (1 << m);
    }
  }
  // When gamesRemainingInRound < N_SLOTS: no exclusions, but requiredMatchups must appear

  return { excludeMatchups, requiredMatchups, gamesRemainingInRound };
}

// After a week is placed, update roundMatchups with the new matchups
function updateRoundMatchups(weekNum, weekMatchups, roundMatchups, requiredMatchups) {
  const weekStartGame = weekNum * N_SLOTS;
  const startRound = Math.floor(weekStartGame / N_MATCHUPS);

  // Copy existing round sets
  const newRoundMatchups = new Map();
  for (const [r, s] of roundMatchups) {
    newRoundMatchups.set(r, new Set(s));
  }

  // How many games remain in startRound?
  const usedInStartRound = newRoundMatchups.get(startRound) || new Set();
  const gamesRemainingInRound = N_MATCHUPS - usedInStartRound.size;

  if (gamesRemainingInRound >= N_SLOTS) {
    // Entire week fits in startRound
    if (!newRoundMatchups.has(startRound)) {
      newRoundMatchups.set(startRound, new Set());
    }
    for (const m of weekMatchups) {
      newRoundMatchups.get(startRound).add(m);
    }
  } else {
    // Week straddles rounds: required matchups go to startRound, extras go to startRound+1
    if (!newRoundMatchups.has(startRound)) {
      newRoundMatchups.set(startRound, new Set());
    }
    if (!newRoundMatchups.has(startRound + 1)) {
      newRoundMatchups.set(startRound + 1, new Set());
    }

    for (const m of weekMatchups) {
      if (requiredMatchups.has(m)) {
        // This matchup completes the current round
        newRoundMatchups.get(startRound).add(m);
      } else {
        // This matchup starts the new round
        newRoundMatchups.get(startRound + 1).add(m);
      }
    }
  }

  return newRoundMatchups;
}

// Worker thread logic
if (!isMainThread) {
  const { inputPaths, startWeek, numWeeks, nTeams, nSlots, nMatchups } = workerData;

  let totalPaths = 0;
  let optimalCount = 0;
  let minPerPath = Infinity;
  let maxPerPath = 0;
  let totalPerPath = 0;

  // OPTIMIZATION: Pre-computed optimal score (2 doubleByes, 4 fiveSlotTeams per week)
  const TARGET_SCORE = [2 * numWeeks, 4 * numWeeks];

  // MEMORY OPTIMIZATION: Store continuations as packed Uint8Arrays
  // Each continuation is (numWeeks - 1) weeks × nSlots bytes
  const CONTINUATION_SIZE = (numWeeks - 1) * nSlots;
  let currentWeek0Key = null;
  // Store as array of Uint8Arrays (each is a packed continuation)
  let currentWeek0Optimal = [];

  // Pack weeks 1..N into a single Uint8Array
  function packContinuation(weeks) {
    const packed = new Uint8Array(CONTINUATION_SIZE);
    for (let w = 1; w < weeks.length; w++) {
      const offset = (w - 1) * nSlots;
      const week = weeks[w];
      for (let i = 0; i < nSlots; i++) {
        packed[offset + i] = week[i];
      }
    }
    return packed;
  }

  // Unpack a continuation for sending (returns array of arrays)
  function unpackContinuation(packed) {
    const result = [];
    for (let w = 0; w < numWeeks - 1; w++) {
      const offset = w * nSlots;
      result.push(Array.from(packed.subarray(offset, offset + nSlots)));
    }
    return result;
  }

  // Rebuild round tracking from a path of weeks
  // NOTE: When a week straddles rounds, we must process ALL required matchups
  // first (to complete the current round), then ALL extras (to start the new round).
  // We cannot process in slot order because extras may appear before required matchups.
  function rebuildRoundMatchups(path) {
    const roundMatchups = new Map();
    let currentRound = 0;
    let usedInRound = new Set();

    for (let weekNum = 0; weekNum < path.length; weekNum++) {
      const week = path[weekNum];
      const gamesRemainingInRound = nMatchups - usedInRound.size;

      if (gamesRemainingInRound <= nSlots) {
        // This week straddles rounds - process required first, then extras
        const required = new Set();
        for (let m = 0; m < nMatchups; m++) {
          if (!usedInRound.has(m)) required.add(m);
        }

        // First pass: add all required matchups to complete current round
        for (const m of week) {
          if (required.has(m)) {
            usedInRound.add(m);
          }
        }

        // Round should now be complete
        if (usedInRound.size === nMatchups) {
          roundMatchups.set(currentRound, usedInRound);
          currentRound++;
          usedInRound = new Set();
        }

        // Second pass: add all extras to start new round
        for (const m of week) {
          if (!required.has(m)) {
            usedInRound.add(m);
          }
        }
      } else {
        // All matchups go to current round
        for (const m of week) {
          usedInRound.add(m);
        }
      }
    }

    // Save any remaining matchups in current round
    if (usedInRound.size > 0) {
      roundMatchups.set(currentRound, usedInRound);
    }

    return roundMatchups;
  }

  // OPTIMIZATION: Optimal score per week is always [2,4]
  const OPTIMAL_WEEK_SCORE = [2, 4];

  // MEMORY OPTIMIZATION: Flush accumulated results every N paths to prevent heap exhaustion
  const FLUSH_THRESHOLD = 10000;

  function flushOptimalIfNeeded(week0, force = false) {
    if (currentWeek0Optimal.length >= FLUSH_THRESHOLD || (force && currentWeek0Optimal.length > 0)) {
      const BATCH_SIZE = 10000;
      for (let i = 0; i < currentWeek0Optimal.length; i += BATCH_SIZE) {
        const batch = currentWeek0Optimal.slice(i, i + BATCH_SIZE);
        parentPort.postMessage({
          type: 'optimal',
          week0: Array.from(week0),
          continuations: batch.map(unpackContinuation),
          score: TARGET_SCORE,
          count: batch.length
        });
      }
      currentWeek0Optimal = [];
    }
  }

  function enumerateFromPath(week0, weekNum, roundMatchups, weeks) {
    if (weekNum === numWeeks) {
      // All weeks passed the [2,4] check, so this is optimal
      totalPaths++;
      optimalCount++;
      currentWeek0Optimal.push(packContinuation(weeks));
      // Flush periodically to prevent memory buildup
      flushOptimalIfNeeded(week0);
      return;
    }

    const gameOffset = weekNum * nSlots;
    const { excludeMatchups, requiredMatchups } = getRoundConstraints(gameOffset, roundMatchups);

    enumerateSchedules({
      excludeMatchups,
      requiredMatchups,
      onSchedule: (sched) => {
        // OPTIMIZATION: Check this week's score immediately, skip if not optimal
        const score = scoreSchedule(sched);
        if (score.doubleByes !== OPTIMAL_WEEK_SCORE[0] || score.fiveSlotTeams !== OPTIMAL_WEEK_SCORE[1]) {
          totalPaths++;  // Still count as explored
          return;  // Prune this branch
        }

        const weekMatchups = Array.from(sched);
        const newRoundMatchups = updateRoundMatchups(weekNum, weekMatchups, roundMatchups, requiredMatchups);
        const newWeeks = weeks.concat([new Uint8Array(sched)]);
        enumerateFromPath(week0, weekNum + 1, newRoundMatchups, newWeeks);
      }
    });
  }

  let processedCount = 0;
  for (const inputPath of inputPaths) {
    const week0 = inputPath[0];
    const week0Key = week0.join(',');
    currentWeek0Key = week0Key;
    currentWeek0Optimal = [];
    const pathsBefore = totalPaths;

    // Notify main thread we're starting a new input path
    parentPort.postMessage({ type: 'startPath', path: inputPath.map(w => Array.from(w)) });

    // Convert input path to Uint8Arrays
    const weeks = inputPath.map(w => new Uint8Array(w));

    // Rebuild round tracking from the input path
    const roundMatchups = rebuildRoundMatchups(inputPath);

    // Continue enumeration from where the input path left off
    enumerateFromPath(week0, startWeek, roundMatchups, weeks);

    // Flush any remaining optimal paths for this week0
    flushOptimalIfNeeded(week0, true);

    const pathsForThisInput = totalPaths - pathsBefore;
    if (pathsForThisInput < minPerPath) minPerPath = pathsForThisInput;
    if (pathsForThisInput > maxPerPath) maxPerPath = pathsForThisInput;
    totalPerPath += pathsForThisInput;
    processedCount++;

    // Send progress update after each input path
    parentPort.postMessage({ type: 'progress', processed: processedCount, total: inputPaths.length, paths: totalPaths });
  }

  parentPort.postMessage({ type: 'done',
    totalPaths,
    optimalCount,
    pathCount: inputPaths.length,
    minPerPath,
    maxPerPath,
    totalPerPath
  });
}

// Main thread logic
if (isMainThread && import.meta.url === `file://${process.argv[1]}`) {
  console.log('Parallel Tournament Schedule Generator' + (VALIDATE ? ' [VALIDATE MODE]' : ''));
  console.log(`Teams: ${N_TEAMS} (${TEAMS.join('')}), Slots: ${N_SLOTS}, Matchups: ${N_MATCHUPS}, Patterns: ${N_PATTERNS}`);
  console.log(`Weeks: ${N_WEEKS}, Workers: ${N_WORKERS}`);

  if (N_WEEKS < 1) {
    console.log('--weeks must be at least 1');
    process.exit(1);
  }

  // Build matchup legend for comments
  const matchupLegend = [];
  for (let t1 = 0; t1 < N_TEAMS - 1; t1++) {
    for (let t2 = t1 + 1; t2 < N_TEAMS; t2++) {
      matchupLegend.push(`${matchupLegend.length}=${TEAMS[t1]}v${TEAMS[t2]}`);
    }
  }

  if (!VALIDATE) mkdirSync('results', { recursive: true });

  // Helper to read paths from a results file in streaming chunks
  async function* streamPathsFromFile(filePath, chunkSize) {
    const rl = createInterface({
      input: createReadStream(filePath),
      crlfDelay: Infinity
    });

    // Read header first
    let header = null;
    let currentPath = [];
    let chunk = [];

    for await (const line of rl) {
      if (line.startsWith('#')) {
        header = parseHeader(line);
        continue;
      }
      if (!line.trim()) continue;

      // Count leading tabs to determine depth
      const depth = line.match(/^\t*/)[0].length;
      const schedule = line.trim().split(',').map(Number);

      // Truncate path to current depth and add new schedule
      currentPath = currentPath.slice(0, depth);
      currentPath.push(schedule);

      // Only yield complete paths (leaf nodes at target depth)
      if (currentPath.length === header.weeks) {
        chunk.push(currentPath.map(s => Array.from(s)));
        if (chunk.length >= chunkSize) {
          yield { chunk, header };
          chunk = [];
        }
      }
    }

    // Yield remaining
    if (chunk.length > 0) {
      yield { chunk, header };
    }
  }

  // Find best available prior results file (largest weeks < N_WEEKS)
  function findBestPriorFile() {
    for (let w = N_WEEKS - 1; w >= 2; w--) {
      const file = `results/${N_TEAMS}teams-${w}weeks.txt`;
      if (existsSync(file)) return { file, weeks: w };
    }
    const week1 = `results/${N_TEAMS}teams-1week.txt`;
    if (existsSync(week1)) return { file: week1, weeks: 1 };
    return null;
  }

  // Check for partial file at current depth for resume
  async function checkForResumableFile() {
    const currentFile = `results/${N_TEAMS}teams-${N_WEEKS}week${N_WEEKS > 1 ? 's' : ''}.txt`;
    if (!existsSync(currentFile)) return null;

    const headerLine = readFileSync(currentFile, 'utf8').split('\n')[0];
    const header = parseHeader(headerLine);

    // Only resume if file is partial and at same depth
    if (!header.partial || header.weeks !== N_WEEKS) return null;

    const reader = new TreeReader(currentFile);
    const { incompletePrefixes, completedPrefixes, isPartial } = await reader.findIncompleteBranches();

    return { file: currentFile, header, incompletePrefixes, completedPrefixes };
  }

  const week1File = `results/${N_TEAMS}teams-1week.txt`;
  let priorFile = null;
  let startingPaths = null;  // Array of paths to extend, or null to generate from scratch
  let startingWeeks = 0;     // Number of weeks already computed in startingPaths
  let priorScore = null;
  let resumeData = null;     // Data for resuming from partial file at same depth

  // First check if we're resuming from a partial file at the same depth
  if (!VALIDATE) {
    resumeData = await checkForResumableFile();
  }

  // Try to load best available prior results
  const bestPrior = !VALIDATE && !resumeData ? findBestPriorFile() : null;

  // Handle resume from partial file at same depth
  if (resumeData) {
    console.log(`\nResuming from partial file: ${resumeData.file}`);
    console.log(`  Score: [${resumeData.header.score}], ${resumeData.header.count} complete paths`);
    console.log(`  ${resumeData.incompletePrefixes.length} incomplete branches (in-flight when interrupted)`);
    console.log(`  ${resumeData.completedPrefixes.size} source paths fully explored`);

    // Find source file (N_WEEKS - 1) to get unprocessed paths
    const sourceWeeks = N_WEEKS - 1;
    const sourceFile = sourceWeeks === 1
      ? `results/${N_TEAMS}teams-1week.txt`
      : `results/${N_TEAMS}teams-${sourceWeeks}weeks.txt`;

    if (!existsSync(sourceFile)) {
      console.error(`Error: Cannot resume - source file ${sourceFile} not found`);
      process.exit(1);
    }

    // Load source paths and filter out completed ones
    console.log(`  Loading source paths from ${sourceFile}...`);
    const sourceReader = new TreeReader(sourceFile);
    const allSourcePaths = await sourceReader.readAll();
    console.log(`  ${allSourcePaths.length} total source paths`);

    // Filter to unprocessed paths (not in completedPrefixes and not in incompletePrefixes)
    const incompleteKeys = new Set(
      resumeData.incompletePrefixes.map(p => p.map(s => s.join(',')).join('|'))
    );
    const unprocessedPaths = allSourcePaths.filter(path => {
      const key = path.map(s => s.join(',')).join('|');
      return !resumeData.completedPrefixes.has(key) && !incompleteKeys.has(key);
    });

    console.log(`  ${unprocessedPaths.length} unprocessed paths to explore`);

    // Combine: incomplete prefixes first (resume in-flight), then unprocessed
    startingPaths = [...resumeData.incompletePrefixes, ...unprocessedPaths];
    startingWeeks = sourceWeeks;
    priorScore = resumeData.header.score;
    priorFile = resumeData.file;  // Will reload complete paths from this in runWorkers
  } else if (bestPrior && bestPrior.weeks > 1) {
    priorFile = bestPrior.file;
    startingWeeks = bestPrior.weeks;
    console.log(`\nFound prior results: ${priorFile}`);

    // Read header to get score
    const headerLine = readFileSync(priorFile, 'utf8').split('\n')[0];
    const header = parseHeader(headerLine);
    priorScore = header.score;
    console.log(`Prior results: ${header.count} paths with score [${priorScore}]`);
  } else if (bestPrior && bestPrior.weeks === 1) {
    // Load single-week schedules
    console.log(`\nLoading 1-week schedules from ${week1File}...`);
    const start = performance.now();
    const content = readFileSync(week1File, 'utf8');
    const lines = content.trim().split('\n').filter(line => line && !line.startsWith('#'));
    startingPaths = lines.map(line => [line.split(',').map(Number)]);
    startingWeeks = 1;

    if (startingPaths.length > 0) {
      const s = scoreSchedule(startingPaths[0][0]);
      priorScore = [s.doubleByes, s.fiveSlotTeams];
    }
    console.log(`Loaded ${startingPaths.length} schedules (score=${priorScore}) in ${((performance.now() - start) / 1000).toFixed(2)}s`);
  } else {
    // Generate week 1 from scratch
    console.log('\nGenerating 1-week schedules...');
    const start = performance.now();

    let bestScore = [Infinity, Infinity];
    const allWeek1 = [];
    enumerateSchedules({
      firstGame: [0, 1],
      onSchedule: (sched) => {
        const arr = Array.from(sched);
        const s = scoreSchedule(arr);
        allWeek1.push({ sched: arr, score: [s.doubleByes, s.fiveSlotTeams] });
        if (s.doubleByes < bestScore[0] || (s.doubleByes === bestScore[0] && s.fiveSlotTeams < bestScore[1])) {
          bestScore = [s.doubleByes, s.fiveSlotTeams];
        }
      }
    });

    // Filter to only optimal
    const optimal = allWeek1
      .filter(w => w.score[0] === bestScore[0] && w.score[1] === bestScore[1])
      .map(w => [w.sched]);  // Wrap in path array

    startingPaths = optimal;
    startingWeeks = 1;
    priorScore = bestScore;

    console.log(`Found ${allWeek1.length} total, ${optimal.length} optimal (score=${bestScore}) in ${((performance.now() - start) / 1000).toFixed(2)}s`);

    // Save 1-week schedules
    if (!VALIDATE) {
      const header = `# teams=${N_TEAMS} weeks=1 count=${optimal.length}\n`;
      const lines = optimal.map(p => p[0].join(','));
      writeFileSync(week1File, header + lines.join('\n') + '\n');
      console.log(`Saved ${optimal.length} optimal 1-week schedules to ${week1File}`);
    }
  }

  // For single-week mode, we're done
  if (N_WEEKS === 1) {
    const count = startingPaths ? startingPaths.length : 0;
    console.log(`\nDone. ${count} optimal single-week schedules with score [${priorScore}]`);
    process.exit(0);
  }

  console.log(`\nExtending from ${startingWeeks} weeks to ${N_WEEKS} weeks...`);
  const workerStart = performance.now();

  // Track progress and optimal results from all workers
  let totalPathsSoFar = 0;

  // OPTIMIZATION: Pre-computed optimal score (2 doubleByes, 4 fiveSlotTeams per week)
  const TARGET_SCORE = [2 * N_WEEKS, 4 * N_WEEKS];

  // MEMORY OPTIMIZATION: Instead of storing all paths in memory, we use a streaming
  // TreeWriter and only keep track of counts and the last written path for prefix compression
  let optimalCount = 0;
  let optimalWriter = null;  // TreeWriter instance, created on first optimal result
  let lastWrittenPath = null; // For prefix compression in streaming writes
  const existingPathHashes = new Set();  // For deduplication during resume - use hashes instead of full keys
  const saveFile = `results/${N_TEAMS}teams-${N_WEEKS}week${N_WEEKS > 1 ? 's' : ''}.txt`;

  // Hash a path to a compact number for deduplication (collision-tolerant since it's for filtering)
  function hashPath(path) {
    let hash = 0;
    for (let w = 0; w < path.length; w++) {
      const week = path[w];
      for (let i = 0; i < week.length; i++) {
        hash = ((hash << 5) - hash + week[i]) | 0;
        hash = ((hash << 13) ^ hash) | 0;
      }
    }
    return hash;
  }

  // Helper: compare two schedule arrays element-by-element
  function schedulesEqual(a, b) {
    if (!a || !b) return false;
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
      if (a[i] !== b[i]) return false;
    }
    return true;
  }

  // MEMORY OPTIMIZATION: Stream writes directly to file instead of accumulating in memory
  // This function writes a batch of paths to the streaming file
  function writePathBatch(paths) {
    if (paths.length === 0) return;

    if (VALIDATE) {
      // In validate mode, just count paths without file I/O
      optimalCount += paths.length;
      return;
    }

    // Initialize writer on first batch
    if (!optimalWriter) {
      optimalWriter = new TreeWriter(saveFile);
      optimalWriter.writeHeader(N_TEAMS, N_WEEKS, 0);
      lastWrittenPath = null;
    }

    // Write each path with prefix compression
    for (const path of paths) {
      // Find common prefix with last written path
      let commonDepth = 0;
      if (lastWrittenPath) {
        while (commonDepth < lastWrittenPath.length &&
               commonDepth < path.length &&
               schedulesEqual(lastWrittenPath[commonDepth], path[commonDepth])) {
          commonDepth++;
        }
      }

      // Write nodes from divergence point onwards
      for (let depth = commonDepth; depth < path.length; depth++) {
        optimalWriter.writeNode(depth, path[depth]);
      }

      lastWrittenPath = path.map(s => Array.from(s));
      optimalCount++;
    }
  }

  // Finalize the streaming file with correct header and handle in-flight paths
  async function finalizeOptimalFile(isFinal = false, inFlightPaths = []) {
    if (VALIDATE) return optimalCount;
    if (!optimalWriter && optimalCount === 0 && inFlightPaths.length === 0) return 0;

    // Write in-flight path markers if any
    if (optimalWriter && inFlightPaths.length > 0) {
      for (const inFlightPath of inFlightPaths) {
        if (!inFlightPath) continue;

        // Find common prefix with last written path
        let commonDepth = 0;
        if (lastWrittenPath) {
          while (commonDepth < lastWrittenPath.length &&
                 commonDepth < inFlightPath.length &&
                 schedulesEqual(lastWrittenPath[commonDepth], inFlightPath[commonDepth])) {
            commonDepth++;
          }
        }

        for (let depth = commonDepth; depth < inFlightPath.length; depth++) {
          optimalWriter.writeNode(depth, inFlightPath[depth]);
        }
        optimalWriter.writeIncompleteMarker(inFlightPath.length);

        lastWrittenPath = inFlightPath.map(s => Array.from(s));
      }
    }

    if (optimalWriter) {
      const isPartial = !isFinal || inFlightPaths.length > 0;
      await optimalWriter.finalize({
        count: optimalCount,
        partial: isPartial
      });
      optimalWriter = null;
    }

    return optimalCount;
  }

  // Spawn workers and distribute work
  // Workers receive paths and starting week number
  async function runWorkers() {
    let allPaths = [];
    let totalInputPaths = 0;

    // MEMORY OPTIMIZATION: When resuming, we copy existing paths to the streaming temp file
    // instead of loading them all into memory. We only track hashes for deduplication.
    if (resumeData && priorFile && existsSync(priorFile)) {
      // Verify the resume file has the expected optimal score
      const resumeScore = resumeData.header.score;
      if (resumeScore[0] !== TARGET_SCORE[0] || resumeScore[1] !== TARGET_SCORE[1]) {
        console.log(`Warning: Resume file has score [${resumeScore}] but target is [${TARGET_SCORE}], starting fresh`);
        // Fall through to use startingPaths
        allPaths = startingPaths;
        totalInputPaths = allPaths.length;
      } else {
        console.log(`Copying existing complete paths from ${priorFile}...`);

        // Stream-copy existing paths to our new temp file and track hashes
        const reader = new TreeReader(priorFile);
        let existingCount = 0;
        let batchPaths = [];
        const BATCH_SIZE = 10000;

        for await (const path of reader.paths()) {
          // Track hash for deduplication
          existingPathHashes.add(hashPath(path));
          batchPaths.push(path);
          existingCount++;

          // Write in batches to avoid memory accumulation
          if (batchPaths.length >= BATCH_SIZE) {
            writePathBatch(batchPaths);
            batchPaths = [];
          }
        }
        // Write remaining
        if (batchPaths.length > 0) {
          writePathBatch(batchPaths);
        }

        console.log(`  Copied ${existingCount} existing paths, tracking ${existingPathHashes.size} hashes for dedup`);

        // Use startingPaths which contains incomplete + unprocessed paths
        allPaths = startingPaths;
        totalInputPaths = allPaths.length;
      }
    } else if (priorFile && existsSync(priorFile) && !VALIDATE) {
      // Stream from prior results file and collect all paths
      console.log(`Streaming paths from ${priorFile}...`);

      for await (const { chunk, header } of streamPathsFromFile(priorFile, 1000)) {
        allPaths.push(...chunk);
        totalInputPaths += chunk.length;
      }
    } else if (startingPaths) {
      // Use in-memory paths
      allPaths = startingPaths;
      totalInputPaths = startingPaths.length;
    }

    // Distribute paths across N_WORKERS chunks
    const chunks = [];
    const chunkSize = Math.ceil(allPaths.length / N_WORKERS);
    for (let i = 0; i < allPaths.length; i += chunkSize) {
      chunks.push(allPaths.slice(i, i + chunkSize));
    }

    console.log(`Distributing ${totalInputPaths} paths across ${chunks.length} workers...`);

    const workerProgress = new Array(chunks.length).fill(0);
    const workerTotals = chunks.map(c => c.length);

    // Track in-flight paths per worker for graceful shutdown
    const workerInFlightPaths = new Array(chunks.length).fill(null);
    const workerInstances = [];  // Store worker instances for termination
    let shuttingDown = false;

    const workers = chunks.map((chunk, i) => {
      return new Promise((resolve, reject) => {
        const worker = new Worker(fileURLToPath(import.meta.url), {
          workerData: {
            inputPaths: chunk,
            startWeek: startingWeeks,
            numWeeks: N_WEEKS,
            nTeams: N_TEAMS,
            nSlots: N_SLOTS,
            nMatchups: N_MATCHUPS
          }
        });
        workerInstances[i] = worker;

        worker.on('message', (msg) => {
          if (msg.type === 'startPath') {
            // Track which path this worker is currently processing
            workerInFlightPaths[i] = msg.path;
          } else if (msg.type === 'progress') {
            workerProgress[i] = msg.processed;
            // Clear in-flight since we just finished one
            workerInFlightPaths[i] = null;
            totalPathsSoFar += msg.paths - (workerProgress[i] > 10 ? msg.paths : 0); // Approximate
            updateProgress();  // Immediate update on progress change
          } else if (msg.type === 'optimal') {
            // OPTIMIZATION: All optimal results have the same pre-computed TARGET_SCORE
            // Build full paths from worker message
            const week0 = msg.week0;
            const pathsToWrite = [];

            for (const laterWeeks of msg.continuations) {
              const fullPath = [week0, ...laterWeeks];

              // Only deduplicate when resuming (existingPathHashes is populated during resume)
              // For fresh runs, workers process disjoint starting paths so no duplicates possible
              if (existingPathHashes.size > 0) {
                const pathHash = hashPath(fullPath);
                if (existingPathHashes.has(pathHash)) {
                  continue;  // Skip duplicate from resume
                }
                existingPathHashes.add(pathHash);
              }

              pathsToWrite.push(fullPath);
            }

            if (pathsToWrite.length > 0) {
              writePathBatch(pathsToWrite);
            }
          } else if (msg.type === 'done') {
            workerProgress[i] = workerTotals[i];
            resolve(msg);
          }
        });
        worker.on('error', reject);
        worker.on('exit', (code) => {
          if (code !== 0) reject(new Error(`Worker ${i} exited with code ${code}`));
        });
      });
    });

    // Format duration for human readability
    function formatDuration(seconds) {
      if (seconds < 60) return `${Math.round(seconds)}s`;
      if (seconds < 3600) {
        const m = Math.floor(seconds / 60);
        const s = Math.round(seconds % 60);
        return `${m}:${s.toString().padStart(2, '0')} minutes`;
      }
      if (seconds < 86400) {
        const h = Math.floor(seconds / 3600);
        const m = Math.round((seconds % 3600) / 60);
        return `${h}:${m.toString().padStart(2, '0')} hours`;
      }
      if (seconds < 86400 * 14) {
        const d = Math.floor(seconds / 86400);
        const h = Math.floor((seconds % 86400) / 3600);
        const m = Math.round((seconds % 3600) / 60);
        return `${d}d ${h}:${m.toString().padStart(2, '0')}`;
      }
      if (seconds < 86400 * 365) {
        const weeks = seconds / (86400 * 7);
        return `${weeks.toFixed(1)} weeks`;
      }
      const years = Math.floor(seconds / (86400 * 365));
      const weeks = Math.round((seconds % (86400 * 365)) / (86400 * 7));
      return `${years}y ${weeks}w`;
    }

    // Progress display function
    function updateProgress(saveInfo = '') {
      const now = performance.now();
      const elapsed = (now - workerStart) / 1000;
      const processed = workerProgress.reduce((a, b) => a + b, 0);

      const pct = ((processed / totalInputPaths) * 100).toFixed(1);
      const rate = processed / elapsed;
      const remaining = totalInputPaths - processed;
      const eta = rate > 0 ? formatDuration(remaining / rate) : '?';

      // MEMORY OPTIMIZATION: optimalCount is now tracked incrementally
      process.stdout.write(`\r  ${processed}/${totalInputPaths} paths (${pct}%) | ${formatDuration(elapsed)} so far | ${eta} to go | ${optimalCount} optimal${saveInfo}   `);
    }

    // Show initial progress immediately
    process.stdout.write(`\r  0/${totalInputPaths} paths (0.0%) | 0s so far | ? to go | 0 optimal   `);

    // Periodic display update (1Hz) - no periodic saves since streaming writes go directly to disk
    // Finalization only happens on interrupt (SIGINT/SIGTERM) or completion
    const progressInterval = setInterval(() => {
      updateProgress();
    }, 1000);  // Update display every 1s

    // SIGINT handler for graceful shutdown
    const handleSigint = () => {
      if (shuttingDown) {
        console.log('\n\nForce exit (second Ctrl+C)');
        process.exit(1);
      }
      shuttingDown = true;
      console.log('\n\nInterrupted - saving progress with incomplete markers...');

      // Terminate all workers
      for (const worker of workerInstances) {
        if (worker) worker.terminate();
      }

      // Collect in-flight paths (filter nulls)
      const inFlightPaths = workerInFlightPaths.filter(p => p !== null);
      console.log(`  ${inFlightPaths.length} in-flight paths marked as incomplete`);

      // MEMORY OPTIMIZATION: Finalize the streaming file with in-flight markers
      finalizeOptimalFile(false, inFlightPaths).then(() => {
        console.log(`  Saved ${optimalCount} complete paths to ${saveFile}`);
        console.log(`  Resume with: node calculate.mjs --teams=${N_TEAMS} --weeks=${N_WEEKS}`);
        clearInterval(progressInterval);
        process.exit(0);
      });
    };
    process.on('SIGINT', handleSigint);
    process.on('SIGTERM', handleSigint);  // Also handle SIGTERM (used by timeout, kill, etc.)

    const results = await Promise.all(workers);
    process.off('SIGINT', handleSigint);  // Remove handler after normal completion
    process.off('SIGTERM', handleSigint);
    clearInterval(progressInterval);
    process.stdout.write('\n');
    return { results, totalInputPaths };
  }

  runWorkers().then(async ({ results, totalInputPaths }) => {
    const elapsed = (performance.now() - workerStart) / 1000;

    // Aggregate stats from workers
    let totalPaths = 0;
    let workerOptimalCount = 0;
    let globalMinPerPath = Infinity;
    let globalMaxPerPath = 0;
    let globalTotalPerPath = 0;
    let totalPathCount = 0;

    for (const r of results) {
      totalPaths += r.totalPaths;
      totalPathCount += r.pathCount;
      globalTotalPerPath += r.totalPerPath;
      workerOptimalCount += r.optimalCount;

      if (r.minPerPath < globalMinPerPath) globalMinPerPath = r.minPerPath;
      if (r.maxPerPath > globalMaxPerPath) globalMaxPerPath = r.maxPerPath;
    }

    const avgPerPath = totalPathCount > 0 ? globalTotalPerPath / totalPathCount : 0;

    console.log(`\nCompleted in ${elapsed.toFixed(2)}s`);
    console.log(`Total: ${totalPaths} complete ${N_WEEKS}-week schedules`);
    console.log(`Optimal: ${optimalCount} with total doubleByes=${TARGET_SCORE[0]}, fiveSlotTeams=${TARGET_SCORE[1]}`);
    console.log(`Input stats: ${totalPathCount} paths, extensions per path: min=${globalMinPerPath}, max=${globalMaxPerPath}, avg=${avgPerPath.toFixed(1)}`);
    console.log(`Throughput: ${(totalPaths / elapsed).toFixed(0)} paths/sec`);

    // MEMORY OPTIMIZATION: Finalize the streaming file (skip in validate mode)
    if (optimalCount > 0 && !VALIDATE) {
      await finalizeOptimalFile(true);
      console.log(`Saved ${optimalCount} optimal paths to ${saveFile}`);
    }
  }).catch((err) => {
    console.error('Worker error:', err);
  });
}
