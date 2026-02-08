// Parallel Tournament Schedule Generator
// Uses worker threads to parallelize multi-week enumeration

import { Worker, isMainThread, parentPort, workerData } from 'worker_threads';
import { cpus } from 'os';
import { fileURLToPath } from 'url';
import { writeFileSync, readFileSync, existsSync, mkdirSync, unlinkSync, openSync, readSync, closeSync, copyFileSync } from 'fs';
import { createReadStream } from 'fs';
import { createInterface } from 'readline';
import { TreeWriter, TreeReader, parseHeader, readHeaderFromFile } from './lib/tree-format.mjs';

// ============================================================================
// Multi-Level Mod Iterator: Diverse path iteration across tree structure
// ============================================================================
// Instead of depth-first file order, iterate paths using modular arithmetic
// at each tree level to maximize diversity across all dimensions.
//
// For step i:
//   w1_idx = i % count_W1_nodes
//   w2_idx = i % children_count(w1_node)
//   w3_idx = i % children_count(w2_node)
//   ...
//
// This ensures early iterations cover diverse branches at ALL tree levels.
// ============================================================================

/**
 * Build a tree index from a results file for multi-level mod iteration.
 *
 * Structure: For each internal node, store:
 *   - schedule: the matchup array for this week
 *   - childCount: number of children (next level nodes or leaves)
 *   - firstChildIdx: index of first child in the next level's array (for internal children)
 *   - firstLeafIdx: global leaf index of first leaf child (for leaf parents)
 *
 * Also stores leafOffsets: Float64Array of byte offsets for ALL leaves (O(1) access)
 *
 * @param {string} filePath - Path to tree-format results file
 * @returns {Promise<{header, nodesByDepth, leafOffsets, totalPaths}>}
 */
async function buildTreeIndex(filePath) {
  console.log('  Building tree index for diverse iteration...');
  const startTime = performance.now();

  const rl = createInterface({
    input: createReadStream(filePath),
    crlfDelay: Infinity
  });

  let header = null;
  let totalPaths = 0;
  let currentByteOffset = 0;

  // nodesByDepth[d] = [{schedule, childCount, firstChildIdx, firstLeafIdx}, ...]
  const nodesByDepth = [];

  // Store byte offsets for ALL leaves - enables O(1) access
  // Using regular array first (will convert to Float64Array after knowing size)
  const leafOffsetsList = [];

  // Stack: [{depth, nodeIndex}] - tracks current position in tree
  const stack = [];

  let lineCount = 0;
  let lastProgressTime = startTime;

  for await (const line of rl) {
    const lineBytes = Buffer.byteLength(line, 'utf8') + 1;
    lineCount++;

    const now = performance.now();
    if (now - lastProgressTime > 2000) {
      const memUsed = (leafOffsetsList.length * 8 / 1024 / 1024).toFixed(0);
      process.stdout.write(`\r    ${lineCount.toLocaleString()} lines, ${leafOffsetsList.length.toLocaleString()} leaves (~${memUsed} MB)...   `);
      lastProgressTime = now;
    }

    if (line.startsWith('#')) {
      header = parseHeader(line);
      for (let d = 0; d < header.weeks - 1; d++) {
        nodesByDepth.push([]);
      }
      currentByteOffset += lineBytes;
      continue;
    }

    if (line.trim() === '' || line.trim() === '…') {
      currentByteOffset += lineBytes;
      continue;
    }

    let depth = 0;
    while (line[depth] === '\t') depth++;

    const content = line.slice(depth);
    const schedule = content.split(',').map(n => parseInt(n.trim(), 10));

    // Pop stack to current depth
    while (stack.length > depth) {
      stack.pop();
    }

    const isLeaf = depth === header.weeks - 1;

    if (isLeaf) {
      // Record leaf byte offset for O(1) access later
      const globalLeafIdx = leafOffsetsList.length;
      leafOffsetsList.push(currentByteOffset);
      totalPaths++;

      // Update leaf parent's child count and firstLeafIdx
      if (stack.length > 0) {
        const parent = stack[stack.length - 1];
        const parentNode = nodesByDepth[parent.depth][parent.nodeIndex];
        parentNode.childCount++;
        if (parentNode.firstLeafIdx === -1) {
          parentNode.firstLeafIdx = globalLeafIdx;
        }
      }
    } else {
      const nodeIndex = nodesByDepth[depth].length;
      const node = {
        schedule,
        childCount: 0,
        firstChildIdx: -1,  // For internal node children
        firstLeafIdx: -1    // For leaf children (only leaf parents use this)
      };
      nodesByDepth[depth].push(node);

      // Link from parent
      if (stack.length > 0) {
        const parent = stack[stack.length - 1];
        const parentNode = nodesByDepth[parent.depth][parent.nodeIndex];
        parentNode.childCount++;
        if (parentNode.firstChildIdx === -1) {
          parentNode.firstChildIdx = nodeIndex;
        }
      }

      stack.push({ depth, nodeIndex });
    }

    currentByteOffset += lineBytes;
  }

  // Convert to typed array for memory efficiency
  const leafOffsets = new Float64Array(leafOffsetsList);

  const elapsed = ((performance.now() - startTime) / 1000).toFixed(1);
  const totalNodes = nodesByDepth.reduce((sum, level) => sum + level.length, 0);
  const leafMemMB = (leafOffsets.byteLength / 1024 / 1024).toFixed(1);
  const nodeMemMB = (totalNodes * 50 / 1024 / 1024).toFixed(1);

  process.stdout.write(`\r    Indexed ${totalNodes.toLocaleString()} nodes + ${totalPaths.toLocaleString()} leaves in ${elapsed}s (${leafMemMB} MB offsets + ~${nodeMemMB} MB nodes)   \n`);

  // Log tree shape
  for (let d = 0; d < nodesByDepth.length; d++) {
    const nodes = nodesByDepth[d];
    const childCounts = nodes.map(n => n.childCount);
    const avgChildren = childCounts.length > 0
      ? (childCounts.reduce((a, b) => a + b, 0) / childCounts.length).toFixed(1)
      : 0;
    console.log(`    Depth ${d} (Week ${d + 1}): ${nodes.length.toLocaleString()} nodes, avg ${avgChildren} children`);
  }

  return { header, nodesByDepth, leafOffsets, totalPaths };
}

/**
 * Multi-level mod iterator: yields paths in diverse order.
 * Uses precomputed leaf byte offsets for O(1) leaf access.
 */
class MultiLevelModIterator {
  #filePath;
  #nodesByDepth;
  #leafOffsets;  // Float64Array of all leaf byte offsets
  #totalPaths;
  #weeks;
  #fd = null;
  #buffer = null;

  constructor(filePath, treeIndex) {
    this.#filePath = filePath;
    this.#nodesByDepth = treeIndex.nodesByDepth;
    this.#leafOffsets = treeIndex.leafOffsets;
    this.#totalPaths = treeIndex.totalPaths;
    this.#weeks = treeIndex.header.weeks;
    this.#buffer = Buffer.alloc(256);  // Enough for one schedule line
  }

  get totalPaths() {
    return this.#totalPaths;
  }

  open() {
    if (!this.#fd) {
      this.#fd = openSync(this.#filePath, 'r');
    }
  }

  close() {
    if (this.#fd) {
      closeSync(this.#fd);
      this.#fd = null;
    }
  }

  /**
   * Get path at step using mixed-radix arithmetic for diverse iteration.
   *
   * To visit all paths while maximizing diversity:
   * - The FIRST dimension (week-1 nodes) cycles fastest: nodeIdx = step % nodeCount
   * - Later dimensions cycle slower: childIdx = floor(step / priorProduct) % childCount
   *
   * This ensures early steps touch different week-1 branches before revisiting any.
   *
   * NOTE: This assumes roughly uniform child counts at each level. For highly non-uniform
   * trees, some paths may be visited multiple times while others are skipped.
   */
  getPathAtStep(step) {
    const path = [];

    // For diverse order, we cycle through week-1 nodes fastest (innermost dimension)
    // and leaves slowest (outermost dimension).
    // This is like reading a number in mixed-radix: step = leaf * nodeCount + nodeIdx

    // Track cumulative product of dimension sizes for mixed-radix indexing
    let cumulativeProduct = 1;

    // Navigate through internal nodes
    for (let depth = 0; depth < this.#weeks - 1; depth++) {
      const nodesAtDepth = this.#nodesByDepth[depth];

      if (depth === 0) {
        // Root level: cycles fastest (step % nodeCount)
        const nodeIdx = step % nodesAtDepth.length;
        path.push({ node: nodesAtDepth[nodeIdx], idx: nodeIdx });
        cumulativeProduct = nodesAtDepth.length;
      } else {
        // Child level: use mixed-radix indexing
        // childOffset = floor(step / cumulativeProduct) % childCount
        const parent = path[depth - 1].node;
        if (parent.childCount === 0) {
          console.error(`Parent at depth ${depth - 1} has no children`);
          return null;
        }
        const childOffset = Math.floor(step / cumulativeProduct) % parent.childCount;
        const nodeIdx = parent.firstChildIdx + childOffset;
        path.push({ node: nodesAtDepth[nodeIdx], idx: nodeIdx });
        cumulativeProduct *= parent.childCount;
      }
    }

    // Get leaf using mixed-radix: leafIdx = floor(step / cumulativeProduct) % childCount
    const leafParent = path[this.#weeks - 2].node;
    const leafChildIdx = Math.floor(step / cumulativeProduct) % leafParent.childCount;
    const globalLeafIdx = leafParent.firstLeafIdx + leafChildIdx;

    // Direct O(1) access to leaf byte offset
    const leafByteOffset = this.#leafOffsets[globalLeafIdx];
    const leafSchedule = this.#readLeafAt(leafByteOffset);

    // Build final path as array of schedules
    const result = path.map(p => p.node.schedule);
    result.push(leafSchedule);

    return { index: step, path: result };
  }

  /**
   * Read a single leaf schedule at a known byte offset - O(1) operation
   */
  #readLeafAt(byteOffset) {
    if (!this.#fd) this.open();

    const bytesRead = readSync(this.#fd, this.#buffer, 0, this.#buffer.length, byteOffset);
    if (bytesRead === 0) return null;

    // Find line end
    let lineEnd = 0;
    while (lineEnd < bytesRead && this.#buffer[lineEnd] !== 10) lineEnd++;

    // Skip leading tabs
    let start = 0;
    while (start < lineEnd && this.#buffer[start] === 9) start++;

    const content = this.#buffer.toString('utf8', start, lineEnd);
    return content.split(',').map(n => parseInt(n.trim(), 10));
  }
}

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
  --breadth=N   Results per path per round in breadth-first mode (default: 32)
  --validate    Run without reading/writing files (test enumeration only)
  --debug       Enable verbose logging
  --help, -h    Show this help message

Examples:
  node schedule-parallel.mjs --teams=6 --weeks=3
  node schedule-parallel.mjs --teams=8 --weeks=4 --workers=8
  node schedule-parallel.mjs --teams=6 --weeks=5 --validate
  node schedule-parallel.mjs --teams=8 --weeks=5 --breadth=100  # Quick stats run

Output files are saved to results/{N}teams-{W}week(s).txt
Prior results are automatically loaded to resume from the best available checkpoint.
`);
    process.exit(0);
  }

  // Default to leaving 2 cores free for OS responsiveness (minimum 1 worker)
  const defaultWorkers = Math.max(1, cpus().length - 2);
  let teams = 8, weeks = 2, debug = false, workers = defaultWorkers, validate = false, breadth = 32;
  for (const arg of args) {
    if (arg.startsWith('--teams=')) teams = parseInt(arg.slice(8));
    else if (arg.startsWith('--weeks=')) weeks = parseInt(arg.slice(8));
    else if (arg.startsWith('--workers=')) workers = parseInt(arg.slice(10));
    else if (arg.startsWith('--breadth=')) breadth = parseInt(arg.slice(10));
    else if (arg === '--debug') debug = true;
    else if (arg === '--validate') validate = true;
  }
  return { teams, weeks, debug, workers, validate, breadth };
}

// Get config from workerData in worker threads, parseArgs in main thread
const CONFIG = isMainThread ? parseArgs() : {
  teams: workerData.nTeams,
  weeks: workerData.numWeeks,
  debug: false,
  workers: 1,
  validate: false,
  breadth: workerData.breadth || 0
};

// Teams encoded as indices 0 to N-1
const N_TEAMS = CONFIG.teams;
const N_WEEKS = CONFIG.weeks;
const DEBUG = CONFIG.debug;
const N_WORKERS = CONFIG.workers;
const VALIDATE = CONFIG.validate;
const BREADTH = CONFIG.breadth;  // 0 = unlimited, >0 = stop after this many optimal paths per worker
const TEAMS = 'ABCDEFGHIJKLMNOP'.slice(0, N_TEAMS).split('');
const N_SLOTS = (N_TEAMS * 3) / 2;
const N_MATCHUPS = (N_TEAMS * (N_TEAMS - 1)) / 2;

// Experimentally-determined average continuations per input path, by team count and target week
// Used to estimate total optimal schedules for progress display
// Format: CONTINUATION_MULTIPLIERS[teams][targetWeek] = average continuations
const CONTINUATION_MULTIPLIERS = {
  6: {
    2: 36,       // Week 2 continuations per week 1
    3: 96,       // Week 3 continuations per week 2
    4: 192,      // Week 4 continuations per week 3
    5: 12,       // Week 5 continuations per week 4
    6: 720,      // Week 6 continuations per week 5
  },
  8: {
    2: 32,       // Week 2 continuations per week 1
    3: 125,      // Week 3 continuations per week 2 (measured: 5.75M unique / 46K paths)
    4: 9147,     // Week 4 continuations per week 3
    5: 22.73,    // Week 5 continuations per week 4
    6: 201600,   // Week 6 continuations per week 5
  }
};

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
  let stopped = false;  // Early termination flag

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
    if (stopped) return;  // Early exit if callback requested stop

    if (slot === N_SLOTS) {
      // Verify all required matchups were placed
      for (let ti = 0; ti < N_TEAMS; ti++) {
        if (requiredOpponents[ti].size > 0) return;  // Required matchup missing
      }
      count++;
      // onSchedule returns false to stop enumeration
      if (onSchedule && onSchedule(games) === false) {
        stopped = true;
      }
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

    for (let i = 0; i < candidates.length - 1 && !stopped; i++) {
      const ti1 = candidates[i];
      for (let j = i + 1; j < candidates.length && !stopped; j++) {
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
      if (stopped) break;
      for (const pb of patternsWithSlot0) {
        if (stopped) break;
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
  // Find the current round: the first incomplete round in roundMatchups
  // (Don't rely on math alone - track actual matchup usage)
  let startRound = 0;
  while (roundMatchups.has(startRound) && roundMatchups.get(startRound).size === N_MATCHUPS) {
    startRound++;
  }
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
  const { startWeek, numWeeks, nTeams, nSlots, nMatchups } = workerData;

  // OPTIMIZATION: Pre-computed optimal score (2 doubleByes, 4 fiveSlotTeams per week)
  const TARGET_SCORE = [2 * numWeeks, 4 * numWeeks];

  // MEMORY OPTIMIZATION: Store continuations as packed Uint8Arrays
  // Each continuation is (numWeeks - 1) weeks × nSlots bytes
  const CONTINUATION_SIZE = (numWeeks - 1) * nSlots;

  // ============================================================================
  // MAJOR OPTIMIZATION: Cache optimal schedules by constraint key
  // Instead of re-enumerating for each input path with the same constraints,
  // we cache the results. The constraints (excludeMatchups, requiredMatchups)
  // dramatically prune the search space, making enumeration fast.
  // Many input paths share the same round state, so caching gives huge wins.
  // ============================================================================

  // LRU cache for constraint-based enumeration results
  // Maps (excludeMatchups, requiredMask) -> array of optimal schedules
  // Limits memory usage while allowing repeated constraint patterns to be fast
  class LRUCache {
    constructor(maxSize) {
      this.maxSize = maxSize;
      this.cache = new Map();
      this.hits = 0;
      this.misses = 0;
    }

    get(key) {
      if (!this.cache.has(key)) {
        this.misses++;
        return undefined;
      }
      this.hits++;
      // Move to end (most recently used)
      const value = this.cache.get(key);
      this.cache.delete(key);
      this.cache.set(key, value);
      return value;
    }

    set(key, value) {
      if (this.cache.has(key)) {
        this.cache.delete(key);
      } else if (this.cache.size >= this.maxSize) {
        // Delete oldest (first) entry
        const firstKey = this.cache.keys().next().value;
        this.cache.delete(firstKey);
      }
      this.cache.set(key, value);
    }

    clear() {
      this.cache.clear();
    }

    stats() {
      return { hits: this.hits, misses: this.misses, size: this.cache.size };
    }
  }

  // Cache size: 10k entries should be plenty for most constraint combinations
  // Each entry stores full schedules for that constraint combination
  const constraintCache = new LRUCache(10000);

  // OPTIMIZATION: Optimal score per week is always [2,4]
  const OPTIMAL_WEEK_SCORE = [2, 4];

  // Enumerate optimal schedules for given constraints, with caching
  // This is MUCH faster than unconstrained enumeration because:
  // 1. excludeMatchups prunes many branches (matchups already used in round)
  // 2. requiredMatchups adds additional constraints (must complete round)
  // 3. Results are cached by constraint key for reuse
  function getValidOptimalWeeks(excludeMatchups, requiredMatchups) {
    // Convert requiredMatchups Set to bitmask for cache key
    let requiredMask = 0;
    for (const m of requiredMatchups) requiredMask |= (1 << m);

    const key = `${excludeMatchups}|${requiredMask}`;
    const cached = constraintCache.get(key);
    if (cached !== undefined) return cached;

    // Cache miss: enumerate with constraints (fast due to pruning)
    const result = [];
    enumerateSchedules({
      excludeMatchups,
      requiredMatchups,
      onSchedule: (sched) => {
        const score = scoreSchedule(sched);
        if (score.doubleByes === OPTIMAL_WEEK_SCORE[0] && score.fiveSlotTeams === OPTIMAL_WEEK_SCORE[1]) {
          result.push(new Uint8Array(sched));
        }
      }
    });

    constraintCache.set(key, result);
    return result;
  }

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

  // State for processing a single work unit
  let optimalCount = 0;
  let schedulesChecked = 0;  // Track total continuations checked
  let skipOffset = 0;
  let breadthLimit = 0;
  let diversifyStep = 0;  // Used to start enumeration at diverse positions
  let breadthReached = false;
  let currentWeek0Optimal = [];

  // Flush results frequently to show progress - use breadth limit or 100, whichever is smaller
  const FLUSH_THRESHOLD = 100;

  function flushOptimalIfNeeded(week0, force = false) {
    if (currentWeek0Optimal.length >= FLUSH_THRESHOLD || (force && currentWeek0Optimal.length > 0)) {
      parentPort.postMessage({
        type: 'optimal',
        week0: Array.from(week0),
        continuations: currentWeek0Optimal.map(unpackContinuation),
        score: TARGET_SCORE,
        count: currentWeek0Optimal.length
      });
      currentWeek0Optimal = [];
    }
  }

  function enumerateFromPath(week0, weekNum, roundMatchups, weeks) {
    // Check if we've collected enough for this breadth chunk
    if (breadthReached) return;

    if (weekNum === numWeeks) {
      // All weeks passed the [2,4] check, so this is optimal
      optimalCount++;

      // Skip results we've already found in previous rounds
      if (optimalCount <= skipOffset) {
        return;  // Already found this one, skip it
      }

      currentWeek0Optimal.push(packContinuation(weeks));
      // Flush periodically to prevent memory buildup
      flushOptimalIfNeeded(week0);

      // Check breadth limit (counting only NEW results, not skipped ones)
      const newResultsFound = optimalCount - skipOffset;
      if (breadthLimit > 0 && newResultsFound >= breadthLimit) {
        breadthReached = true;
      }
      return;
    }

    const gameOffset = weekNum * nSlots;
    const { excludeMatchups, requiredMatchups } = getRoundConstraints(gameOffset, roundMatchups);

    // Always use cached results for consistent ordering - this enables diversification
    // Caching is fast due to constraint pruning (excludeMatchups, requiredMatchups)
    const validSchedules = getValidOptimalWeeks(excludeMatchups, requiredMatchups);
    schedulesChecked += validSchedules.length;
    
    if (validSchedules.length === 0) return;

    // For breadth-limited mode with diversification:
    // Start iterating from a diversified position based on diversifyStep
    // This ensures different input paths explore different week orderings first
    const startIdx = diversifyStep % validSchedules.length;
    
    for (let i = 0; i < validSchedules.length; i++) {
      if (breadthReached) return;

      // Wrap around: start from startIdx, then continue to end, then from 0
      const schedIdx = (startIdx + i) % validSchedules.length;
      const sched = validSchedules[schedIdx];

      const weekMatchups = Array.from(sched);
      const newRoundMatchups = updateRoundMatchups(weekNum, weekMatchups, roundMatchups, requiredMatchups);
      const newWeeks = weeks.concat([new Uint8Array(sched)]);
      enumerateFromPath(week0, weekNum + 1, newRoundMatchups, newWeeks);
    }
  }

  // Process a single work unit (one input path with skipOffset and breadth)
  function processWorkUnit(inputPath, skip, breadth, divStep = 0) {
    // Reset state for this work unit
    optimalCount = 0;
    schedulesChecked = 0;  // Reset counter for this work unit
    skipOffset = skip;
    breadthLimit = breadth;
    diversifyStep = divStep;  // Used to start enumeration at diverse positions
    breadthReached = false;
    currentWeek0Optimal = [];

    const week0 = inputPath[0];

    // Convert input path to Uint8Arrays
    const weeks = inputPath.map(w => new Uint8Array(w));

    // Rebuild round tracking from the input path
    const roundMatchups = rebuildRoundMatchups(inputPath);

    // Continue enumeration from where the input path left off
    enumerateFromPath(week0, startWeek, roundMatchups, weeks);

    // Flush any remaining optimal paths
    flushOptimalIfNeeded(week0, true);

    // Return results
    const newResultsFound = optimalCount - skipOffset;
    const isComplete = !breadthReached;  // If we didn't hit breadth, we found everything

    return {
      totalFound: optimalCount,  // Total optimal paths found (including skipped)
      newResults: newResultsFound,  // New results found this round
      schedulesChecked,  // Total continuations checked
      isComplete
    };
  }

  // Message-based work loop: wait for work assignments from main thread
  parentPort.on('message', (msg) => {
    if (msg.type === 'work') {
      const { inputPath, pathIndex, skipOffset: skip, breadth, diversifyStep: divStep } = msg;
      const result = processWorkUnit(inputPath, skip, breadth, divStep || 0);

      parentPort.postMessage({
        type: 'result',
        pathIndex,
        totalFound: result.totalFound,
        newResults: result.newResults,
        schedulesChecked: result.schedulesChecked,
        isComplete: result.isComplete
      });
    } else if (msg.type === 'done') {
      // No more work, exit cleanly
      process.exit(0);
    }
  });

  // Signal ready to receive work
  parentPort.postMessage({ type: 'ready' });
}

// Main thread logic
if (isMainThread && fileURLToPath(import.meta.url) === process.argv[1]) {
  console.log('Parallel Tournament Schedule Generator' + (VALIDATE ? ' [VALIDATE MODE]' : ''));
  console.log(`Teams: ${N_TEAMS} (${TEAMS.join('')}), Slots: ${N_SLOTS}, Matchups: ${N_MATCHUPS}, Patterns: ${N_PATTERNS}`);
  console.log(`Weeks: ${N_WEEKS}, Workers: ${N_WORKERS}, Breadth: ${BREADTH}`);

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

    // Stream header to handle large files
    const header = await readHeaderFromFile(currentFile);

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
    console.log(`  ${resumeData.header.count} complete paths`);
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

    // MEMORY OPTIMIZATION: Don't load paths into memory
    // Workers will stream through source file and skip completed/incomplete paths
    const sourceHeader = await readHeaderFromFile(sourceFile);
    const totalSourcePaths = sourceHeader.count;

    // Build set of keys for incomplete prefixes (small - 4 in example)
    const incompleteKeys = new Set(
      resumeData.incompletePrefixes.map(p => p.map(s => s.join(',')).join('|'))
    );

    const skippedCount = resumeData.completedPrefixes.size + incompleteKeys.size;
    const unprocessedCount = totalSourcePaths - skippedCount;

    console.log(`  ${totalSourcePaths} total source paths`);
    console.log(`  ${resumeData.completedPrefixes.size} completed, ${incompleteKeys.size} incomplete (will resume)`);
    console.log(`  ${unprocessedCount} unprocessed paths to explore`);

    // For resume: incomplete prefixes go to startingPaths (small, in-memory)
    // Unprocessed paths will be streamed by workers, skipping completed/incomplete
    startingPaths = resumeData.incompletePrefixes;  // Small array, safe in memory
    startingWeeks = sourceWeeks;
    priorFile = resumeData.file;  // Will reload complete paths from this in runWorkers

    // Store resume info for worker distribution - keep sets in memory for filtering
    resumeData.sourceFile = sourceFile;
    resumeData.totalSourcePaths = totalSourcePaths;
    resumeData.unprocessedCount = unprocessedCount;
    resumeData.completedKeys = resumeData.completedPrefixes;
    resumeData.incompleteKeys = incompleteKeys;
  } else if (bestPrior && bestPrior.weeks > 1) {
    priorFile = bestPrior.file;
    startingWeeks = bestPrior.weeks;
    console.log(`\nFound prior results: ${priorFile}`);

    // Read header to get count (streaming to handle large files)
    const header = await readHeaderFromFile(priorFile);
    console.log(`Prior results: ${header.count} paths`);
  } else if (bestPrior && bestPrior.weeks === 1) {
    // Load single-week schedules
    console.log(`\nLoading 1-week schedules from ${week1File}...`);
    const start = performance.now();
    const content = readFileSync(week1File, 'utf8');
    const lines = content.trim().split('\n').filter(line => line && !line.startsWith('#'));
    startingPaths = lines.map(line => [line.split(',').map(Number)]);
    startingWeeks = 1;

    console.log(`Loaded ${startingPaths.length} schedules in ${((performance.now() - start) / 1000).toFixed(2)}s`);
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

    console.log(`Found ${allWeek1.length} total, ${optimal.length} optimal in ${((performance.now() - start) / 1000).toFixed(2)}s`);

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
    console.log(`\nDone. ${count} optimal single-week schedules`);
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
  const EXISTING_HASH_SHARDS = 32;
  const existingPathHashes = Array.from(
    { length: EXISTING_HASH_SHARDS },
    () => new Set(),
  );  // For deduplication during resume - use hashes instead of full keys
  const saveFile = `results/${N_TEAMS}teams-${N_WEEKS}week${N_WEEKS > 1 ? 's' : ''}.txt`;
  if (!VALIDATE && resumeData && priorFile && priorFile === saveFile && existsSync(saveFile)) {
    const now = new Date();
    const stamp = [
      now.getFullYear(),
      String(now.getMonth() + 1).padStart(2, '0'),
      String(now.getDate()).padStart(2, '0'),
      String(now.getHours()).padStart(2, '0'),
      String(now.getMinutes()).padStart(2, '0'),
    ].join('');
    const backupFile = saveFile.replace(/\.txt$/, `.${stamp}.txt`);
    copyFileSync(saveFile, backupFile);
    resumeData.readFile = backupFile;
    console.log(`Backed up resume source to ${backupFile}`);
  }

  // Hash a path to BigInt for deduplication (64-bit to minimize collisions at scale)
  // With 5M+ items, 32-bit hash has ~4000 expected collisions; 64-bit has ~1e-6
  function hashPath(path) {
    let h1 = 0n, h2 = 0n;
    for (let w = 0; w < path.length; w++) {
      const week = path[w];
      for (let i = 0; i < week.length; i++) {
        const v = BigInt(week[i]);
        h1 = ((h1 << 5n) - h1 + v) & 0xFFFFFFFFn;
        h2 = ((h2 << 7n) + h2 + v) & 0xFFFFFFFFn;
      }
    }
    return (h1 << 32n) | h2;
  }

  function hashShard(hash) {
    return Number(hash & BigInt(EXISTING_HASH_SHARDS - 1));
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

  function commitSaveFileIfNeeded() {
    return { committed: true, path: saveFile };
  }

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

  // Spawn workers and run with round-robin work distribution
  async function runWorkers() {
    // ========================================================================
    // STEP 1: Determine input source and total path count
    // ========================================================================
    let sourceFile = null;      // File to stream from (null = use in-memory paths)
    let inMemoryPaths = null;   // In-memory paths (for small datasets or resume incomplete)
    let totalInputPaths = 0;
    let resumeFilter = null;    // { completedKeys, incompleteKeys } for resume streaming

    if (resumeData && priorFile) {
      const resumeReadFile = resumeData.readFile || priorFile;
      if (!existsSync(resumeReadFile)) {
        throw new Error(`Resume read file not found: ${resumeReadFile}`);
      }
      // Resume mode: copy existing results, then load ALL source paths
      // (filtering out completed ones, marking incomplete ones for resume)
      console.log(`Copying existing complete paths from ${resumeReadFile}...`);
      const reader = new TreeReader(resumeReadFile);
      let existingCount = 0;
      let batchPaths = [];
      const BATCH_SIZE = 10000;

      for await (const path of reader.paths()) {
        const pathHash = hashPath(path);
        existingPathHashes[hashShard(pathHash)].add(pathHash);
        batchPaths.push(path);
        existingCount++;
        if (batchPaths.length >= BATCH_SIZE) {
          writePathBatch(batchPaths);
          batchPaths = [];
        }
      }
      if (batchPaths.length > 0) {
        writePathBatch(batchPaths);
      }
      console.log(`  Copied ${existingCount} existing paths`);

      // Build sets for quick lookup
      const completedKeys = resumeData.completedKeys || new Set();
      const incompleteKeys = resumeData.incompleteKeys || new Set();

      // Resume incomplete prefixes in-memory (small), stream all others from file
      inMemoryPaths = resumeData.incompletePrefixes && resumeData.incompletePrefixes.length > 0
        ? resumeData.incompletePrefixes
        : null;
      sourceFile = resumeData.sourceFile;
      resumeFilter = { completedKeys, incompleteKeys };

      const skippedComplete = completedKeys.size;
      const resumeIncomplete = incompleteKeys.size;
      const addedUnprocessed = resumeData.unprocessedCount || 0;

      console.log(`  Streaming source paths from ${resumeData.sourceFile}...`);
      console.log(`  Source: ${skippedComplete} complete (skip), ${resumeIncomplete} incomplete (resume), ${addedUnprocessed} unprocessed (new)`);

      totalInputPaths = (inMemoryPaths ? inMemoryPaths.length : 0) + addedUnprocessed;
    } else if (priorFile && existsSync(priorFile) && !VALIDATE) {
      // Build tree index for diverse iteration (multi-level mod)
      sourceFile = priorFile;
      const treeIndex = await buildTreeIndex(priorFile);
      totalInputPaths = treeIndex.totalPaths;

      // Create multi-level mod iterator for diverse path ordering
      var modIterator = new MultiLevelModIterator(priorFile, treeIndex);
      modIterator.open();
    } else if (startingPaths) {
      inMemoryPaths = startingPaths;
      totalInputPaths = startingPaths.length;
    }

    if (totalInputPaths === 0) {
      console.log('No input paths to process');
      return { totalInputPaths: 0 };
    }

    // ========================================================================
    // STEP 2: Initialize work queue state
    // ========================================================================
    // Track state for each input path
    const foundCounts = new Array(totalInputPaths).fill(0);  // How many optimal found per path
    const isComplete = new Array(totalInputPaths).fill(false);  // Whether path is fully explored
    const incompletePathsMap = new Map();  // Store incomplete paths by index (for streaming mode)
    let currentRound = 0;   // Which "round" of breadth exploration we're on

    // For streaming: we iterate through the file, yielding work units
    // When round ends, we re-stream from beginning for incomplete paths
    let pathIterator = null;
    let currentPathIndex = 0;
    let roundComplete = false;

    // Queue to serialize getNextWork() calls (prevents readline race conditions)
    let workQueue = Promise.resolve();

    async function* createPathIterator() {
      if (inMemoryPaths) {
        for (let i = 0; i < inMemoryPaths.length; i++) {
          yield { index: i, path: inMemoryPaths[i], diversifyStep: i };
        }
      } else if (modIterator) {
        // Multi-level mod iteration for diverse coverage
        // NOTE: Non-uniform child counts can cause duplicate path generation;
        // we deduplicate here to avoid wasting worker computation
        const dispatchedPathKeys = new Set();
        let uniqueIndex = 0;  // Sequential index for unique paths only
        let duplicatesSkipped = 0;
        const originalTotal = totalInputPaths;
        for (let step = 0; step < originalTotal; step++) {
          const result = modIterator.getPathAtStep(step);
          if (result && result.path) {
            const pathKey = result.path.map(s => s.join(',')).join('|');
            if (!dispatchedPathKeys.has(pathKey)) {
              dispatchedPathKeys.add(pathKey);
              // Use sequential index for unique paths (not step, which has gaps)
              // Pass step (not uniqueIndex) as diversifyStep to maximize spread
              yield { index: uniqueIndex++, path: result.path, diversifyStep: step };
            } else {
              duplicatesSkipped++;
            }
          }
        }
        if (duplicatesSkipped > 0) {
          console.log(`  (Mod iterator: ${duplicatesSkipped.toLocaleString()} redundant paths skipped, ${uniqueIndex.toLocaleString()} unique dispatched)`);
          // Shrink tracking arrays to match actual unique count (avoids false incomplete detection)
          totalInputPaths = uniqueIndex;
          foundCounts.length = uniqueIndex;
          isComplete.length = uniqueIndex;
        }
      } else {
        // Fallback: sequential file order
        const reader = new TreeReader(sourceFile);
        let idx = 0;
        for await (const path of reader.paths()) {
          yield { index: idx, path, diversifyStep: idx };
          idx++;
        }
      }
    }

    // Get next work unit (round-robin across incomplete paths)
    // Returns null when current round is exhausted; caller should check roundComplete
    async function getNextWorkInternal() {
      while (true) {
        if (!pathIterator) {
          pathIterator = createPathIterator();
          currentPathIndex = 0;
          roundComplete = false;
        }

        const next = await pathIterator.next();
        if (next.done) {
          // End of current round
          pathIterator = null;
          roundComplete = true;

          // Check if any paths still incomplete
          const hasIncomplete = isComplete.some(c => !c);
          if (hasIncomplete) {
            currentRound++;
            pathsProcessedThisRound = 0;  // Reset for new round
            // Start new round - caller will call getNextWork again
            pathIterator = createPathIterator();
            currentPathIndex = 0;
            roundComplete = false;
            continue;  // Try again with new iterator
          } else {
            return null;  // All done
          }
        }

        const { index, path, diversifyStep } = next.value;
        currentPathIndex = index;

        if (!isComplete[index]) {
          return {
            pathIndex: index,
            inputPath: path,
            skipOffset: foundCounts[index],
            breadth: BREADTH,
            diversifyStep: diversifyStep || index
          };
        }
        // Path already complete, continue to next
      }
    }

    // Serialized wrapper to prevent concurrent iterator access (readline race condition)
    async function getNextWork() {
      // Chain onto the queue - each caller waits for all previous to finish
      const myTurn = workQueue;
      let signalDone;
      workQueue = new Promise(resolve => { signalDone = resolve; });

      await myTurn;
      try {
        return await getNextWorkInternal();
      } finally {
        signalDone();
      }
    }

    // ========================================================================
    // STEP 3: Estimate total optimal schedules
    // ========================================================================
    const multipliers = CONTINUATION_MULTIPLIERS[N_TEAMS];
    const multiplier = multipliers ? multipliers[N_WEEKS] : null;
    const estimatedTotalOptimal = multiplier
      ? Math.round(totalInputPaths * multiplier)
      : null;

    if (estimatedTotalOptimal) {
      console.log(`Estimated total optimal schedules: ~${estimatedTotalOptimal.toLocaleString()} (based on ${multiplier.toLocaleString()} continuations/path)`);
    } else {
      console.log(`No continuation multiplier known for ${N_TEAMS} teams, week ${N_WEEKS}`);
    }

    if (modIterator) {
      console.log(`Diverse iteration: multi-level mod across all tree depths`);
    }
    console.log(`Breadth limit: ${BREADTH} results per path per round`);

    // ========================================================================
    // STEP 4: Create workers and set up message handling
    // ========================================================================
    const workerInstances = [];
    const workerCurrentPath = new Array(N_WORKERS).fill(null);  // Track current work per worker
    let workersFinished = 0;
    let shuttingDown = false;
    let pathsProcessed = 0;
    let pathsProcessedThisRound = 0;  // Reset each round for progress display
    let totalSchedulesChecked = 0;  // Track total continuations checked across all workers
    let firstWorkTime = null;  // Track when first worker starts (for meaningful elapsed time)

    // Progress display
    function updateProgress(saveInfo = '') {
      const now = performance.now();

      // Before first work assigned, show "initializing" message
      if (!firstWorkTime) {
        const warmupElapsed = (now - workerStart) / 1000;
        process.stdout.write(`\r  Initializing... (${formatDuration(warmupElapsed)})${saveInfo}   `);
        return;
      }

      const elapsed = (now - firstWorkTime) / 1000;
      const completedPaths = isComplete.filter(c => c).length;

      let pct, eta, progressStr;
      if (estimatedTotalOptimal) {
        // Always show progress against estimated total
        pct = ((optimalCount / estimatedTotalOptimal) * 100).toFixed(1);
        const optimalRate = optimalCount / elapsed;
        const remainingOptimal = estimatedTotalOptimal - optimalCount;
        eta = optimalRate > 0 ? formatDuration(remainingOptimal / optimalRate) : '?';
        progressStr = `${optimalCount.toLocaleString()}/~${estimatedTotalOptimal.toLocaleString()} (${pct}%)`;
      } else {
        pct = ((completedPaths / totalInputPaths) * 100).toFixed(1);
        const rate = completedPaths / elapsed;
        const remaining = totalInputPaths - completedPaths;
        eta = rate > 0 ? formatDuration(remaining / rate) : '?';
        progressStr = `${completedPaths}/${totalInputPaths} paths (${pct}%) | ${optimalCount} optimal`;
      }

      // Add round info - paths processed this round vs total paths needing work
      const pathsToProcess = totalInputPaths - completedPaths;  // Paths not yet fully exhausted
      const roundPct = pathsToProcess > 0 ? ((pathsProcessedThisRound / pathsToProcess) * 100).toFixed(1) : '100.0';
      progressStr += ` | round ${currentRound + 1} (${roundPct}% done)`;

      // Add discarded count (can be temporarily negative due to message ordering:
      // optimalCount updates on 'optimal' messages, totalSchedulesChecked on 'result')
      const discarded = totalSchedulesChecked - optimalCount;
      if (totalSchedulesChecked > 0 && discarded >= 0) {
        progressStr += ` | ${discarded.toLocaleString()} discarded`;
      }

      process.stdout.write(`\r  ${progressStr} | ${formatDuration(elapsed)} elapsed | ${eta} remaining${saveInfo}   `);
    }

    // Show initial progress (initializing phase)
    process.stdout.write(`\r  Initializing...   `);

    const progressInterval = setInterval(() => {
      updateProgress();
    }, 1000);

    return new Promise((resolve, reject) => {
      // Create workers
      for (let i = 0; i < N_WORKERS; i++) {
        const worker = new Worker(fileURLToPath(import.meta.url), {
          workerData: {
            startWeek: startingWeeks,
            numWeeks: N_WEEKS,
            nTeams: N_TEAMS,
            nSlots: N_SLOTS,
            nMatchups: N_MATCHUPS
          }
        });
        workerInstances[i] = worker;

        worker.on('message', async (msg) => {
          if (msg.type === 'ready') {
            // Worker is ready - assign first work unit
            const work = await getNextWork();
            if (work) {
              if (!firstWorkTime) firstWorkTime = performance.now();
              workerCurrentPath[i] = work.inputPath;
              worker.postMessage({ type: 'work', ...work });
            } else {
              worker.postMessage({ type: 'done' });
              workersFinished++;
              if (workersFinished === N_WORKERS) {
                finishUp();
              }
            }
          } else if (msg.type === 'optimal') {
            // Worker found optimal paths - write them
            // Skip if shutting down (workers terminated, messages may be corrupted)
            if (shuttingDown) return;

            const week0 = msg.week0;
            const pathsToWrite = [];

            for (const laterWeeks of msg.continuations) {
              const fullPath = [week0, ...laterWeeks];

              // Validate path structure (guards against corrupted messages during shutdown)
              let valid = true;
              for (const week of fullPath) {
                if (!Array.isArray(week) || week.length !== N_SLOTS || week.some(v => !Number.isFinite(v))) {
                  valid = false;
                  break;
                }
              }
              if (!valid) continue;

              // Always deduplicate - async race conditions can cause duplicates
              const pathHash = hashPath(fullPath);
              const shard = hashShard(pathHash);
              if (existingPathHashes[shard].has(pathHash)) {
                continue;
              }
              existingPathHashes[shard].add(pathHash);

              pathsToWrite.push(fullPath);
            }

            if (pathsToWrite.length > 0) {
              writePathBatch(pathsToWrite);
              updateProgress();  // Update display when results come in
            }
          } else if (msg.type === 'result') {
            // Worker finished a work unit
            const { pathIndex, totalFound, schedulesChecked, isComplete: pathComplete } = msg;

            foundCounts[pathIndex] = totalFound;
            isComplete[pathIndex] = pathComplete;
            pathsProcessed++;
            pathsProcessedThisRound++;
            if (schedulesChecked !== undefined) {
              totalSchedulesChecked += schedulesChecked;
            }

            // If path hit breadth limit (not complete), write … marker immediately
            if (!pathComplete && workerCurrentPath[i] && optimalWriter) {
              const inputPath = workerCurrentPath[i];

              // Find common prefix with last written path
              let commonDepth = 0;
              if (lastWrittenPath) {
                while (commonDepth < lastWrittenPath.length &&
                       commonDepth < inputPath.length &&
                       schedulesEqual(lastWrittenPath[commonDepth], inputPath[commonDepth])) {
                  commonDepth++;
                }
              }

              // Write the input path nodes that differ from last written
              for (let depth = commonDepth; depth < inputPath.length; depth++) {
                optimalWriter.writeNode(depth, inputPath[depth]);
              }

              // Write incomplete marker at the next depth (where continuations would go)
              optimalWriter.writeIncompleteMarker(inputPath.length);

              lastWrittenPath = inputPath.map(s => Array.from(s));

              // Track for resume
              incompletePathsMap.set(pathIndex, inputPath);
            } else if (pathComplete) {
              incompletePathsMap.delete(pathIndex);  // Path is now complete
            }

            workerCurrentPath[i] = null;

            updateProgress();

            // Assign next work
            const work = await getNextWork();
            if (work) {
              workerCurrentPath[i] = work.inputPath;
              worker.postMessage({ type: 'work', ...work });
            } else {
              worker.postMessage({ type: 'done' });
              workersFinished++;
              if (workersFinished === N_WORKERS) {
                finishUp();
              }
            }
          }
        });

        worker.on('error', (err) => {
          console.error(`Worker ${i} error:`, err);
          reject(err);
        });

        worker.on('exit', (code) => {
          if (code !== 0 && !shuttingDown) {
            reject(new Error(`Worker ${i} exited with code ${code}`));
          }
        });
      }

      // SIGINT handler
      const handleSigint = () => {
        if (shuttingDown) {
          console.log('\n\nForce exit (second Ctrl+C)');
          process.exit(1);
        }
        shuttingDown = true;
        console.log('\n\nInterrupted - saving progress...');

        // Terminate all workers
        for (const worker of workerInstances) {
          if (worker) worker.terminate();
        }

        // Collect all incomplete paths:
        // 1. In-flight paths (currently being processed by workers)
        // 2. Paths that hit breadth limit and weren't fully explored
        const inFlightPaths = workerCurrentPath.filter(p => p !== null);
        const breadthLimitPaths = Array.from(incompletePathsMap.values());
        const allIncompletePaths = [...inFlightPaths, ...breadthLimitPaths];

        console.log(`  ${inFlightPaths.length} in-flight + ${breadthLimitPaths.length} breadth-limited = ${allIncompletePaths.length} incomplete paths`);

        finalizeOptimalFile(false, allIncompletePaths).then(() => {
          const commitInfo = commitSaveFileIfNeeded();
          if (modIterator) modIterator.close();
          console.log(`  Saved ${optimalCount} complete paths to ${commitInfo.path}`);
          console.log(`  Resume with: node calculate.mjs --teams=${N_TEAMS} --weeks=${N_WEEKS}`);
          clearInterval(progressInterval);
          process.exit(0);
        });
      };
      process.on('SIGINT', handleSigint);
      process.on('SIGTERM', handleSigint);

      function finishUp() {
        process.off('SIGINT', handleSigint);
        process.off('SIGTERM', handleSigint);
        clearInterval(progressInterval);
        if (modIterator) modIterator.close();
        process.stdout.write('\n');

        // Collect incomplete paths from the map (works for both streaming and in-memory modes)
        const incompletePaths = Array.from(incompletePathsMap.values());

        resolve({
          totalInputPaths,
          pathsProcessed,
          incompletePaths,
          foundCounts,
          firstWorkTime
        });
      }
    });
  }

  runWorkers().then(async ({ totalInputPaths, pathsProcessed, incompletePaths, foundCounts, firstWorkTime: fwt }) => {
    const totalElapsed = (performance.now() - workerStart) / 1000;
    const warmupTime = fwt ? (fwt - workerStart) / 1000 : totalElapsed;
    const activeTime = fwt ? (performance.now() - fwt) / 1000 : 0;

    const timeStr = fwt
      ? `${formatDuration(totalElapsed)} total (${formatDuration(warmupTime)} warmup + ${formatDuration(activeTime)} active)`
      : formatDuration(totalElapsed);
    console.log(`\nCompleted in ${timeStr}`);
    console.log(`Optimal: ${optimalCount} with total doubleByes=${TARGET_SCORE[0]}, fiveSlotTeams=${TARGET_SCORE[1]}`);

    if (foundCounts && foundCounts.length > 0) {
      const validCounts = foundCounts.filter(c => c > 0);
      if (validCounts.length > 0) {
        const min = Math.min(...validCounts);
        const max = Math.max(...validCounts);
        const avg = validCounts.reduce((a, b) => a + b, 0) / validCounts.length;
        console.log(`Continuations per path: min=${min}, max=${max}, avg=${avg.toFixed(1)}`);
      }
    }

    const throughputTime = fwt ? activeTime : totalElapsed;
    if (throughputTime > 0 && optimalCount > 0) {
      console.log(`Throughput: ${(optimalCount / throughputTime).toFixed(0)} optimal/sec`);
    }

    if (incompletePaths && incompletePaths.length > 0) {
      console.log(`Incomplete paths: ${incompletePaths.length} (breadth limit)`);
    }

    // Finalize file
    if (optimalCount > 0 && !VALIDATE) {
      const isPartial = incompletePaths && incompletePaths.length > 0;
      await finalizeOptimalFile(!isPartial, isPartial ? incompletePaths : []);
      const commitInfo = commitSaveFileIfNeeded();
      console.log(`Saved ${optimalCount} optimal paths to ${commitInfo.path}${isPartial ? ' (partial)' : ''}`);
    }
  }).catch((err) => {
    console.error('Worker error:', err);
  });
}
