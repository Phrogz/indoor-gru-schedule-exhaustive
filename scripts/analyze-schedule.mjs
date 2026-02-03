// Analyze a single schedule from a results file to understand round-robin
import { createReadStream } from 'fs';
import { createInterface } from 'readline';

const file = process.argv[2] || 'results/6teams-2weeks.txt';

// Parse header for config
const headerMatch = (await import('fs')).readFileSync(file, 'utf8').split('\n')[0];
const teamsMatch = headerMatch.match(/teams=(\d+)/);
const weeksMatch = headerMatch.match(/weeks=(\d+)/);
const N_TEAMS = teamsMatch ? parseInt(teamsMatch[1]) : 6;
const N_WEEKS = weeksMatch ? parseInt(weeksMatch[1]) : 2;
const N_SLOTS = (N_TEAMS * 3) / 2;
const N_MATCHUPS = (N_TEAMS * (N_TEAMS - 1)) / 2;

console.log(`Config: ${N_TEAMS} teams, ${N_WEEKS} weeks, ${N_SLOTS} slots/week, ${N_MATCHUPS} matchups/round`);

// Read first complete path from file
const rl = createInterface({ input: createReadStream(file) });
let stack = [];
let foundPath = null;

for await (const line of rl) {
  if (line.startsWith('#') || !line.trim()) continue;
  let depth = 0;
  while (line[depth] === '\t') depth++;
  const content = line.slice(depth);
  if (content === '…') continue;
  const schedule = content.split(',').map(n => parseInt(n.trim(), 10));
  while (stack.length > depth) stack.pop();
  stack.push(schedule);
  if (stack.length === N_WEEKS) {
    foundPath = stack.map(s => [...s]);
    break;
  }
}
rl.close();

if (!foundPath) {
  console.log('No complete path found');
  process.exit(1);
}

console.log('\nFirst schedule found:');
for (let w = 0; w < N_WEEKS; w++) {
  console.log(`  Week ${w}: [${foundPath[w].join(',')}]`);
}

// Analyze round-robin
const allMatchups = foundPath.flat();
console.log(`\nAll ${allMatchups.length} matchups (flattened): [${allMatchups.join(',')}]`);

// Check for duplicates across all games
const counts = {};
for (const m of allMatchups) {
  counts[m] = (counts[m] || 0) + 1;
}
const dups = Object.entries(counts).filter(([m, c]) => c > 1);
console.log('\nDuplicate matchups (across all weeks):', dups.length > 0 ? dups.map(([m,c]) => `${m}(x${c})`).join(', ') : 'None');

// For round-robin validation:
// - Round 0 consists of the first 15 UNIQUE matchups encountered
// - Round 1 starts when we see a matchup that was already in round 0
const round0 = new Set();
const round1 = new Set();
let round0Complete = false;

for (const m of allMatchups) {
  if (!round0Complete) {
    if (round0.has(m)) {
      // This matchup repeats, so round 0 is done and this starts round 1
      round0Complete = true;
      round1.add(m);
    } else {
      round0.add(m);
      if (round0.size === N_MATCHUPS) {
        round0Complete = true;
      }
    }
  } else {
    round1.add(m);
  }
}

console.log(`\nRound analysis:`);
console.log(`  Round 0: ${round0.size} unique matchups`);
console.log(`  Round 1: ${round1.size} unique matchups (so far)`);

if (round0.size === N_MATCHUPS) {
  console.log(`  ✓ Round 0 is complete (all ${N_MATCHUPS} matchups played once)`);
} else {
  const missing = [];
  for (let m = 0; m < N_MATCHUPS; m++) {
    if (!round0.has(m)) missing.push(m);
  }
  console.log(`  ✗ Round 0 incomplete: missing matchups [${missing.join(',')}]`);
}

// Show where rounds transition
console.log(`\nGame-by-game round assignment:`);
let pos = 0;
const seen = new Set();
for (let w = 0; w < N_WEEKS; w++) {
  const weekRounds = [];
  for (const m of foundPath[w]) {
    if (seen.has(m)) {
      weekRounds.push(`${m}*`);  // * means this is a round 1 game (repeat)
    } else {
      weekRounds.push(`${m}`);
      seen.add(m);
    }
  }
  console.log(`  Week ${w}: [${weekRounds.join(',')}]  (* = round 1)`);
}
