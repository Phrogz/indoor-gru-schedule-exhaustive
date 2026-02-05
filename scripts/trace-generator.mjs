#!/usr/bin/env node
// Trace through calculate.mjs logic to verify constraint tracking
// Usage: node scripts/trace-generator.mjs [weeks...]
// Example: node scripts/trace-generator.mjs "0,1,7,2,8,14,19,23,24,25,26,27" "13,16,20,17,21,5,12,3,4,9,10,22" "0,5,11,6,12,16,21,14,15,18,19,22"

const TEAMS = 'ABCDEFGHIJKLMNOP';
const N_TEAMS = 8;
const N_MATCHUPS = (N_TEAMS * (N_TEAMS - 1)) / 2;  // 28
const N_SLOTS = (N_TEAMS * 3) / 2;  // 12

function decodeMatchup(matchupIdx) {
  let idx = 0;
  for (let t1 = 0; t1 < N_TEAMS - 1; t1++) {
    for (let t2 = t1 + 1; t2 < N_TEAMS; t2++) {
      if (idx === matchupIdx) return [t1, t2];
      idx++;
    }
  }
  throw new Error(`Invalid matchup index: ${matchupIdx}`);
}

function formatMatchup(m) {
  const [t1, t2] = decodeMatchup(m);
  return `${TEAMS[t1]}v${TEAMS[t2]}`;
}

// ============================================================================
// EXACT COPY of getRoundConstraints from calculate.mjs (lines 353-391)
// ============================================================================
function getRoundConstraints(weekStartGame, roundMatchups) {
  // Find the current round: the first incomplete round in roundMatchups
  let startRound = 0;
  while (roundMatchups.has(startRound) && roundMatchups.get(startRound).size === N_MATCHUPS) {
    startRound++;
  }
  const usedInRound = roundMatchups.get(startRound) || new Set();
  const gamesPlayedInRound = usedInRound.size;
  const gamesRemainingInRound = N_MATCHUPS - gamesPlayedInRound;

  const requiredMatchups = new Set();
  if (gamesRemainingInRound <= N_SLOTS && gamesRemainingInRound > 0) {
    for (let m = 0; m < N_MATCHUPS; m++) {
      if (!usedInRound.has(m)) {
        requiredMatchups.add(m);
      }
    }
  }

  let excludeMatchups = 0;
  if (gamesRemainingInRound >= N_SLOTS) {
    for (const m of usedInRound) {
      excludeMatchups |= (1 << m);
    }
  }

  return { excludeMatchups, requiredMatchups, gamesRemainingInRound, startRound, usedInRound };
}

// ============================================================================
// EXACT COPY of updateRoundMatchups from calculate.mjs (lines 394-437)
// ============================================================================
function updateRoundMatchups(weekNum, weekMatchups, roundMatchups, requiredMatchups) {
  const weekStartGame = weekNum * N_SLOTS;
  const startRound = Math.floor(weekStartGame / N_MATCHUPS);

  const newRoundMatchups = new Map();
  for (const [r, s] of roundMatchups) {
    newRoundMatchups.set(r, new Set(s));
  }

  const usedInStartRound = newRoundMatchups.get(startRound) || new Set();
  const gamesRemainingInRound = N_MATCHUPS - usedInStartRound.size;

  if (gamesRemainingInRound >= N_SLOTS) {
    if (!newRoundMatchups.has(startRound)) {
      newRoundMatchups.set(startRound, new Set());
    }
    for (const m of weekMatchups) {
      newRoundMatchups.get(startRound).add(m);
    }
  } else {
    if (!newRoundMatchups.has(startRound)) {
      newRoundMatchups.set(startRound, new Set());
    }
    if (!newRoundMatchups.has(startRound + 1)) {
      newRoundMatchups.set(startRound + 1, new Set());
    }
    for (const m of weekMatchups) {
      if (requiredMatchups.has(m)) {
        newRoundMatchups.get(startRound).add(m);
      } else {
        newRoundMatchups.get(startRound + 1).add(m);
      }
    }
  }

  return newRoundMatchups;
}

// ============================================================================
// Parse input weeks from command line or use defaults
// ============================================================================
const args = process.argv.slice(2);
let inputPath;

if (args.length > 0) {
  inputPath = args.map(arg => arg.split(',').map(n => parseInt(n.trim(), 10)));
} else {
  // Default: first valid 3-week path from 8teams-3weeks.txt
  inputPath = [
    [0,1,7,2,8,14,19,23,24,25,26,27],   // Week 0
    [13,16,20,17,21,5,12,3,4,9,10,22],  // Week 1
    [0,5,11,6,12,16,21,14,15,18,19,22]  // Week 2
  ];
}

console.log('═'.repeat(70));
console.log('TRACE: Simulating calculate.mjs constraint tracking');
console.log('═'.repeat(70));
console.log(`\nInput: ${inputPath.length} weeks, tracing constraints for week ${inputPath.length}`);

// Step through weeks, showing how roundMatchups evolves
let roundMatchups = new Map();

for (let weekNum = 0; weekNum < inputPath.length; weekNum++) {
  const week = inputPath[weekNum];
  const weekStartGame = weekNum * N_SLOTS;
  
  console.log(`\n${'─'.repeat(70)}`);
  console.log(`Week ${weekNum}: [${week.join(',')}]`);
  console.log(`         ${week.map(m => formatMatchup(m)).join(' ')}`);
  
  // Get constraints BEFORE this week (what the generator sees)
  const { excludeMatchups, requiredMatchups, gamesRemainingInRound, startRound, usedInRound } = 
    getRoundConstraints(weekStartGame, roundMatchups);
  
  console.log(`\n  Constraints for this week:`);
  console.log(`    startRound=${startRound}, usedInRound.size=${usedInRound.size}, remaining=${gamesRemainingInRound}`);
  
  if (requiredMatchups.size > 0) {
    console.log(`    requiredMatchups: [${[...requiredMatchups].join(',')}]`);
  }
  
  const excludedBits = [];
  for (let m = 0; m < N_MATCHUPS; m++) {
    if (excludeMatchups & (1 << m)) excludedBits.push(m);
  }
  if (excludedBits.length > 0) {
    console.log(`    excludeMatchups: [${excludedBits.join(',')}] (${excludedBits.length} excluded)`);
  } else {
    console.log(`    excludeMatchups: (none - week straddles or starts fresh round)`);
  }
  
  // Update round tracking AFTER this week
  roundMatchups = updateRoundMatchups(weekNum, week, roundMatchups, requiredMatchups);
  
  console.log(`\n  After week ${weekNum}:`);
  for (const [r, s] of [...roundMatchups.entries()].sort((a,b) => a[0] - b[0])) {
    const status = s.size === N_MATCHUPS ? '✓ COMPLETE' : `${s.size}/${N_MATCHUPS}`;
    console.log(`    Round ${r}: ${status}`);
  }
}

// Now show what constraints would be computed for the NEXT week
const nextWeekNum = inputPath.length;
const nextWeekStartGame = nextWeekNum * N_SLOTS;

console.log(`\n${'═'.repeat(70)}`);
console.log(`CONSTRAINTS FOR WEEK ${nextWeekNum} (what the generator will use)`);
console.log('═'.repeat(70));

const { excludeMatchups, requiredMatchups, gamesRemainingInRound, startRound, usedInRound } = 
  getRoundConstraints(nextWeekStartGame, roundMatchups);

console.log(`\nweekStartGame=${nextWeekStartGame}`);
console.log(`startRound=${startRound} (first incomplete round)`);
console.log(`usedInRound.size=${usedInRound.size}`);
console.log(`gamesRemainingInRound=${gamesRemainingInRound}`);

if (usedInRound.size > 0) {
  console.log(`\nMatchups already in round ${startRound}:`);
  console.log(`  [${[...usedInRound].sort((a,b)=>a-b).join(',')}]`);
  console.log(`  ${[...usedInRound].sort((a,b)=>a-b).map(m => formatMatchup(m)).join(' ')}`);
}

if (requiredMatchups.size > 0) {
  console.log(`\nREQUIRED matchups (must appear in week ${nextWeekNum}):`);
  console.log(`  [${[...requiredMatchups].join(',')}]`);
  console.log(`  ${[...requiredMatchups].map(m => formatMatchup(m)).join(' ')}`);
} else {
  console.log(`\nNo required matchups (gamesRemainingInRound=${gamesRemainingInRound} > N_SLOTS=${N_SLOTS})`);
}

const excludedBits = [];
for (let m = 0; m < N_MATCHUPS; m++) {
  if (excludeMatchups & (1 << m)) excludedBits.push(m);
}

if (excludedBits.length > 0) {
  console.log(`\nEXCLUDED matchups (cannot appear in week ${nextWeekNum}):`);
  console.log(`  [${excludedBits.join(',')}]`);
  console.log(`  ${excludedBits.map(m => formatMatchup(m)).join(' ')}`);
} else {
  console.log(`\nNo excluded matchups`);
  if (gamesRemainingInRound < N_SLOTS) {
    console.log(`  (gamesRemainingInRound=${gamesRemainingInRound} < N_SLOTS=${N_SLOTS}, week will straddle)`);
  }
}
