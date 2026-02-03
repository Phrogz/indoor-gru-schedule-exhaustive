#!/usr/bin/env node
// Trace through calculate.mjs logic to understand why invalid week 3 was generated

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

// This is the FIXED rebuildRoundMatchups from calculate.mjs
function rebuildRoundMatchups(path) {
  const roundMatchups = new Map();
  let currentRound = 0;
  let usedInRound = new Set();

  console.log('='.repeat(70));
  console.log('rebuildRoundMatchups - simulating FIXED calculate.mjs logic');
  console.log('='.repeat(70));

  for (let weekNum = 0; weekNum < path.length; weekNum++) {
    const week = path[weekNum];
    const gamesRemainingInRound = N_MATCHUPS - usedInRound.size;

    console.log(`\nWeek ${weekNum}: [${week.join(',')}]`);
    console.log(`  currentRound=${currentRound}, usedInRound.size=${usedInRound.size}, gamesRemainingInRound=${gamesRemainingInRound}`);

    if (gamesRemainingInRound <= N_SLOTS) {
      // This week straddles rounds - process required first, then extras
      const required = new Set();
      for (let m = 0; m < N_MATCHUPS; m++) {
        if (!usedInRound.has(m)) required.add(m);
      }
      console.log(`  STRADDLING: required=[${[...required].join(',')}]`);

      // First pass: add all required matchups to complete current round
      console.log(`  First pass - adding required matchups:`);
      for (const m of week) {
        if (required.has(m)) {
          usedInRound.add(m);
          console.log(`    m=${m} (${formatMatchup(m)}): REQUIRED → add to round ${currentRound}, usedInRound.size=${usedInRound.size}`);
        }
      }

      // Round should now be complete
      if (usedInRound.size === N_MATCHUPS) {
        roundMatchups.set(currentRound, new Set(usedInRound));
        console.log(`  *** Round ${currentRound} COMPLETE (${usedInRound.size} matchups). Saving and incrementing.`);
        currentRound++;
        usedInRound = new Set();
      }

      // Second pass: add all extras to start new round
      console.log(`  Second pass - adding extras to round ${currentRound}:`);
      for (const m of week) {
        if (!required.has(m)) {
          usedInRound.add(m);
          console.log(`    m=${m} (${formatMatchup(m)}): EXTRA → add to round ${currentRound}, usedInRound.size=${usedInRound.size}`);
        }
      }
    } else {
      // All matchups go to current round
      console.log(`  ALL IN ROUND ${currentRound}`);
      for (const m of week) {
        usedInRound.add(m);
      }
      console.log(`  → usedInRound.size=${usedInRound.size}: [${[...usedInRound].sort((a,b)=>a-b).join(',')}]`);
    }
  }

  // Save any remaining matchups in current round
  if (usedInRound.size > 0) {
    roundMatchups.set(currentRound, usedInRound);
    console.log(`\nSaving remaining round ${currentRound} with ${usedInRound.size} matchups`);
  }

  console.log('\n' + '-'.repeat(70));
  console.log('Final roundMatchups:');
  for (const [r, s] of roundMatchups) {
    console.log(`  Round ${r}: [${[...s].sort((a,b)=>a-b).join(',')}] (${s.size} matchups)`);
  }

  return roundMatchups;
}

// This is the exact getRoundConstraints from calculate.mjs
function getRoundConstraints(weekStartGame, roundMatchups) {
  const startRound = Math.floor(weekStartGame / N_MATCHUPS);
  const usedInRound = roundMatchups.get(startRound) || new Set();
  const gamesPlayedInRound = usedInRound.size;
  const gamesRemainingInRound = N_MATCHUPS - gamesPlayedInRound;

  console.log('\n' + '='.repeat(70));
  console.log('getRoundConstraints - what the generator sees for week 3');
  console.log('='.repeat(70));
  console.log(`  weekStartGame=${weekStartGame}`);
  console.log(`  startRound=${startRound}`);
  console.log(`  usedInRound from roundMatchups.get(${startRound}): [${[...usedInRound].sort((a,b)=>a-b).join(',')}]`);
  console.log(`  gamesPlayedInRound=${gamesPlayedInRound}`);
  console.log(`  gamesRemainingInRound=${gamesRemainingInRound}`);

  const requiredMatchups = new Set();

  if (gamesRemainingInRound <= N_SLOTS && gamesRemainingInRound > 0) {
    for (let m = 0; m < N_MATCHUPS; m++) {
      if (!usedInRound.has(m)) {
        requiredMatchups.add(m);
      }
    }
    console.log(`  requiredMatchups (gamesRemainingInRound <= N_SLOTS): [${[...requiredMatchups].join(',')}]`);
  } else {
    console.log(`  requiredMatchups: (none - gamesRemainingInRound > N_SLOTS)`);
  }

  let excludeMatchups = 0;
  if (gamesRemainingInRound >= N_SLOTS) {
    for (const m of usedInRound) {
      excludeMatchups |= (1 << m);
    }
    console.log(`  excludeMatchups: APPLIED (gamesRemainingInRound >= N_SLOTS)`);
    console.log(`    Excluded indices: [${[...usedInRound].sort((a,b)=>a-b).join(',')}]`);
    console.log(`    As matchups: ${[...usedInRound].sort((a,b)=>a-b).map(m => formatMatchup(m)).join(' ')}`);
  } else {
    console.log(`  excludeMatchups: 0 (gamesRemainingInRound < N_SLOTS, straddling)`);
  }

  return { excludeMatchups, requiredMatchups, gamesRemainingInRound };
}

// The failing schedule
const inputPath = [
  [0,6,12,2,8,26,20,15,22,16,23,14],  // Week 0
  [21,13,17,18,24,1,9,4,5,10,11,25],  // Week 1
  [0,1,7,3,9,15,24,19,25,21,27,20],   // Week 2
];

const week3 = [11,25,9,23,10,22,13,2,17,21,1,6];

console.log('Input path (weeks 0-2):');
for (let i = 0; i < inputPath.length; i++) {
  console.log(`  Week ${i}: [${inputPath[i].join(',')}]`);
  console.log(`          ${inputPath[i].map(m => formatMatchup(m)).join(' ')}`);
}
console.log(`\nWeek 3 (the problem): [${week3.join(',')}]`);
console.log(`          ${week3.map(m => formatMatchup(m)).join(' ')}`);

// Step 1: Rebuild round tracking from weeks 0-2
const roundMatchups = rebuildRoundMatchups(inputPath);

// Step 2: Get constraints for week 3
const weekStartGame = 3 * N_SLOTS;  // 36
const { excludeMatchups, requiredMatchups, gamesRemainingInRound } = getRoundConstraints(weekStartGame, roundMatchups);

// Step 3: Check which matchups in week 3 should have been excluded
console.log('\n' + '='.repeat(70));
console.log('Analysis: Which week 3 matchups violate constraints?');
console.log('='.repeat(70));

const round1Matchups = roundMatchups.get(1) || new Set();
console.log(`\nRound 1 matchups after weeks 0-2: [${[...round1Matchups].sort((a,b)=>a-b).join(',')}]`);
console.log(`As games: ${[...round1Matchups].sort((a,b)=>a-b).map(m => formatMatchup(m)).join(' ')}`);

console.log(`\nWeek 3 matchups: [${week3.join(',')}]`);
console.log(`As games: ${week3.map(m => formatMatchup(m)).join(' ')}`);

const violations = week3.filter(m => round1Matchups.has(m));
console.log(`\nVIOLATIONS (matchups in week 3 that are already in round 1):`);
console.log(`  [${violations.join(',')}] = ${violations.map(m => formatMatchup(m)).join(' ')}`);

console.log(`\nexcludeMatchups bitmask: ${excludeMatchups}`);
if (excludeMatchups === 0) {
  console.log(`  *** BUG: excludeMatchups is 0, so NO matchups are excluded!`);
  console.log(`  *** This is because gamesRemainingInRound (${gamesRemainingInRound}) >= N_SLOTS (${N_SLOTS}) is ${gamesRemainingInRound >= N_SLOTS}`);
  console.log(`  *** But wait... 20 >= 12 is TRUE, so excludeMatchups SHOULD be applied!`);
} else {
  console.log(`  Checking if violations are excluded:`);
  for (const m of violations) {
    const isExcluded = (excludeMatchups & (1 << m)) !== 0;
    console.log(`    m=${m} (${formatMatchup(m)}): excluded=${isExcluded}`);
  }
}
