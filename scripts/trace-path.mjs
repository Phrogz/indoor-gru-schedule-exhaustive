#!/usr/bin/env node
// Trace round-robin logic for a specific path

const weeks = [
  [0,6,12,2,8,26,20,15,22,16,23,14],
  [21,13,17,18,24,1,9,4,5,10,11,25],
  [0,1,7,3,9,15,24,19,25,21,27,20],
  [11,25,9,23,10,22,13,2,17,21,1,6]
];

const nTeams = 8;
const nMatchups = (nTeams * (nTeams - 1)) / 2;  // 28
const nSlots = (nTeams * 3) / 2;  // 12

console.log('nMatchups:', nMatchups, 'nSlots:', nSlots);
console.log();

let currentRound = 0;
let usedInRound = new Set();

for (let weekNum = 0; weekNum < weeks.length; weekNum++) {
  const week = weeks[weekNum];
  const weekMatchups = new Set(week);

  const remainingInRound = nMatchups - usedInRound.size;
  console.log(`Week ${weekNum}: usedInRound.size=${usedInRound.size}, remainingInRound=${remainingInRound}`);
  console.log(`  Matchups in week: ${week.join(',')}`);

  if (remainingInRound <= nSlots) {
    console.log(`  -> Completing round ${currentRound}`);

    // Required matchups to complete current round
    const required = [];
    for (let m = 0; m < nMatchups; m++) {
      if (!usedInRound.has(m)) required.push(m);
    }
    console.log(`  Required: ${required.join(',')}`);

    const missingRequired = required.filter(m => !weekMatchups.has(m));
    if (missingRequired.length > 0) {
      console.log(`  ❌ Missing required: ${missingRequired.join(',')}`);
    }

    // Mark required as used in current round
    for (const m of required) {
      if (weekMatchups.has(m)) {
        usedInRound.add(m);
      }
    }

    // Extras start next round
    const extras = week.filter(m => !required.includes(m));
    console.log(`  Extras starting round ${currentRound + 1}: ${extras.join(',')}`);

    currentRound++;
    usedInRound = new Set(extras);
    console.log(`  Now in round ${currentRound}, usedInRound: {${[...usedInRound].join(',')}}`);
  } else {
    console.log(`  -> Adding to round ${currentRound}`);

    const reused = week.filter(m => usedInRound.has(m));
    if (reused.length > 0) {
      console.log(`  ❌ Reused in round: ${reused.join(',')}`);
    }

    for (const m of week) {
      usedInRound.add(m);
    }
    console.log(`  usedInRound now has ${usedInRound.size} matchups`);
  }
  console.log();
}
