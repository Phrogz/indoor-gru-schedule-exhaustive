#!/usr/bin/env node
// Trace round-robin logic for a specific path with cumulative matchup grids

// First path from 8teams-3weeks.txt
const weeks = [
  [0,1,7,2,8,14,19,23,24,25,26,27],   // Week 0
  [13,16,20,17,21,5,12,3,4,9,10,22],  // Week 1
  [0,5,11,6,12,16,21,14,15,18,19,22]  // Week 2
];

const nTeams = 8;
const TEAMS = 'ABCDEFGH';

// Decode matchup index to team pair
function decodeMatchup(m) {
  let idx = 0;
  for (let t1 = 0; t1 < nTeams - 1; t1++) {
    for (let t2 = t1 + 1; t2 < nTeams; t2++) {
      if (idx === m) return [t1, t2];
      idx++;
    }
  }
}

// Build cumulative grid: grid[t1][t2] = how many times t1 and t2 have played
const grid = [];
for (let i = 0; i < nTeams; i++) {
  grid.push(new Array(nTeams).fill(0));
}

function printGrid(weekNum) {
  console.log(`\nAfter Week ${weekNum}:`);
  console.log('    ' + TEAMS.split('').join(' '));
  for (let t1 = 0; t1 < nTeams; t1++) {
    let row = TEAMS[t1] + '  ';
    for (let t2 = 0; t2 < nTeams; t2++) {
      if (t1 === t2) {
        row += ' -';
      } else {
        row += ' ' + grid[t1][t2];
      }
    }
    console.log(row);
  }
}

console.log('8 teams: ' + TEAMS + '\n');

for (let weekNum = 0; weekNum < weeks.length; weekNum++) {
  const week = weeks[weekNum];
  
  console.log(`Week ${weekNum}: [${week.join(',')}]`);
  console.log(`  Games: ${week.map(m => { const [t1,t2] = decodeMatchup(m); return TEAMS[t1]+'v'+TEAMS[t2]; }).join(' ')}`);
  
  // Update grid
  for (const m of week) {
    const [t1, t2] = decodeMatchup(m);
    grid[t1][t2]++;
    grid[t2][t1]++;
  }
  
  printGrid(weekNum);
  console.log();
}
