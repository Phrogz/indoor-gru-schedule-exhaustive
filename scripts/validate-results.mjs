#!/usr/bin/env node
// Validate saved schedule results for correctness
// Usage: node scripts/validate-results.mjs results/6teams-3weeks.txt

import { createReadStream } from 'fs';
import { createInterface } from 'readline';

const filePath = process.argv[2];
if (!filePath) {
  console.error('Usage: node scripts/validate-results.mjs <results-file>');
  process.exit(1);
}

// Parse header
async function parseHeader(filePath) {
  const rl = createInterface({ input: createReadStream(filePath), crlfDelay: Infinity });
  for await (const line of rl) {
    if (line.startsWith('#')) {
      const result = { teams: 0, weeks: 0, score: [0, 0], count: 0 };
      const teamsMatch = line.match(/teams=(\d+)/);
      if (teamsMatch) result.teams = parseInt(teamsMatch[1], 10);
      const weeksMatch = line.match(/weeks=(\d+)/);
      if (weeksMatch) result.weeks = parseInt(weeksMatch[1], 10);
      const scoreMatch = line.match(/score=(\d+),(\d+)/);
      if (scoreMatch) result.score = [parseInt(scoreMatch[1], 10), parseInt(scoreMatch[2], 10)];
      const countMatch = line.match(/count=(\d+)/);
      if (countMatch) result.count = parseInt(countMatch[1], 10);
      rl.close();
      return result;
    }
  }
  throw new Error('No header found');
}

// Decode matchup index to team pair
function decodeMatchup(matchupIdx, nTeams) {
  let idx = 0;
  for (let t1 = 0; t1 < nTeams - 1; t1++) {
    for (let t2 = t1 + 1; t2 < nTeams; t2++) {
      if (idx === matchupIdx) return [t1, t2];
      idx++;
    }
  }
  throw new Error(`Invalid matchup index: ${matchupIdx}`);
}

// Validate a single week schedule
function validateWeek(schedule, nTeams, nSlots, weekNum, pathNum) {
  const errors = [];
  const nMatchups = (nTeams * (nTeams - 1)) / 2;

  // Check schedule length
  if (schedule.length !== nSlots) {
    errors.push(`Week ${weekNum}: Expected ${nSlots} slots, got ${schedule.length}`);
    return errors;
  }

  // Check for duplicate matchups in same week
  const usedMatchups = new Set();
  for (let s = 0; s < schedule.length; s++) {
    const m = schedule[s];
    if (m < 0 || m >= nMatchups) {
      errors.push(`Week ${weekNum}, slot ${s}: Invalid matchup index ${m}`);
      continue;
    }
    if (usedMatchups.has(m)) {
      errors.push(`Week ${weekNum}: Duplicate matchup ${m} in same week`);
    }
    usedMatchups.add(m);
  }

  // Check each team plays exactly 3 games
  const teamGames = new Array(nTeams).fill(0);
  const teamSlots = Array.from({ length: nTeams }, () => []);

  for (let s = 0; s < schedule.length; s++) {
    const [t1, t2] = decodeMatchup(schedule[s], nTeams);
    teamGames[t1]++;
    teamGames[t2]++;
    teamSlots[t1].push(s);
    teamSlots[t2].push(s);
  }

  for (let t = 0; t < nTeams; t++) {
    if (teamGames[t] !== 3) {
      errors.push(`Week ${weekNum}: Team ${t} has ${teamGames[t]} games, expected 3`);
    }
  }

  // Check span constraint (max 5 slots)
  for (let t = 0; t < nTeams; t++) {
    if (teamSlots[t].length === 3) {
      const span = teamSlots[t][2] - teamSlots[t][0] + 1;
      if (span > 5) {
        errors.push(`Week ${weekNum}: Team ${t} has span ${span}, exceeds max 5`);
      }
    }
  }

  return errors;
}

// Score a week
function scoreWeek(schedule, nTeams) {
  const teamSlots = Array.from({ length: nTeams }, () => []);

  for (let s = 0; s < schedule.length; s++) {
    const [t1, t2] = decodeMatchup(schedule[s], nTeams);
    teamSlots[t1].push(s);
    teamSlots[t2].push(s);
  }

  let doubleByes = 0;
  let fiveSlotTeams = 0;

  for (let t = 0; t < nTeams; t++) {
    const slots = teamSlots[t];
    if (slots.length !== 3) continue;

    const span = slots[2] - slots[0] + 1;
    if (span === 5) fiveSlotTeams++;

    for (let i = 0; i < 2; i++) {
      if (slots[i + 1] - slots[i] === 3) doubleByes++;
    }
  }

  return { doubleByes, fiveSlotTeams };
}

// ╔══════════════════════════════════════════════════════════════════════════════╗
// ║  ⚠️  CRITICAL: ROUND-ROBIN CROSS-WEEK RULES  ⚠️                              ║
// ║                                                                              ║
// ║  When a week STRADDLES TWO ROUNDS, games from the NEW round may appear in    ║
// ║  ANY slot - INCLUDING BEFORE games that complete the current round!          ║
// ║                                                                              ║
// ║  The ONLY requirements are:                                                  ║
// ║  1. Within a single week, no matchup is repeated                             ║
// ║  2. Within a single round, each matchup appears exactly once                 ║
// ║  3. When a week completes a round, the required matchups must all appear     ║
// ║                                                                              ║
// ║  There is NO slot-order constraint based on round membership.                ║
// ╚══════════════════════════════════════════════════════════════════════════════╝

// Validate round-robin across weeks
// This validates that each round contains exactly all matchups before starting the next
function validateRoundRobin(weeks, nTeams) {
  const errors = [];
  const nMatchups = (nTeams * (nTeams - 1)) / 2;
  const nSlots = (nTeams * 3) / 2;

  // Track matchups used in current round
  let currentRound = 0;
  let usedInRound = new Set();

  for (let weekNum = 0; weekNum < weeks.length; weekNum++) {
    const week = weeks[weekNum];
    const weekMatchups = new Set(week);

    // Calculate remaining matchups needed to complete current round
    const remainingInRound = nMatchups - usedInRound.size;

    if (remainingInRound <= nSlots) {
      // This week should complete the current round
      // Required matchups are those not yet used in current round
      const required = [];
      for (let m = 0; m < nMatchups; m++) {
        if (!usedInRound.has(m)) required.push(m);
      }

      // Check all required matchups appear in this week
      const missingRequired = required.filter(m => !weekMatchups.has(m));
      if (missingRequired.length > 0) {
        errors.push(`Week ${weekNum}: Missing required matchups [${missingRequired.join(',')}] to complete round ${currentRound}`);
      }

      // Mark required matchups as completing current round
      for (const m of required) {
        if (weekMatchups.has(m)) {
          usedInRound.add(m);
        }
      }

      // The extra matchups in this week start the next round
      const extrasStartingNextRound = week.filter(m => !required.includes(m));

      // Check for duplicates in the extras (should not reuse matchups from required)
      const extraSet = new Set(extrasStartingNextRound);
      if (extraSet.size !== extrasStartingNextRound.length) {
        errors.push(`Week ${weekNum}: Duplicate matchups in extras starting round ${currentRound + 1}`);
      }

      // Move to next round
      currentRound++;
      usedInRound = new Set(extrasStartingNextRound);
    } else {
      // This week is entirely within current round
      // Check for reuse of matchups already in this round
      const reused = week.filter(m => usedInRound.has(m));
      if (reused.length > 0) {
        errors.push(`Week ${weekNum}: Reused matchups [${reused.join(',')}] within round ${currentRound}`);
      }

      // Add all matchups to current round
      for (const m of week) {
        usedInRound.add(m);
      }
    }
  }

  return errors;
}

// Main validation
async function validate() {
  const header = await parseHeader(filePath);
  console.log(`Validating: ${filePath}`);
  console.log(`  Teams: ${header.teams}, Weeks: ${header.weeks}, Expected count: ${header.count}, Expected score: [${header.score}]`);

  const nTeams = header.teams;
  const nWeeks = header.weeks;
  const nSlots = (nTeams * 3) / 2;
  const expectedPerWeekScore = [2, 4];

  const rl = createInterface({ input: createReadStream(filePath), crlfDelay: Infinity });

  const stack = [];
  let pathCount = 0;
  let errorCount = 0;
  let duplicateCount = 0;
  let scoreErrors = 0;
  const seenPaths = new Set();

  for await (const line of rl) {
    if (line.startsWith('#') || line.trim() === '' || line.trim() === '…') continue;

    // Count leading tabs for depth
    let depth = 0;
    while (line[depth] === '\t') depth++;

    const schedule = line.trim().split(',').map(n => parseInt(n.trim(), 10));

    // Pop stack to current depth
    while (stack.length > depth) stack.pop();
    stack.push(schedule);

    // Validate this week
    const weekErrors = validateWeek(schedule, nTeams, nSlots, depth, pathCount);
    if (weekErrors.length > 0) {
      console.error(`Path ${pathCount}, Week ${depth}:`, weekErrors);
      errorCount += weekErrors.length;
    }

    // Check week score
    const weekScore = scoreWeek(schedule, nTeams);
    if (weekScore.doubleByes !== expectedPerWeekScore[0] || weekScore.fiveSlotTeams !== expectedPerWeekScore[1]) {
      if (scoreErrors < 10) {
        console.error(`Path ${pathCount}, Week ${depth}: Score [${weekScore.doubleByes},${weekScore.fiveSlotTeams}] != expected [${expectedPerWeekScore}]`);
      }
      scoreErrors++;
    }

    // If complete path, validate round-robin and check for duplicates
    if (stack.length === nWeeks) {
      const pathKey = stack.map(w => w.join(',')).join('|');
      if (seenPaths.has(pathKey)) {
        duplicateCount++;
        if (duplicateCount <= 10) {
          console.error(`Duplicate path found: ${pathKey.substring(0, 50)}...`);
        }
      } else {
        seenPaths.add(pathKey);
      }

      const rrErrors = validateRoundRobin(stack, nTeams);
      if (rrErrors.length > 0) {
        console.error(`Path ${pathCount}:`, rrErrors);
        errorCount += rrErrors.length;
      }

      pathCount++;
      if (pathCount % 100000 === 0) {
        process.stdout.write(`\r  Validated ${pathCount} paths...`);
      }
    }
  }

  console.log(`\n\nValidation complete:`);
  console.log(`  Paths counted: ${pathCount} (expected: ${header.count})`);
  console.log(`  Unique paths: ${seenPaths.size}`);
  console.log(`  Duplicate paths: ${duplicateCount}`);
  console.log(`  Structure errors: ${errorCount}`);
  console.log(`  Score errors: ${scoreErrors}`);

  if (pathCount !== header.count) {
    console.error(`  ERROR: Path count mismatch!`);
  }
  if (duplicateCount > 0) {
    console.error(`  ERROR: ${duplicateCount} duplicate paths found!`);
  }
  if (errorCount > 0 || scoreErrors > 0) {
    console.error(`  ERROR: Validation failed!`);
    process.exit(1);
  }

  console.log(`  ✓ All paths valid`);
}

validate().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
