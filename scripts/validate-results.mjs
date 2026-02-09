#!/usr/bin/env node
// Validate saved schedule results for correctness
// Usage: node scripts/validate-results.mjs results/6teams-3weeks.txt [--fix]
// With --fix: removes invalid paths and rewrites the file

import { createReadStream, writeFileSync, renameSync } from 'fs';
import { createInterface } from 'readline';
import { isCompleteMarker, isIncompleteMarker } from '../lib/tree-format.mjs';

const args = process.argv.slice(2);
const fixMode = args.includes('--fix');
const filePath = args.find(a => !a.startsWith('--'));

if (!filePath) {
  console.error('Usage: node scripts/validate-results.mjs <results-file> [--fix]');
  console.error('  --fix: Remove invalid paths and rewrite the file');
  process.exit(1);
}

// Parse header
async function parseHeader(filePath) {
  const rl = createInterface({ input: createReadStream(filePath), crlfDelay: Infinity });
  for await (const line of rl) {
    if (line.startsWith('#')) {
      const result = { teams: 0, weeks: 0, count: 0 };
      const teamsMatch = line.match(/teams=(\d+)/);
      if (teamsMatch) result.teams = parseInt(teamsMatch[1], 10);
      const weeksMatch = line.match(/weeks=(\d+)/);
      if (weeksMatch) result.weeks = parseInt(weeksMatch[1], 10);
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
  
  // Check for NaN or non-finite values
  if (schedule.some(v => !Number.isFinite(v))) {
    errors.push(`Week ${weekNum}: Schedule contains invalid values (NaN or non-finite)`);
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
  const nMatchups = (nTeams * (nTeams - 1)) / 2;

  for (let s = 0; s < schedule.length; s++) {
    const m = schedule[s];
    // Skip invalid matchups (NaN, out of range)
    if (!Number.isFinite(m) || m < 0 || m >= nMatchups) {
      return { doubleByes: -1, fiveSlotTeams: -1 };  // Invalid score
    }
    const [t1, t2] = decodeMatchup(m, nTeams);
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

  // Check for invalid values in any week first
  for (let weekNum = 0; weekNum < weeks.length; weekNum++) {
    const week = weeks[weekNum];
    if (week.some(m => !Number.isFinite(m) || m < 0 || m >= nMatchups)) {
      errors.push(`Week ${weekNum}: Contains invalid matchup values`);
      return errors;  // Can't validate round-robin with invalid data
    }
  }

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
  console.log(`Validating: ${filePath}${fixMode ? ' [FIX MODE]' : ''}`);
  console.log(`  Teams: ${header.teams}, Weeks: ${header.weeks}, Expected count: ${header.count}`);
  
  if (header.count > 10_000_000) {
    console.log(`  Note: Duplicate detection disabled for large files (>10M paths)`);
  }

  const nTeams = header.teams;
  const nWeeks = header.weeks;
  const nSlots = (nTeams * 3) / 2;
  const expectedPerWeekScore = [2, 4];

  const rl = createInterface({ input: createReadStream(filePath), crlfDelay: Infinity });

  const stack = [];
  let pathCount = 0;
  let validPathCount = 0;
  let errorCount = 0;
  let duplicateCount = 0;
  let scoreErrors = 0;
  let roundRobinErrors = 0;
  
  // For --fix mode: collect valid paths
  const validPaths = fixMode ? [] : null;
  
  // For very large files (>10M paths), skip duplicate detection to avoid Set size limits
  // Structure validation is more important and will catch most issues
  const enableDuplicateDetection = header.count <= 10_000_000;
  
  // Use hash-based duplicate detection for medium files (avoids Set size limits)
  // Uses 64-bit BigInt hash to minimize collision probability
  function hashPath(pathKey) {
    let hash = 5381n;
    for (let i = 0; i < pathKey.length; i++) {
      hash = ((hash << 5n) + hash) + BigInt(pathKey.charCodeAt(i));
      hash = hash & 0xFFFFFFFFFFFFFFFFn; // Keep 64 bits
    }
    return hash;
  }
  
  const seenPathHashes = enableDuplicateDetection ? new Set() : null;

  for await (const line of rl) {
    const trimmed = line.trim();
    if (line.startsWith('#') || trimmed === '' || isIncompleteMarker(trimmed) || isCompleteMarker(trimmed)) continue;

    // Count leading tabs for depth
    let depth = 0;
    while (line[depth] === '\t') depth++;

    const schedule = line.trim().split(',').map(n => parseInt(n.trim(), 10));

    // Pop stack to current depth
    while (stack.length > depth) stack.pop();
    stack.push(schedule);

    // Validate this week
    const weekErrors = validateWeek(schedule, nTeams, nSlots, depth, pathCount);
    if (weekErrors.length > 0 && !fixMode) {
      console.error(`Path ${pathCount}, Week ${depth}:`, weekErrors);
    }

    // Check week score (skip if week already has validation errors)
    let hasScoreError = false;
    if (weekErrors.length === 0) {
      const weekScore = scoreWeek(schedule, nTeams);
      hasScoreError = weekScore.doubleByes !== expectedPerWeekScore[0] || weekScore.fiveSlotTeams !== expectedPerWeekScore[1];
      if (hasScoreError && !fixMode && scoreErrors < 10) {
        console.error(`Path ${pathCount}, Week ${depth}: Score [${weekScore.doubleByes},${weekScore.fiveSlotTeams}] != expected [${expectedPerWeekScore}]`);
      }
    } else {
      hasScoreError = true;  // Treat validation errors as score errors for counting
    }

    // If complete path, validate round-robin and check for duplicates
    if (stack.length === nWeeks) {
      let pathHasErrors = weekErrors.length > 0 || hasScoreError;
      
      // Duplicate detection (skipped for very large files)
      let isDuplicate = false;
      if (enableDuplicateDetection) {
        const pathKey = stack.map(w => w.join(',')).join('|');
        const pathHash = hashPath(pathKey);
        if (seenPathHashes.has(pathHash)) {
          isDuplicate = true;
          duplicateCount++;
          if (duplicateCount <= 10 && !fixMode) {
            console.error(`Duplicate path found: ${pathKey.substring(0, 50)}...`);
          }
        } else {
          seenPathHashes.add(pathHash);
        }
      }

      const rrErrors = validateRoundRobin(stack, nTeams);
      if (rrErrors.length > 0) {
        if (!fixMode) {
          console.error(`Path ${pathCount}:`, rrErrors);
        }
        roundRobinErrors++;
        pathHasErrors = true;
      }
      
      // Count errors
      if (weekErrors.length > 0) errorCount += weekErrors.length;
      if (hasScoreError) scoreErrors++;
      
      // Track valid paths for --fix mode
      if (fixMode && !pathHasErrors && !isDuplicate) {
        validPaths.push(stack.map(w => [...w]));
        validPathCount++;
      } else if (!pathHasErrors && !isDuplicate) {
        validPathCount++;
      }

      pathCount++;
      if (pathCount % 100000 === 0) {
        process.stdout.write(`\r  Validated ${pathCount} paths...`);
      }
    }
  }

  console.log(`\n\nValidation complete:`);
  console.log(`  Paths counted: ${pathCount} (expected: ${header.count})`);
  console.log(`  Valid paths: ${validPathCount}`);
  console.log(`  Invalid paths: ${pathCount - validPathCount}`);
  if (enableDuplicateDetection) {
    console.log(`  Duplicate paths: ${duplicateCount}`);
  } else {
    console.log(`  Duplicate detection: Skipped (file too large, >10M paths)`);
  }
  console.log(`  Structure errors: ${errorCount}`);
  console.log(`  Score errors: ${scoreErrors}`);
  console.log(`  Round-robin errors: ${roundRobinErrors}`);

  if (pathCount !== header.count) {
    console.error(`  WARNING: Path count mismatch!`);
  }
  
  // --fix mode: rewrite file with only valid paths
  if (fixMode && validPathCount > 0 && validPathCount < pathCount) {
    console.log(`\n  Writing ${validPathCount} valid paths to ${filePath}...`);
    
    // Sort paths for consistent tree output
    validPaths.sort((a, b) => {
      for (let w = 0; w < a.length; w++) {
        const aKey = a[w].join(',');
        const bKey = b[w].join(',');
        if (aKey < bKey) return -1;
        if (aKey > bKey) return 1;
      }
      return 0;
    });
    
    // Write in tree format
    const lines = [`# teams=${nTeams} weeks=${nWeeks} count=${validPathCount}`];
    let lastPath = null;
    
    for (const path of validPaths) {
      // Find common prefix with last path
      let commonDepth = 0;
      if (lastPath) {
        while (commonDepth < lastPath.length && 
               commonDepth < path.length &&
               lastPath[commonDepth].join(',') === path[commonDepth].join(',')) {
          commonDepth++;
        }
      }
      
      // Write from divergence point
      for (let d = commonDepth; d < path.length; d++) {
        lines.push('\t'.repeat(d) + path[d].join(','));
      }
      lastPath = path;
    }
    
    // Backup original and write new
    const backupPath = filePath + '.backup';
    renameSync(filePath, backupPath);
    writeFileSync(filePath, lines.join('\n') + '\n');
    console.log(`  Original backed up to ${backupPath}`);
    console.log(`  ✓ Fixed file written with ${validPathCount} valid paths`);
  } else if (fixMode && validPathCount === 0) {
    console.error(`  ERROR: No valid paths found! File not modified.`);
    process.exit(1);
  } else if (fixMode && validPathCount === pathCount) {
    console.log(`  All paths already valid, no changes needed.`);
  } else if (errorCount > 0 || scoreErrors > 0 || roundRobinErrors > 0) {
    console.error(`  ERROR: Validation failed!`);
    if (!fixMode) {
      console.error(`  Run with --fix to remove invalid paths`);
    }
    process.exit(1);
  } else {
    console.log(`  ✓ All paths valid`);
  }
}

validate().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
