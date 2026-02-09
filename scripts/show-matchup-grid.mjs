#!/usr/bin/env node
// Display week-by-week cumulative matchup count grid for a schedule
//
// Usage:
//   node scripts/show-matchup-grid.mjs <file> <schedule-number>
//   node scripts/show-matchup-grid.mjs results/8teams-6weeks-best.txt 4132
//   node scripts/show-matchup-grid.mjs results/8teams-4weeks.txt 42

import { readFileSync } from 'fs';
import { isCompleteMarker, isIncompleteMarker } from '../lib/tree-format.mjs';

const TEAMS = 'ABCDEFGHIJKLMNOP';

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

// Encode team pair to matchup index
function encodeMatchup(t1Name, t2Name, nTeams) {
  const t1 = TEAMS.indexOf(t1Name);
  const t2 = TEAMS.indexOf(t2Name);
  if (t1 < 0 || t2 < 0) throw new Error(`Invalid team names: ${t1Name}, ${t2Name}`);
  const [lo, hi] = t1 < t2 ? [t1, t2] : [t2, t1];
  let idx = 0;
  for (let a = 0; a < nTeams; a++) {
    for (let b = a + 1; b < nTeams; b++) {
      if (a === lo && b === hi) return idx;
      idx++;
    }
  }
  throw new Error(`Could not encode ${t1Name}v${t2Name}`);
}

// Format matchup as "AvB"
function formatMatchup(matchupIdx, nTeams) {
  const [t1, t2] = decodeMatchup(matchupIdx, nTeams);
  return `${TEAMS[t1]}v${TEAMS[t2]}`;
}

// Print matchup grid (upper triangular)
function printMatchupGrid(matchupCounts, nTeams) {
  // Header row
  let header = '   ';
  for (let t = 1; t < nTeams; t++) {
    header += ` ${TEAMS[t]}`;
  }
  console.log(header);

  // Data rows
  for (let t1 = 0; t1 < nTeams - 1; t1++) {
    let row = ` ${TEAMS[t1]} `;
    for (let t2 = 1; t2 < nTeams; t2++) {
      if (t2 <= t1) {
        row += '  ';
      } else {
        const count = matchupCounts[t1][t2];
        row += ` ${count}`;
      }
    }
    console.log(row);
  }
}

// Show cumulative matchup grid after each week
function showMatchupGrids(weeks, nTeams) {
  // matchupCounts[t1][t2] = number of games between team t1 and t2
  const matchupCounts = Array.from({ length: nTeams }, () => new Array(nTeams).fill(0));

  for (let weekNum = 0; weekNum < weeks.length; weekNum++) {
    const week = weeks[weekNum];

    // Update matchup counts
    for (const m of week) {
      const [t1, t2] = decodeMatchup(m, nTeams);
      matchupCounts[t1][t2]++;
      matchupCounts[t2][t1]++;
    }

    // Show decoded games
    const decoded = week.map(m => formatMatchup(m, nTeams));
    console.log(`Week ${weekNum}: ${decoded.join(' ')}`);
    printMatchupGrid(matchupCounts, nTeams);
    console.log();
  }
}

// Parse a week line from evaluated file: "Week 1: AvB AvD BvD..."
function parseEvaluatedWeek(line, nTeams) {
  const match = line.match(/^Week \d+:\s+(.+)$/);
  if (!match) return null;
  const games = match[1].split(/\s+/);
  return games.map(g => {
    const [t1, t2] = g.split('v');
    return encodeMatchup(t1, t2, nTeams);
  });
}

// Parse a week line from tree format: comma-separated indices with optional leading tabs
function parseTreeWeek(line) {
  const trimmed = line.replace(/^\t+/, '');
  if (!trimmed || trimmed.startsWith('#') || isIncompleteMarker(trimmed) || isCompleteMarker(trimmed)) return null;
  return trimmed.split(',').map(Number);
}

// Load schedule from evaluated file by schedule ID (e.g., 7969379)
function loadFromEvaluatedFile(filePath, scheduleId, nTeams) {
  const content = readFileSync(filePath, 'utf8');
  const lines = content.split('\n');

  let inTargetSchedule = false;
  let weeks = [];

  for (const line of lines) {
    if (line.startsWith('Schedule ')) {
      // Check if this is our target schedule
      const match = line.match(/^Schedule (\d+)\//);
      if (match) {
        const thisId = parseInt(match[1]);
        if (inTargetSchedule && weeks.length > 0) {
          // We just finished the target schedule, stop
          break;
        }
        if (thisId === scheduleId) {
          inTargetSchedule = true;
          weeks = [];
        } else {
          inTargetSchedule = false;
        }
      }
    } else if (inTargetSchedule && line.startsWith('Week ')) {
      const week = parseEvaluatedWeek(line, nTeams);
      if (week) weeks.push(week);
    }
  }

  if (weeks.length === 0) {
    throw new Error(`Schedule ${scheduleId} not found in file`);
  }

  return weeks;
}

// Load schedule from tree format file by path number (1-indexed)
function loadFromTreeFile(filePath, pathNum, nTeams, nWeeks) {
  const content = readFileSync(filePath, 'utf8');
  const lines = content.split('\n');

  const stack = []; // Current path being built
  let pathCount = 0;

  for (const line of lines) {
    const trimmed = line.trim();
    if (line.startsWith('#') || trimmed === '' || isIncompleteMarker(trimmed) || isCompleteMarker(trimmed)) continue;

    const depth = line.match(/^\t*/)[0].length;
    const week = parseTreeWeek(line);
    if (!week) continue;

    // Trim stack to depth, then push new week
    stack.length = depth;
    stack.push(week);

    // If we've built a full path
    if (stack.length === nWeeks) {
      pathCount++;
      if (pathCount === pathNum) {
        return stack.map(w => Array.from(w));
      }
    }
  }

  throw new Error(`Path ${pathNum} not found (only ${pathCount} paths in file)`);
}

// Parse header to get teams and weeks
function parseHeader(filePath) {
  const content = readFileSync(filePath, 'utf8');
  const firstLine = content.split('\n')[0];

  const teamsMatch = firstLine.match(/teams=(\d+)/);
  const weeksMatch = firstLine.match(/weeks=(\d+)/);

  if (!teamsMatch || !weeksMatch) {
    throw new Error(`Could not parse header: ${firstLine}`);
  }

  return {
    nTeams: parseInt(teamsMatch[1]),
    nWeeks: parseInt(weeksMatch[1])
  };
}

// Detect file type from content
function detectFileType(filePath) {
  const content = readFileSync(filePath, 'utf8');
  const lines = content.split('\n').slice(0, 10);

  for (const line of lines) {
    if (line.startsWith('Schedule ')) return 'evaluated';
    if (line.match(/^\t*\d+,/)) return 'tree';
  }

  return 'tree'; // default
}

// Main
const args = process.argv.slice(2);

// Parse arguments
let filePath = null;
let scheduleId = null;
let pathNum = null;

for (let i = 0; i < args.length; i++) {
  if (args[i] === '--schedule' && i + 1 < args.length) {
    scheduleId = parseInt(args[++i]);
  } else if (args[i] === '--path' && i + 1 < args.length) {
    pathNum = parseInt(args[++i]);
  } else if (!filePath) {
    filePath = args[i];
  }
}

if (!filePath || (!scheduleId && !pathNum)) {
  console.log('Usage:');
  console.log('  node scripts/show-matchup-grid.mjs <file> --schedule <id>');
  console.log('  node scripts/show-matchup-grid.mjs <file> --path <num>');
  console.log('');
  console.log('Examples:');
  console.log('  node scripts/show-matchup-grid.mjs results/8teams-6weeks-evaluated.txt --schedule 7969379');
  console.log('  node scripts/show-matchup-grid.mjs results/8teams-4weeks.txt --path 42');
  process.exit(1);
}

const { nTeams, nWeeks } = parseHeader(filePath);
const fileType = detectFileType(filePath);

let weeks;
if (scheduleId) {
  if (fileType !== 'evaluated') {
    console.error('--schedule only works with evaluated files');
    process.exit(1);
  }
  console.log(`Loading schedule ${scheduleId} from ${filePath}`);
  console.log(`Teams: ${nTeams}, Weeks: ${nWeeks}`);
  console.log('');
  weeks = loadFromEvaluatedFile(filePath, scheduleId, nTeams);
} else {
  console.log(`Loading path ${pathNum} from ${filePath}`);
  console.log(`Teams: ${nTeams}, Weeks: ${nWeeks}`);
  console.log('');
  weeks = loadFromTreeFile(filePath, pathNum, nTeams, nWeeks);
}

showMatchupGrids(weeks, nTeams);
