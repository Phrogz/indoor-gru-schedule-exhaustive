#!/usr/bin/env node
// Cleanup tool: Reads a tree file and rewrites with optimal prefix compression
// Uses streaming I/O and external sort to handle arbitrarily large files
// Usage: node scripts/cleanup-tree.mjs results/8teams-4weeks.txt

import { createReadStream, createWriteStream, unlinkSync } from 'fs';
import { createInterface } from 'readline';
import { execSync } from 'child_process';
import { tmpdir } from 'os';
import { join } from 'path';

const inputFile = process.argv[2];
if (!inputFile) {
  console.error('Usage: node scripts/cleanup-tree.mjs <file>');
  console.error('Example: node scripts/cleanup-tree.mjs results/8teams-4weeks.txt');
  process.exit(1);
}

console.log(`Reading ${inputFile}...`);
const start = performance.now();

const tempFile = join(tmpdir(), `cleanup-${process.pid}-${Date.now()}.txt`);
const sortedFile = tempFile + '.sorted';

// Cleanup temp files on exit
function cleanupTemp() {
  try { unlinkSync(tempFile); } catch {}
  try { unlinkSync(sortedFile); } catch {}
}
process.on('exit', cleanupTemp);
process.on('SIGINT', () => { cleanupTemp(); process.exit(130); });

// ── Phase 1: Stream input, extract flat paths to temp file, track incomplete parents ──

let teams, weeks;
let targetWeeks, numTeams, slotsPerWeek;
let parentDepth, leafDepth;
let pathCount = 0;
// Memory note: parentFinalState can still grow large for huge inputs; for extreme cases, a two-pass stream can avoid tracking all parents.
const parentFinalState = new Map();  // key -> {hasChildren, markedIncomplete}

{
  const rl = createInterface({ input: createReadStream(inputFile), crlfDelay: Infinity });
  const tempStream = createWriteStream(tempFile);

  let lineNum = 0;
  let currentPath = [];   // stores schedule strings (not parsed arrays)
  let currentParentKey = null;
  let currentParentHasChildren = false;
  let currentParentMarkedIncomplete = false;
  let warningCount = 0;
  const MAX_WARNINGS = 10;

  function finalizeParent() {
    if (currentParentKey !== null) {
      parentFinalState.set(currentParentKey, {
        hasChildren: currentParentHasChildren,
        markedIncomplete: currentParentMarkedIncomplete
      });
    }
  }

  for await (const line of rl) {
    lineNum++;

    // ── Header ──
    if (lineNum === 1) {
      if (!line.startsWith('#')) { console.error('Error: File does not have a valid header'); process.exit(1); }
      const m = line.match(/teams=(\d+).*weeks=(\d+).*count=(\d+)/);
      if (!m) { console.error('Error: Could not parse header'); process.exit(1); }
      teams = m[1]; weeks = m[2];
      const sourceIsPartial = line.includes('(partial)');
      console.log(`  teams=${teams}, weeks=${weeks}, count=${m[3]}${sourceIsPartial ? ' (partial)' : ''}`);
      targetWeeks = parseInt(weeks);
      numTeams = parseInt(teams);
      slotsPerWeek = 3 * (numTeams / 2);
      parentDepth = targetWeeks - 2;
      leafDepth = targetWeeks - 1;
      console.log('Extracting paths...');
      continue;
    }

    if (!line.trim()) continue;

    const depth = line.match(/^\t*/)[0].length;
    const content = line.trim();

    // Handle incomplete markers
    if (content === '…') {
      if (depth === leafDepth && currentParentKey !== null) {
        currentParentMarkedIncomplete = true;
      }
      continue;
    }

    // Validate schedule
    const parts = content.split(',');
    if (parts.length !== slotsPerWeek || parts.some(p => p === '' || !Number.isFinite(Number(p)))) {
      warningCount++;
      if (warningCount <= MAX_WARNINGS) {
        console.log(`  WARNING: Skipping invalid schedule at line ${lineNum}: ${content}`);
      }
      continue;
    }

    if (depth <= parentDepth) {
      finalizeParent();
      currentPath = currentPath.slice(0, depth);
      currentPath.push(content);

      if (depth === parentDepth) {
        currentParentKey = currentPath.join('|');
        currentParentHasChildren = false;
        currentParentMarkedIncomplete = false;
      } else {
        currentParentKey = null;
      }
    } else {
      currentPath = currentPath.slice(0, depth);
      currentPath.push(content);

      if (depth === leafDepth) {
        currentParentHasChildren = true;
        pathCount++;
        tempStream.write(currentPath.join('|') + '\n');
      }
    }

    if (lineNum % 5_000_000 === 0) {
      console.log(`  ...processed ${lineNum.toLocaleString()} lines, ${pathCount.toLocaleString()} paths`);
    }
  }

  finalizeParent();

  if (warningCount > MAX_WARNINGS) {
    console.log(`  ...${(warningCount - MAX_WARNINGS).toLocaleString()} more invalid schedule warnings suppressed`);
  }

  await new Promise((resolve, reject) => { tempStream.end(resolve); tempStream.on('error', reject); });
  console.log(`  Extracted ${pathCount.toLocaleString()} paths`);
}

// Collect incomplete parent keys, then free the map
const incompleteParentKeys = new Set();
for (const [key, state] of parentFinalState) {
  if (state.markedIncomplete) incompleteParentKeys.add(key);
}
parentFinalState.clear();
console.log(`  ${incompleteParentKeys.size.toLocaleString()} parent paths remain incomplete (last occurrence had marker)`);

if (pathCount === 0) {
  const outputFile = inputFile.replace('.txt', '-clean.txt');
  const { writeFileSync } = await import('fs');
  writeFileSync(outputFile, `# teams=${teams} weeks=${weeks} count=0\n`);
  console.log(`\nNo paths found. Wrote empty output to ${outputFile}`);
  process.exit(0);
}

// ── Phase 2: External sort + deduplicate ──

console.log('Sorting and deduplicating (external sort)...');
execSync(`LC_ALL=C sort -u -o "${sortedFile}" "${tempFile}"`, { maxBuffer: 10 * 1024 * 1024 });
try { unlinkSync(tempFile); } catch {}

const uniqueCount = parseInt(execSync(`wc -l < "${sortedFile}"`).toString().trim());
const duplicatesRemoved = pathCount - uniqueCount;
if (duplicatesRemoved > 0) {
  console.log(`  Removed ${duplicatesRemoved.toLocaleString()} duplicates`);
}
console.log(`  ${uniqueCount.toLocaleString()} unique paths`);

// ── Phase 3: Stream sorted file, write compressed output ──

console.log('Writing compressed output...');
const outputFile = inputFile.replace('.txt', '-clean.txt');

{
  const rl = createInterface({ input: createReadStream(sortedFile), crlfDelay: Infinity });
  const outStream = createWriteStream(outputFile);

  const hasIncomplete = incompleteParentKeys.size > 0;
  const partialMarker = hasIncomplete ? ' (partial)' : '';
  outStream.write(`# teams=${teams} weeks=${weeks} count=${uniqueCount}${partialMarker}\n`);

  let prevParts = [];
  let lastParentKey = null;
  let needsIncompleteMarker = false;
  let outputLineCount = 1;  // header
  const linesPerDepth = new Array(targetWeeks).fill(0);
  let incompleteMarkerCount = 0;

  for await (const line of rl) {
    const parts = line.split('|');

    // Check parent change for incomplete markers
    const parentKey = parts.slice(0, targetWeeks - 1).join('|');

    if (lastParentKey !== null && lastParentKey !== parentKey) {
      if (needsIncompleteMarker) {
        outStream.write('\t'.repeat(leafDepth) + '…\n');
        outputLineCount++;
        incompleteMarkerCount++;
      }
    }

    // Find common prefix depth
    let commonDepth = 0;
    while (commonDepth < prevParts.length &&
           commonDepth < parts.length &&
           prevParts[commonDepth] === parts[commonDepth]) {
      commonDepth++;
    }

    // Write nodes from divergence point onwards
    for (let depth = commonDepth; depth < parts.length; depth++) {
      outStream.write('\t'.repeat(depth) + parts[depth] + '\n');
      outputLineCount++;
      if (depth < targetWeeks) linesPerDepth[depth]++;
    }

    prevParts = parts;
    lastParentKey = parentKey;
    needsIncompleteMarker = incompleteParentKeys.has(parentKey);
  }

  // Last parent's incomplete marker
  if (needsIncompleteMarker) {
    outStream.write('\t'.repeat(leafDepth) + '…\n');
    outputLineCount++;
    incompleteMarkerCount++;
  }

  await new Promise((resolve, reject) => { outStream.end(resolve); outStream.on('error', reject); });

  try { unlinkSync(sortedFile); } catch {}

  const elapsed = ((performance.now() - start) / 1000).toFixed(2);
  console.log(`\nDone in ${elapsed}s`);
  console.log(`Output: ${outputFile}${partialMarker}`);
  console.log(`Lines: ${outputLineCount.toLocaleString()}`);
  if (incompleteMarkerCount > 0) {
    console.log(`Incomplete markers preserved: ${incompleteMarkerCount.toLocaleString()}`);
  }
  console.log('\nCompression stats by depth:');
  for (let d = 0; d < targetWeeks; d++) {
    console.log(`  Week ${d + 1}: ${linesPerDepth[d].toLocaleString()} lines`);
  }
}
