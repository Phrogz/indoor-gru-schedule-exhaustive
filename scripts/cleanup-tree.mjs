#!/usr/bin/env node
// Cleanup tool: Reads a tree file and rewrites with optimal prefix compression
// Usage: node --max-old-space-size=8192 scripts/cleanup-tree.mjs results/8teams-4weeks.txt

import { readFileSync, writeFileSync } from 'fs';

const inputFile = process.argv[2];
if (!inputFile) {
  console.error('Usage: node --max-old-space-size=8192 scripts/cleanup-tree.mjs <file>');
  console.error('Example: node --max-old-space-size=8192 scripts/cleanup-tree.mjs results/8teams-4weeks.txt');
  process.exit(1);
}

console.log(`Reading ${inputFile}...`);
const start = performance.now();

const content = readFileSync(inputFile, 'utf8');
const lines = content.split('\n');

// Parse header
const headerLine = lines[0];
if (!headerLine.startsWith('#')) {
  console.error('Error: File does not have a valid header');
  process.exit(1);
}

// Extract header info
const headerMatch = headerLine.match(/teams=(\d+).*weeks=(\d+).*count=(\d+)/);
if (!headerMatch) {
  console.error('Error: Could not parse header');
  process.exit(1);
}

const [, teams, weeks, count] = headerMatch;
const sourceIsPartial = headerLine.includes('(partial)');
console.log(`  teams=${teams}, weeks=${weeks}, count=${count}${sourceIsPartial ? ' (partial)' : ''}`);

// Extract all complete paths and track the FINAL state of each parent path
// A parent is incomplete only if its LAST occurrence in the file has an incomplete marker
// (If a later pass completed the exploration, the parent should NOT be marked incomplete)
console.log('Extracting paths...');
const paths = [];
const parentFinalState = new Map();  // key -> {hasChildren: bool, markedIncomplete: bool}
let currentPath = [];
let currentParentKey = null;
let currentParentHasChildren = false;
let currentParentMarkedIncomplete = false;

const targetWeeks = parseInt(weeks);
const parentDepth = targetWeeks - 2;  // Depth of parent nodes (0-indexed, e.g., depth 1 for 3-week file)
const leafDepth = targetWeeks - 1;    // Depth of leaf nodes (e.g., depth 2 for 3-week file)

function finalizeParent() {
  if (currentParentKey !== null) {
    // Update or set the final state for this parent
    // Each time we see a parent, we overwrite its state (so last occurrence wins)
    parentFinalState.set(currentParentKey, {
      hasChildren: currentParentHasChildren,
      markedIncomplete: currentParentMarkedIncomplete
    });
  }
}

for (let i = 1; i < lines.length; i++) {
  const line = lines[i];
  if (!line.trim()) continue;
  
  const depth = line.match(/^\t*/)[0].length;
  const content = line.trim();
  
  // Handle incomplete markers
  if (content === '…') {
    // Mark current parent as incomplete (if we're at leaf depth, meaning parent is incomplete)
    if (depth === leafDepth && currentParentKey !== null) {
      currentParentMarkedIncomplete = true;
    }
    continue;
  }
  
  const schedule = content.split(',').map(Number);
  
  // Skip invalid schedules (NaN values, wrong length, etc.)
  if (schedule.some(n => !Number.isFinite(n)) || schedule.length !== 12) {
    console.log(`  WARNING: Skipping invalid schedule at line ${i + 1}: ${content}`);
    continue;
  }
  
  // Check if we're changing parents
  if (depth <= parentDepth) {
    // We're at or above parent depth - finalize previous parent
    finalizeParent();
    
    // Update current path
    currentPath = currentPath.slice(0, depth);
    currentPath.push(schedule);
    
    // If we're exactly at parent depth, start tracking a new parent
    if (depth === parentDepth) {
      currentParentKey = currentPath.map(s => s.join(',')).join('|');
      currentParentHasChildren = false;
      currentParentMarkedIncomplete = false;
    } else {
      // We're above parent depth, no current parent
      currentParentKey = null;
    }
  } else {
    // We're below parent depth (at leaf level)
    currentPath = currentPath.slice(0, depth);
    currentPath.push(schedule);
    
    if (depth === leafDepth) {
      currentParentHasChildren = true;
      paths.push(currentPath.map(s => [...s])); // Deep copy
    }
  }
}

// Don't forget to finalize the last parent
finalizeParent();

// Count how many parents are incomplete (last occurrence had marker)
// If a parent's LAST occurrence has an incomplete marker, more children may exist
// (regardless of whether any children were found in that final occurrence)
const incompleteParentKeys = new Set();
let completedInLaterPass = 0;
for (const [key, state] of parentFinalState) {
  if (state.markedIncomplete) {
    // Last occurrence had marker - this parent is incomplete
    incompleteParentKeys.add(key);
  } else if (state.hasChildren) {
    // Last occurrence had children but NO marker - completed in a later pass
    // (We don't need to track these, but count for info)
  }
}

// Count parents that had markers in earlier passes but were completed later
// (Their last occurrence has children but no marker)
for (const [key, state] of parentFinalState) {
  if (!state.markedIncomplete && state.hasChildren) {
    // This parent's last occurrence was complete - check if earlier occurrences had markers
    // (We can't easily tell from this data, but the difference from total parents gives us a clue)
  }
}

console.log(`  Extracted ${paths.length} complete paths`);
console.log(`  ${incompleteParentKeys.size} parent paths remain incomplete (last occurrence had marker)`);

// Free memory from original content
lines.length = 0;

// Sort paths lexicographically
console.log('Sorting paths...');
paths.sort((a, b) => {
  for (let i = 0; i < Math.min(a.length, b.length); i++) {
    const cmp = a[i].join(',').localeCompare(b[i].join(','));
    if (cmp !== 0) return cmp;
  }
  return a.length - b.length;
});

// Deduplicate using a Set for exactness
console.log('Deduplicating...');
const beforeDedup = paths.length;
const pathToKey = p => p.map(s => s.join(',')).join('|');
const seenKeys = new Set();
let writeIdx = 0;
for (let i = 0; i < paths.length; i++) {
  const currKey = pathToKey(paths[i]);
  if (!seenKeys.has(currKey)) {
    seenKeys.add(currKey);
    paths[writeIdx] = paths[i];
    writeIdx++;
  }
}
paths.length = writeIdx;
const duplicatesRemoved = beforeDedup - paths.length;
if (duplicatesRemoved > 0) {
  console.log(`  Removed ${duplicatesRemoved.toLocaleString()} duplicates`);
}

// Write with optimal prefix compression, adding … markers for incomplete branches
console.log('Writing compressed output...');
const outputLines = [];
let prevPath = [];
let lastParentKey = null;
let needsIncompleteMarker = false;

for (const path of paths) {
  // Check if we're changing parents (at depth weeks-1)
  const parentKey = path.slice(0, parseInt(weeks) - 1).map(s => s.join(',')).join('|');
  
  if (lastParentKey !== null && lastParentKey !== parentKey) {
    // Parent changed - check if previous parent was incomplete
    if (needsIncompleteMarker) {
      outputLines.push('\t'.repeat(leafDepth) + '…');
    }
  }
  
  // Find common prefix depth
  let commonDepth = 0;
  while (commonDepth < prevPath.length && 
         commonDepth < path.length &&
         prevPath[commonDepth].join(',') === path[commonDepth].join(',')) {
    commonDepth++;
  }
  
  // Write nodes from divergence point onwards
  for (let depth = commonDepth; depth < path.length; depth++) {
    outputLines.push('\t'.repeat(depth) + path[depth].join(','));
  }
  
  prevPath = path;
  lastParentKey = parentKey;
  needsIncompleteMarker = incompleteParentKeys.has(parentKey);
}

// Don't forget the last parent's incomplete marker
if (needsIncompleteMarker) {
  outputLines.push('\t'.repeat(leafDepth) + '…');
}

// Count unique prefixes at each depth for stats (skip incomplete markers)
const uniqueByDepth = [];
for (let d = 0; d < parseInt(weeks); d++) {
  uniqueByDepth.push(new Set());
}
currentPath = [];
for (const line of outputLines) {
  if (line.trim() === '…') continue;  // Skip incomplete markers in stats
  const depth = line.match(/^\t*/)[0].length;
  if (depth >= parseInt(weeks)) continue;  // Safety check
  currentPath = currentPath.slice(0, depth);
  currentPath.push(line.trim());
  uniqueByDepth[depth].add(currentPath.join('|'));
}

// Determine if output should be marked partial
// Only mark as partial if there are ACTUAL incomplete markers in the output
// (source being partial doesn't matter if all branches are now complete)
const hasIncompleteMarkers = outputLines.some(l => l.trim() === '…');
const outputIsPartial = hasIncompleteMarkers;

// Write output
const partialMarker = outputIsPartial ? ' (partial)' : '';
const header = `# teams=${teams} weeks=${weeks} count=${paths.length}${partialMarker}`;
const outputFile = inputFile.replace('.txt', '-clean.txt');
writeFileSync(outputFile, header + '\n' + outputLines.join('\n') + '\n');

const elapsed = ((performance.now() - start) / 1000).toFixed(2);
console.log(`\nDone in ${elapsed}s`);
console.log(`Output: ${outputFile}${outputIsPartial ? ' (partial)' : ''}`);
console.log(`Lines: ${outputLines.length + 1} (was ${content.split('\n').length})`);
if (hasIncompleteMarkers) {
  const markerCount = outputLines.filter(l => l.trim() === '…').length;
  console.log(`Incomplete markers preserved: ${markerCount}`);
}
console.log('\nCompression stats by depth:');
for (let d = 0; d < uniqueByDepth.length; d++) {
  const linesAtDepth = outputLines.filter(l => l.match(/^\t*/)[0].length === d).length;
  console.log(`  Week ${d + 1}: ${uniqueByDepth[d].size} unique = ${linesAtDepth} lines (1.0x)`);
}
