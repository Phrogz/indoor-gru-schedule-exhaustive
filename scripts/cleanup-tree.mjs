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
const headerMatch = headerLine.match(/teams=(\d+).*weeks=(\d+).*score=([\d,]+).*count=(\d+)/);
if (!headerMatch) {
  console.error('Error: Could not parse header');
  process.exit(1);
}

const [, teams, weeks, score, count] = headerMatch;
console.log(`  teams=${teams}, weeks=${weeks}, score=${score}, count=${count}`);

// Extract all complete paths
console.log('Extracting paths...');
const paths = [];
let currentPath = [];

for (let i = 1; i < lines.length; i++) {
  const line = lines[i];
  if (!line.trim() || line.trim() === '…') continue;
  
  const depth = line.match(/^\t*/)[0].length;
  const schedule = line.trim().split(',').map(Number);
  
  currentPath = currentPath.slice(0, depth);
  currentPath.push(schedule);
  
  if (currentPath.length === parseInt(weeks)) {
    paths.push(currentPath.map(s => [...s])); // Deep copy
  }
}

console.log(`  Extracted ${paths.length} complete paths`);

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

// Write with optimal prefix compression
console.log('Writing compressed output...');
const outputLines = [];
let prevPath = [];

for (const path of paths) {
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
}

// Count unique prefixes at each depth for stats
const uniqueByDepth = [];
for (let d = 0; d < parseInt(weeks); d++) {
  uniqueByDepth.push(new Set());
}
currentPath = [];
for (const line of outputLines) {
  const depth = line.match(/^\t*/)[0].length;
  currentPath = currentPath.slice(0, depth);
  currentPath.push(line.trim());
  uniqueByDepth[depth].add(currentPath.join('|'));
}

// Write output
const header = `# teams=${teams} weeks=${weeks} count=${paths.length}`;
const outputFile = inputFile.replace('.txt', '-clean.txt');
writeFileSync(outputFile, header + '\n' + outputLines.join('\n') + '\n');

const elapsed = ((performance.now() - start) / 1000).toFixed(2);
console.log(`\nDone in ${elapsed}s`);
console.log(`Output: ${outputFile}`);
console.log(`Lines: ${outputLines.length + 1} (was ${content.split('\n').length})`);
console.log('\nCompression stats by depth:');
for (let d = 0; d < uniqueByDepth.length; d++) {
  const linesAtDepth = outputLines.filter(l => l.match(/^\t*/)[0].length === d).length;
  console.log(`  Week ${d + 1}: ${uniqueByDepth[d].size} unique = ${linesAtDepth} lines (1.0x)`);
}
