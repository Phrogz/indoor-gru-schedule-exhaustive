#!/usr/bin/env node
// Analyze memory requirements for different path iteration strategies
// Usage: node scripts/analyze-memory.mjs results/6teams-4weeks.txt

import { createReadStream } from 'fs';
import { createInterface } from 'readline';
import { stat } from 'fs/promises';

const filePath = process.argv[2] || 'results/6teams-4weeks.txt';

console.log(`Analyzing: ${filePath}\n`);

// Get file size
const fileStats = await stat(filePath);
console.log(`File size: ${(fileStats.size / 1024 / 1024).toFixed(2)} MB\n`);

// Stream through file and collect statistics
const rl = createInterface({
  input: createReadStream(filePath),
  crlfDelay: Infinity
});

let header = null;
let totalPaths = 0;
let totalLines = 0;
let totalBytes = 0;

// Track tree structure - in prefix-compressed format, each line = one node
// nodeCountByDepth[depth] = number of nodes (lines) at that depth
const nodeCountByDepth = [];

// For calculating average schedule size
let totalScheduleBytes = 0;

console.log('Streaming through file...');
const startTime = Date.now();
let lastProgressTime = startTime;

for await (const line of rl) {
  totalLines++;
  const lineBytes = Buffer.byteLength(line, 'utf8') + 1; // +1 for newline
  totalBytes += lineBytes;
  
  // Progress every 5 seconds
  const now = Date.now();
  if (now - lastProgressTime > 5000) {
    const elapsed = (now - startTime) / 1000;
    const mbProcessed = totalBytes / 1024 / 1024;
    process.stdout.write(`\r  ${totalLines.toLocaleString()} lines, ${mbProcessed.toFixed(1)} MB, ${totalPaths.toLocaleString()} paths (${elapsed.toFixed(0)}s)   `);
    lastProgressTime = now;
  }
  
  // Parse header
  if (line.startsWith('#')) {
    header = parseHeader(line);
    continue;
  }
  
  if (line.trim() === '' || line.trim() === '…') {
    continue;
  }
  
  // Count leading tabs for depth
  let depth = 0;
  while (line[depth] === '\t') depth++;
  
  const content = line.slice(depth);
  const scheduleBytes = Buffer.byteLength(content, 'utf8');
  totalScheduleBytes += scheduleBytes;
  
  // Initialize depth tracking if needed
  while (nodeCountByDepth.length <= depth) {
    nodeCountByDepth.push(0);
  }
  
  // Each line in prefix-compressed format is a unique node
  nodeCountByDepth[depth]++;
  
  // Check if this is a leaf (complete path)
  if (depth === header.weeks - 1) {
    totalPaths++;
  }
}

const elapsed = (Date.now() - startTime) / 1000;
console.log(`\r  Done in ${elapsed.toFixed(1)}s                                        \n`);

// Calculate statistics
const totalNodes = nodeCountByDepth.reduce((a, b) => a + b, 0);

console.log('='.repeat(60));
console.log('FILE STATISTICS');
console.log('='.repeat(60));
console.log(`Header: teams=${header.teams}, weeks=${header.weeks}, count=${header.count}`);
console.log(`Actual paths found: ${totalPaths.toLocaleString()}`);
console.log(`Total lines: ${totalLines.toLocaleString()}`);
console.log(`Total nodes: ${totalNodes.toLocaleString()}`);
console.log(`Total bytes: ${(totalBytes / 1024 / 1024).toFixed(2)} MB`);
console.log(`Average schedule size: ${(totalScheduleBytes / totalNodes).toFixed(1)} bytes`);

console.log('\n' + '='.repeat(60));
console.log('TREE STRUCTURE');
console.log('='.repeat(60));

let totalInternalNodes = 0;
let totalLeafNodes = 0;

for (let d = 0; d < nodeCountByDepth.length; d++) {
  const nodeCount = nodeCountByDepth[d];
  const isLeaf = d === header.weeks - 1;
  
  // Calculate average children from next level's count
  const childCount = d < nodeCountByDepth.length - 1 ? nodeCountByDepth[d + 1] : 0;
  const avgChildren = nodeCount > 0 ? (childCount / nodeCount).toFixed(1) : 0;
  
  if (isLeaf) {
    totalLeafNodes += nodeCount;
    console.log(`Depth ${d} (Week ${d + 1}, LEAF): ${nodeCount.toLocaleString()} nodes`);
  } else {
    totalInternalNodes += nodeCount;
    console.log(`Depth ${d} (Week ${d + 1}): ${nodeCount.toLocaleString()} nodes, avg children: ${avgChildren}`);
  }
}

console.log(`\nTotal internal nodes: ${totalInternalNodes.toLocaleString()}`);
console.log(`Total leaf nodes: ${totalLeafNodes.toLocaleString()} (= paths)`);

// Calculate memory requirements for each option
console.log('\n' + '='.repeat(60));
console.log('MEMORY REQUIREMENTS');
console.log('='.repeat(60));

const bytesPerInt32 = 4;
const bytesPerInt64 = 8;
const avgScheduleBytes = totalScheduleBytes / totalNodes;
const slotsPerWeek = (header.teams * 3) / 2;

// Option A: Path indices + byte offsets
console.log('\n--- OPTION A: Index array + byte offsets ---');
const optA_indices = totalPaths * bytesPerInt32;
const optA_offsets = totalPaths * bytesPerInt64;
const optA_total = optA_indices + optA_offsets;
console.log(`  Path indices (${totalPaths.toLocaleString()} × 4 bytes): ${(optA_indices / 1024 / 1024).toFixed(2)} MB`);
console.log(`  Byte offsets (${totalPaths.toLocaleString()} × 8 bytes): ${(optA_offsets / 1024 / 1024).toFixed(2)} MB`);
console.log(`  TOTAL: ${(optA_total / 1024 / 1024).toFixed(2)} MB`);
console.log(`  Access: O(1) random access via seek`);

// Option B: Tree structure only (no leaf storage)
console.log('\n--- OPTION B: Tree structure only (no leaf indices) ---');
// For each internal node: byte offset (8) + child count (4) + first child index (4)
const optB_internalNodeBytes = 16;
const optB_internal = totalInternalNodes * optB_internalNodeBytes;
// Plus: array mapping node index to children range
const optB_childIndex = totalInternalNodes * bytesPerInt32 * 2; // start, end indices
const optB_total = optB_internal + optB_childIndex;
console.log(`  Internal nodes (${totalInternalNodes.toLocaleString()} × ${optB_internalNodeBytes} bytes): ${(optB_internal / 1024 / 1024).toFixed(2)} MB`);
console.log(`  Child index arrays: ${(optB_childIndex / 1024 / 1024).toFixed(2)} MB`);
console.log(`  TOTAL: ${(optB_total / 1024 / 1024).toFixed(2)} MB`);
console.log(`  Access: Traverse tree in round-robin order, need byte offsets to seek`);

// Option B2: Tree structure with byte offsets at leaf parents
console.log('\n--- OPTION B2: Tree + leaf parent offsets ---');
const leafParentCount = nodeCountByDepth.length >= 2 ? nodeCountByDepth[nodeCountByDepth.length - 2] : 0;
const optB2_leafOffsets = leafParentCount * bytesPerInt64;
const optB2_total = optB_internal + optB_childIndex + optB2_leafOffsets;
console.log(`  Internal nodes: ${(optB_internal / 1024 / 1024).toFixed(2)} MB`);
console.log(`  Child index arrays: ${(optB_childIndex / 1024 / 1024).toFixed(2)} MB`);
console.log(`  Leaf parent offsets (${leafParentCount.toLocaleString()} × 8): ${(optB2_leafOffsets / 1024 / 1024).toFixed(2)} MB`);
console.log(`  TOTAL: ${(optB2_total / 1024 / 1024).toFixed(2)} MB`);
console.log(`  Access: Seek to leaf parent, scan children linearly`);

// Option C: Full paths in memory
console.log('\n--- OPTION C: Full paths in memory ---');
const pathBytes = header.weeks * slotsPerWeek * 1; // 1 byte per matchup index
const optC_paths = totalPaths * pathBytes;
const optC_overhead = totalPaths * 8 * header.weeks; // Array object overhead estimate
const optC_reorderArray = totalPaths * bytesPerInt32;
const optC_total = optC_paths + optC_overhead + optC_reorderArray;
console.log(`  Path data (${totalPaths.toLocaleString()} × ${header.weeks} weeks × ${slotsPerWeek} slots): ${(optC_paths / 1024 / 1024).toFixed(2)} MB`);
console.log(`  JS array overhead (estimate): ${(optC_overhead / 1024 / 1024).toFixed(2)} MB`);
console.log(`  Reorder array: ${(optC_reorderArray / 1024 / 1024).toFixed(2)} MB`);
console.log(`  TOTAL: ${(optC_total / 1024 / 1024).toFixed(2)} MB`);
console.log(`  Access: O(1) direct array access, fastest`);

// Option D: Minimal - just reorder indices, re-stream for access
console.log('\n--- OPTION D: Reorder indices only (re-stream for access) ---');
const optD_total = totalPaths * bytesPerInt32;
console.log(`  Reorder array (${totalPaths.toLocaleString()} × 4 bytes): ${(optD_total / 1024 / 1024).toFixed(2)} MB`);
console.log(`  Access: Must re-stream file for each batch of paths`);
console.log(`  Trade-off: Minimal memory, but O(N) access per batch`);

// Summary
console.log('\n' + '='.repeat(60));
console.log('SUMMARY');
console.log('='.repeat(60));
console.log(`Option A (indices + offsets):     ${(optA_total / 1024 / 1024).toFixed(2)} MB - O(1) random access`);
console.log(`Option B (tree structure):        ${(optB_total / 1024 / 1024).toFixed(2)} MB - Tree traversal + seek`);
console.log(`Option B2 (tree + leaf offsets):  ${(optB2_total / 1024 / 1024).toFixed(2)} MB - Hybrid approach`);
console.log(`Option C (full paths):            ${(optC_total / 1024 / 1024).toFixed(2)} MB - Fastest, most memory`);
console.log(`Option D (indices only):          ${(optD_total / 1024 / 1024).toFixed(2)} MB - Minimal, re-stream`);

// Recommendation
console.log('\n' + '='.repeat(60));
console.log('RECOMMENDATION FOR BREADTH-FIRST ITERATION');
console.log('='.repeat(60));
console.log(`
For true breadth-first (W1 fastest, then W2, then W3...) iteration:

1. BEST FOR SMALL FILES (< 100MB paths): Option C
   - Load all paths, sort by depth-reversed order
   - Fastest iteration, most memory

2. BEST FOR MEDIUM FILES (100MB - 1GB): Option B2
   - Store tree structure with byte offsets
   - Round-robin traverse tree, seek to read paths
   - Good balance of memory and speed

3. BEST FOR LARGE FILES (> 1GB): Option D with batching
   - Store only reorder indices
   - Process in batches: load batch, reorder, process, repeat
   - Slowest but scales to any size

Given your file has ${totalPaths.toLocaleString()} paths:
  - If you have ${(optC_total / 1024 / 1024).toFixed(0)} MB to spare: Use Option C
  - If you have ${(optB2_total / 1024 / 1024).toFixed(0)} MB to spare: Use Option B2  
  - Minimum viable: Option D with ${(optD_total / 1024 / 1024).toFixed(0)} MB
`);

function parseHeader(line) {
  const result = { teams: 0, weeks: 0, count: 0, partial: false };
  const teamsMatch = line.match(/teams=(\d+)/);
  if (teamsMatch) result.teams = parseInt(teamsMatch[1], 10);
  const weeksMatch = line.match(/weeks=(\d+)/);
  if (weeksMatch) result.weeks = parseInt(weeksMatch[1], 10);
  const countMatch = line.match(/count=(\d+)/);
  if (countMatch) result.count = parseInt(countMatch[1], 10);
  result.partial = line.includes('(partial)');
  return result;
}
