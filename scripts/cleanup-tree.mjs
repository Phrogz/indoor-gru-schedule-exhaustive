#!/usr/bin/env node
// Cleanup tool: Reads a tree file and rewrites with optimal prefix compression
// Uses streaming I/O and external sort to handle arbitrarily large files
// Usage: node scripts/cleanup-tree.mjs results/8teams-4weeks.txt

import { cleanupTreeFile } from '../lib/tree-cleanup.mjs';

const inputFile = process.argv[2];
if (!inputFile) {
  console.error('Usage: node scripts/cleanup-tree.mjs <file>');
  console.error('Example: node scripts/cleanup-tree.mjs results/8teams-4weeks.txt');
  process.exit(1);
}

console.log(`Reading ${inputFile}...`);

const {
	outputFile,
	teams,
	weeks,
	pathCount,
	uniqueCount,
	duplicatesRemoved,
	incompleteParentCount,
	partialMarker,
	outputLineCount,
	linesPerDepth,
	incompleteMarkerCount,
	completeMarkerCount,
	elapsed
} = await cleanupTreeFile(inputFile);

console.log(`  teams=${teams}, weeks=${weeks}`);
console.log(`  Extracted ${pathCount.toLocaleString()} paths`);
if (duplicatesRemoved > 0) {
	console.log(`  Removed ${duplicatesRemoved.toLocaleString()} duplicates`);
}
console.log(`  ${uniqueCount.toLocaleString()} unique paths`);
console.log(`  ${incompleteParentCount.toLocaleString()} parent paths remain incomplete (last occurrence had marker)`);

console.log(`\nDone in ${elapsed}s`);
console.log(`Output: ${outputFile}${partialMarker}`);
console.log(`Lines: ${outputLineCount.toLocaleString()}`);
if (incompleteMarkerCount > 0) {
	console.log(`Incomplete markers preserved: ${incompleteMarkerCount.toLocaleString()}`);
}
if (completeMarkerCount > 0) {
	console.log(`Complete markers preserved: ${completeMarkerCount.toLocaleString()}`);
}
console.log('\nCompression stats by depth:');
for (let d = 0; d < linesPerDepth.length; d++) {
	console.log(`  Week ${d + 1}: ${linesPerDepth[d].toLocaleString()} lines`);
}
