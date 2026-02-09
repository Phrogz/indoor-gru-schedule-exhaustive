// Cleanup helpers for streaming tree result files

import { createReadStream, createWriteStream, unlinkSync } from 'fs';
import { createInterface } from 'readline';
import { execSync } from 'child_process';
import { tmpdir } from 'os';
import { join } from 'path';
import { COMPLETE_MARKER, INCOMPLETE_MARKER, isCompleteMarker, isIncompleteMarker } from './tree-format.mjs';

function validateSchedule(content, expectedLength) {
	const parts = content.split(',');
	if (parts.length !== expectedLength) return false;
	for (const part of parts) {
		if (part === '' || !Number.isFinite(Number(part))) return false;
	}
	return true;
}

export async function extractTreePaths(inputFile) {
	const tempFile = join(tmpdir(), `cleanup-${process.pid}-${Date.now()}.txt`);
	const sortedFile = tempFile + '.sorted';
	const parentFinalState = new Map();  // key -> {hasChildren, lastMarker, markedComplete}

	let teams;
	let weeks;
	let targetWeeks;
	let numTeams;
	let slotsPerWeek;
	let parentDepth;
	let leafDepth;
	let pathCount = 0;

	const rl = createInterface({ input: createReadStream(inputFile), crlfDelay: Infinity });
	const tempStream = createWriteStream(tempFile);

	let lineNum = 0;
	let currentPath = [];   // stores schedule strings (not parsed arrays)
	let currentParentKey = null;
	let currentParentHasChildren = false;
	let currentParentLastMarker = 'none';
	let currentParentCompleteSeen = false;
	let warningCount = 0;
	const MAX_WARNINGS = 10;

	function finalizeParent() {
		if (currentParentKey !== null) {
			const prev = parentFinalState.get(currentParentKey) || { hasChildren: false, lastMarker: 'none', markedComplete: false };
			parentFinalState.set(currentParentKey, {
				hasChildren: currentParentHasChildren || prev.hasChildren,
				lastMarker: currentParentLastMarker,
				markedComplete: prev.markedComplete || currentParentCompleteSeen
			});
		}
	}

	for await (const line of rl) {
		lineNum++;

		// ── Header ──
		if (lineNum === 1) {
			if (!line.startsWith('#')) {
				throw new Error('File does not have a valid header');
			}
			const m = line.match(/teams=(\d+).*weeks=(\d+).*count=(\d+)/);
			if (!m) {
				throw new Error('Could not parse header');
			}
			teams = m[1];
			weeks = m[2];
			targetWeeks = parseInt(weeks, 10);
			numTeams = parseInt(teams, 10);
			slotsPerWeek = 3 * (numTeams / 2);
			parentDepth = targetWeeks - 2;
			leafDepth = targetWeeks - 1;
			continue;
		}

		if (!line.trim()) continue;

		const depth = line.match(/^\t*/)[0].length;
		const content = line.trim();

		// Handle markers
		if (isIncompleteMarker(content) || isCompleteMarker(content)) {
			if (depth === leafDepth && currentParentKey !== null) {
				if (isIncompleteMarker(content)) currentParentLastMarker = 'incomplete';
				if (isCompleteMarker(content)) {
					currentParentLastMarker = 'complete';
					currentParentCompleteSeen = true;
				}
			}
			continue;
		}

		// Validate schedule
		if (!validateSchedule(content, slotsPerWeek)) {
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
				currentParentLastMarker = 'none';
				currentParentCompleteSeen = false;
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
	}

	finalizeParent();
	await new Promise((resolve, reject) => { tempStream.end(resolve); tempStream.on('error', reject); });

	const incompleteParentKeys = new Set();
	const completeParentKeys = new Set();
	for (const [key, state] of parentFinalState) {
		if (state.markedComplete) {
			completeParentKeys.add(key);
		} else if (state.lastMarker === 'incomplete') {
			incompleteParentKeys.add(key);
		}
	}

	return {
		tempFile,
		sortedFile,
		teams,
		weeks,
		targetWeeks,
		parentDepth,
		leafDepth,
		pathCount,
		incompleteParentKeys,
		completeParentKeys,
		warningCount,
		maxWarnings: MAX_WARNINGS
	};
}

export function sortAndDeduplicate(tempFile, sortedFile) {
	execSync(`LC_ALL=C sort -u -o "${sortedFile}" "${tempFile}"`, { maxBuffer: 10 * 1024 * 1024 });
	const uniqueCount = parseInt(execSync(`wc -l < "${sortedFile}"`).toString().trim(), 10);
	return uniqueCount;
}

export async function writeCompressedTree({
	sortedFile,
	outputFile,
	teams,
	weeks,
	targetWeeks,
	leafDepth,
	uniqueCount,
	incompleteParentKeys,
	completeParentKeys
}) {
	const rl = createInterface({ input: createReadStream(sortedFile), crlfDelay: Infinity });
	const outStream = createWriteStream(outputFile);

	const hasIncomplete = incompleteParentKeys.size > 0;
	const partialMarker = hasIncomplete ? ' (partial)' : '';
	outStream.write(`# teams=${teams} weeks=${weeks} count=${uniqueCount}${partialMarker}\n`);

	let prevParts = [];
	let lastParentKey = null;
	let needsIncompleteMarker = false;
	let needsCompleteMarker = false;
	let outputLineCount = 1;  // header
	const linesPerDepth = new Array(targetWeeks).fill(0);
	let incompleteMarkerCount = 0;
	let completeMarkerCount = 0;

	for await (const line of rl) {
		const parts = line.split('|');

		// Check parent change for incomplete markers
		const parentKey = parts.slice(0, targetWeeks - 1).join('|');

		if (lastParentKey !== null && lastParentKey !== parentKey) {
			if (needsCompleteMarker) {
				outStream.write('\t'.repeat(leafDepth) + COMPLETE_MARKER + '\n');
				outputLineCount++;
				completeMarkerCount++;
			} else if (needsIncompleteMarker) {
				outStream.write('\t'.repeat(leafDepth) + INCOMPLETE_MARKER + '\n');
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
		needsCompleteMarker = completeParentKeys.has(parentKey);
		needsIncompleteMarker = !needsCompleteMarker && incompleteParentKeys.has(parentKey);
	}

	// Last parent's marker
	if (needsCompleteMarker) {
		outStream.write('\t'.repeat(leafDepth) + COMPLETE_MARKER + '\n');
		outputLineCount++;
		completeMarkerCount++;
	} else if (needsIncompleteMarker) {
		outStream.write('\t'.repeat(leafDepth) + INCOMPLETE_MARKER + '\n');
		outputLineCount++;
		incompleteMarkerCount++;
	}

	await new Promise((resolve, reject) => { outStream.end(resolve); outStream.on('error', reject); });

	return {
		partialMarker,
		outputLineCount,
		linesPerDepth,
		incompleteMarkerCount,
		completeMarkerCount
	};
}

export async function cleanupTreeFile(inputFile) {
	const start = performance.now();
	const {
		tempFile,
		sortedFile,
		teams,
		weeks,
		targetWeeks,
		leafDepth,
		pathCount,
		incompleteParentKeys,
		completeParentKeys,
		warningCount,
		maxWarnings
	} = await extractTreePaths(inputFile);

	if (warningCount > maxWarnings) {
		console.log(`  ...${(warningCount - maxWarnings).toLocaleString()} more invalid schedule warnings suppressed`);
	}

	if (pathCount === 0) {
		const outputFile = inputFile.replace('.txt', '-clean.txt');
		const { writeFileSync } = await import('fs');
		writeFileSync(outputFile, `# teams=${teams} weeks=${weeks} count=0\n`);
		return {
			outputFile,
			teams,
			weeks,
			uniqueCount: 0,
			partialMarker: '',
			linesPerDepth: new Array(targetWeeks).fill(0),
			incompleteMarkerCount: 0,
			elapsed: ((performance.now() - start) / 1000).toFixed(2)
		};
	}

	const uniqueCount = sortAndDeduplicate(tempFile, sortedFile);
	try { unlinkSync(tempFile); } catch {}
	const duplicatesRemoved = pathCount - uniqueCount;

	const outputFile = inputFile.replace('.txt', '-clean.txt');
	const {
		partialMarker,
		outputLineCount,
		linesPerDepth,
		incompleteMarkerCount,
		completeMarkerCount
	} = await writeCompressedTree({
		sortedFile,
		outputFile,
		teams,
		weeks,
		targetWeeks,
		leafDepth,
		uniqueCount,
		incompleteParentKeys,
		completeParentKeys
	});

	try { unlinkSync(sortedFile); } catch {}

	return {
		outputFile,
		teams,
		weeks,
		pathCount,
		uniqueCount,
		duplicatesRemoved,
		incompleteParentCount: incompleteParentKeys.size,
		partialMarker,
		outputLineCount,
		linesPerDepth,
		incompleteMarkerCount,
		completeMarkerCount,
		elapsed: ((performance.now() - start) / 1000).toFixed(2)
	};
}
