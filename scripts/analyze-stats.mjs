#!/usr/bin/env node
// Analyze game statistics from result files

import { createReadStream, readdirSync } from 'fs';
import { createInterface } from 'readline';

// const N_TEAMS = 6;
const N_TEAMS = 8;

/**
 * Find the maximum week count available in result files for N_TEAMS.
 * @returns {number} Maximum week count found, or 0 if no files exist
 */
function findMaxWeeks() {
	const pattern = new RegExp(`^${N_TEAMS}teams-(\\d+)weeks?\\.txt$`);
	let maxWeeks = 0;
	for (const file of readdirSync('results')) {
		const match = file.match(pattern);
		if (match) {
			maxWeeks = Math.max(maxWeeks, parseInt(match[1], 10));
		}
	}
	return maxWeeks;
}

/**
 * Count how many children each parent node has at a given depth in a tab-indented tree file.
 * Only includes parent nodes that have at least one child (ignoring incomplete branches).
 * @param {string} filepath - Path to the tree file
 * @param {number} parentDepth - Tab depth of parent nodes (0 = root)
 * @returns {Promise<number[]>} Array of child counts (one per parent node)
 */
async function countChildrenAtDepth(filepath, parentDepth) {
	const rl = createInterface({
		input: createReadStream(filepath),
		crlfDelay: Infinity
	});

	const childDepth = parentDepth + 1;
	const counts = [];
	let currentCount = 0;
	let inParent = false;
	let hasChild = false;

	for await (const line of rl) {
		if (line.startsWith('#') || !line.trim()) continue;

		// Count tabs at start
		let depth = 0;
		while (line[depth] === '\t') depth++;

		// Skip incomplete markers
		if (line.slice(depth).trim() === '…') continue;

		if (depth === parentDepth) {
			// Found a parent node - save previous if it had children
			if (inParent && hasChild) {
				counts.push(currentCount);
			}
			inParent = true;
			currentCount = 0;
			hasChild = false;
		} else if (depth === childDepth && inParent) {
			// Found a child node
			currentCount++;
			hasChild = true;
		} else if (depth < parentDepth && inParent) {
			// Moved above parent depth - save if had children
			if (hasChild) {
				counts.push(currentCount);
			}
			inParent = false;
			currentCount = 0;
			hasChild = false;
		}
	}

	// Don't forget the last parent if file ends while tracking one
	if (inParent && hasChild) {
		counts.push(currentCount);
	}

	return counts;
}

/**
 * Calculate statistics for an array of numbers
 * @param {number[]} numbers
 * @returns {{min: number, max: number, median: number, average: number}}
 */
function calculateStats(numbers) {
	const sorted = [...numbers].sort((a, b) => a - b);
	const min = sorted[0];
	const max = sorted[sorted.length - 1];
	const median = sorted.length % 2 === 0
		? (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2
		: sorted[Math.floor(sorted.length / 2)];
	const average = numbers.reduce((a, b) => a + b, 0) / numbers.length;

	return { min, max, median, average };
}

/**
 * Print analysis for a given file
 * @param {string} filepath - Path to the results file
 * @param {number} [weeksOverride] - Override weeks from filename parsing
 */
async function analyzeFile(filepath, weeksOverride = null) {
	// Parse weeks from filename if not overridden
	const match = filepath.match(/(\d+)teams-(\d+)weeks?/);
	if (!match && !weeksOverride) {
		console.error(`Cannot determine weeks from filename: ${filepath}`);
		return;
	}
	const fileWeeks = weeksOverride ?? parseInt(match[2], 10);
	const parentDepth = fileWeeks - 2;  // 0-indexed depth of parent nodes

	// User-facing week numbers (1-indexed)
	const parentWeek = fileWeeks - 1;
	const childWeek = fileWeeks;

	console.log('='.repeat(70));
	console.log(filepath);
	console.log('='.repeat(70));

	const counts = await countChildrenAtDepth(filepath, parentDepth);

	console.log(`Week ${parentWeek} options analyzed: ${counts.length.toLocaleString()}`);

	if (counts.length > 0) {
		const stats = calculateStats(counts);
		const total = counts.reduce((a, b) => a + b, 0);
		console.log(`Week ${childWeek} continuations per week ${parentWeek}: min=${stats.min.toLocaleString()}, median=${stats.median.toLocaleString()}, max=${stats.max.toLocaleString()}, average=${stats.average.toFixed(2)}`);
		console.log(`Total week ${childWeek} schedules: ${total.toLocaleString()}`);
	} else {
		console.log(`No week ${parentWeek} options found in file.`);
	}
}

// Main execution
const args = process.argv.slice(2);
const analysisType = args[0] || 'all';

// Check if argument is a file path
if (analysisType.includes('/') || analysisType.endsWith('.txt')) {
	await analyzeFile(analysisType);
} else {
	const maxWeeks = findMaxWeeks();
	
	if (maxWeeks < 2) {
		console.log(`No result files found for ${N_TEAMS} teams with 2+ weeks.`);
		process.exit(1);
	}
	
	if (analysisType === 'all') {
		for (let weeks = 2; weeks <= maxWeeks; weeks++) {
			if (weeks > 2) console.log('\n');
			await analyzeFile(`results/${N_TEAMS}teams-${weeks}weeks.txt`);
		}
	} else {
		const weeks = parseInt(analysisType.replace('weeks', ''), 10);
		if (weeks >= 2 && weeks <= maxWeeks) {
			await analyzeFile(`results/${N_TEAMS}teams-${weeks}weeks.txt`);
		} else {
			console.log(`Usage: node analyze_stats.mjs [all|2weeks|3weeks|...|${maxWeeks}weeks|<filepath>]`);
		}
	}
}
