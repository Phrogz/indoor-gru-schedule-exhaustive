#!/usr/bin/env node
// Analyze leaf counts per parent in result files

import { readdirSync } from 'fs';
import { readHeaderFromFile } from '../lib/tree-format.mjs';
import { collectParentStats } from '../lib/tree-parent-stats.mjs';

// const N_TEAMS = 6;
const N_TEAMS = 8;

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

async function analyzeFile(filepath, weeksOverride = null, includePartials = false) {
	let fileWeeks = weeksOverride;
	if (!fileWeeks) {
		const header = await readHeaderFromFile(filepath);
		fileWeeks = header.weeks || null;
	}

	if (!fileWeeks) {
		const match = filepath.match(/(\d+)teams-(\d+)weeks?/);
		if (!match) {
			console.error(`Cannot determine weeks from filename or header: ${filepath}`);
			return;
		}
		fileWeeks = parseInt(match[2], 10);
	}

	const parentDepth = fileWeeks - 2;  // 0-indexed depth of parent nodes

	// User-facing week numbers (1-indexed)
	const parentWeek = fileWeeks - 1;
	const childWeek = fileWeeks;

	console.log('='.repeat(70));
	console.log(filepath);
	console.log('='.repeat(70));

	const statsMap = await collectParentStats(filepath, parentDepth, { includePartials: true });
	const countsCompleted = [];
	const countsIncomplete = [];
	const countsUnknown = [];
	for (const { childCount, partial, complete } of statsMap.values()) {
		if (complete) countsCompleted.push(childCount);
		else if (partial) countsIncomplete.push(childCount);
		else countsUnknown.push(childCount);
	}

	const counts = includePartials
		? countsCompleted.concat(countsUnknown, countsIncomplete)
		: countsCompleted.concat(countsUnknown);

	console.log(`Week ${parentWeek} options analyzed: ${counts.length.toLocaleString()}`);
	if (!includePartials && countsIncomplete.length > 0) {
		console.log(`Incomplete parents excluded: ${countsIncomplete.length.toLocaleString()}`);
	}

	if (counts.length > 0) {
		const stats = calculateStats(counts);
		const total = counts.reduce((a, b) => a + b, 0);
		console.log(`Week ${childWeek} continuations per week ${parentWeek}: min=${stats.min.toLocaleString()}, median=${stats.median.toLocaleString()}, max=${stats.max.toLocaleString()}, average=${stats.average.toFixed(2)}`);
		console.log(`Total week ${childWeek} schedules: ${total.toLocaleString()}`);
	} else {
		console.log(`No week ${parentWeek} options found in file.`);
	}

	printStatsLine('Completed-only stats', countsCompleted);
	printStatsLine('Unknown-only stats', countsUnknown);
	printStatsLine('Incomplete-only stats', countsIncomplete);
}

function printStatsLine(label, counts) {
	if (counts.length === 0) {
		console.log(`${label}: min=0, median=0, max=0, average=0.00, total=0`);
		return;
	}
	const stats = calculateStats(counts);
	const total = counts.reduce((a, b) => a + b, 0);
	console.log(`${label}: min=${stats.min.toLocaleString()}, median=${stats.median.toLocaleString()}, max=${stats.max.toLocaleString()}, average=${stats.average.toFixed(2)}, total=${total.toLocaleString()}`);
}

const args = process.argv.slice(2);
const includePartials = args.includes('--include-partials');
const filteredArgs = args.filter(a => a !== '--include-partials');
const analysisType = filteredArgs[0] || 'all';

if (analysisType.includes('/') || analysisType.endsWith('.txt')) {
	await analyzeFile(analysisType, null, includePartials);
} else {
	const maxWeeks = findMaxWeeks();

	if (maxWeeks < 2) {
		console.log(`No result files found for ${N_TEAMS} teams with 2+ weeks.`);
		process.exit(1);
	}

	if (analysisType === 'all') {
		for (let weeks = 2; weeks <= maxWeeks; weeks++) {
			if (weeks > 2) console.log('\n');
			await analyzeFile(`results/${N_TEAMS}teams-${weeks}weeks.txt`, weeks, includePartials);
		}
	} else {
		const weeks = parseInt(analysisType.replace('weeks', ''), 10);
		if (weeks >= 2 && weeks <= maxWeeks) {
			await analyzeFile(`results/${N_TEAMS}teams-${weeks}weeks.txt`, weeks, includePartials);
		} else {
			console.log(`Usage: node scripts/leaves-per-path.mjs [--include-partials] [all|2weeks|3weeks|...|${maxWeeks}weeks|<filepath>]`);
		}
	}
}
