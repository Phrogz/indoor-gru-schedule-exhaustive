#!/usr/bin/env node
// Display schedules from saved .js files

import { pathToFileURL } from 'url';
import { resolve } from 'path';

// Parse command-line args
const args = process.argv.slice(2);
let file = null, limit = 10, showMatchups = false, showGrid = true;

for (let i = 0; i < args.length; i++) {
	const arg = args[i];
	if (arg.startsWith('--limit=')) limit = parseInt(arg.slice(8));
	else if (arg === '--limit') limit = parseInt(args[++i]);
	else if (arg === '--matchups') showMatchups = true;
	else if (arg === '--no-grid') showGrid = false;
	else if (!arg.startsWith('--')) file = arg;
}

if (!file) {
	console.log('Usage: node display-schedules.mjs <file> [--limit N] [--matchups] [--no-grid]');
	console.log('  --limit N    Show at most N schedules (default: 10, 0 for all)');
	console.log('  --matchups   Show matchup indices instead of team letters');
	console.log('  --no-grid    Hide the slot grid, show only summary');
	process.exit(1);
}

// Try to extract teams from filename (e.g., "6teams-week0.js" or "8teams-2weeks.js")
let N_TEAMS = null;
const teamsMatch = file.match(/(\d+)teams/);
if (teamsMatch) N_TEAMS = parseInt(teamsMatch[1]);

// Load the file
const absPath = resolve(file);
let data;
try {
	const module = await import(pathToFileURL(absPath).href);
	data = module.default;
} catch (e) {
	console.error(`Failed to load ${file}:`, e.message);
	process.exit(1);
}

// Infer teams from data if not in filename
if (!N_TEAMS && data.length > 0) {
	// For week0 file: data is array of schedules, each schedule is array of matchups
	// For multi-week: data is array of [week0, children] nodes
	let sampleSchedule = data[0];
	if (Array.isArray(sampleSchedule) && sampleSchedule.length === 2 && Array.isArray(sampleSchedule[1])) {
		// Tree format: [schedule, children]
		sampleSchedule = sampleSchedule[0];
	}
	// Number of slots = (N_TEAMS * 3) / 2, so N_TEAMS = slots * 2 / 3
	const slots = sampleSchedule.length;
	N_TEAMS = (slots * 2) / 3;
}

if (!N_TEAMS || N_TEAMS % 1 !== 0) {
	console.error('Could not determine team count from file');
	process.exit(1);
}

const TEAMS = 'ABCDEFGHIJKLMNOP'.slice(0, N_TEAMS).split('');
const N_SLOTS = (N_TEAMS * 3) / 2;

// Precompute matchup decoding
const matchupToTeams = [];
for (let ti1 = 0; ti1 < N_TEAMS - 1; ti1++) {
	for (let ti2 = ti1 + 1; ti2 < N_TEAMS; ti2++) {
		matchupToTeams.push([ti1, ti2]);
	}
}

function matchupToString(m) {
	const [t1, t2] = matchupToTeams[m];
	return `${TEAMS[t1]}v${TEAMS[t2]}`;
}

function formatWeek(matchups, weekNum) {
	const lines = [];
	const header = `Week ${weekNum}:`;

	if (showMatchups) {
		lines.push(`${header} [${matchups.join(',')}]`);
	} else {
		lines.push(`${header} ${matchups.map(matchupToString).join(' ')}`);
	}

	if (showGrid) {
		// Build slot grid
		const grid = [];
		for (let ti = 0; ti < N_TEAMS; ti++) {
			grid.push(new Array(N_SLOTS).fill('.'));
		}

		for (let slot = 0; slot < matchups.length; slot++) {
			const [t1, t2] = matchupToTeams[matchups[slot]];
			grid[t1][slot] = TEAMS[t2];
			grid[t2][slot] = TEAMS[t1];
		}

		// Print grid
		for (let ti = 0; ti < N_TEAMS; ti++) {
			lines.push(`  ${TEAMS[ti]}: ${grid[ti].join(' ')}`);
		}
	}

	return lines.join('\n');
}

// Determine if this is week0-only or multi-week tree format
function isTreeNode(item) {
	return Array.isArray(item) && item.length === 2 && Array.isArray(item[0]) && Array.isArray(item[1]);
}

function isLeafNode(item) {
	return Array.isArray(item) && item.length === 1 && Array.isArray(item[0]);
}

// Flatten tree into array of complete schedules (paths from root to leaves)
function flattenTree(node, path = []) {
	if (isLeafNode(node)) {
		// Leaf: [schedule] with no children
		return [path.concat([node[0]])];
	} else if (isTreeNode(node)) {
		// Internal: [schedule, children]
		const [schedule, children] = node;
		const newPath = path.concat([schedule]);
		const results = [];
		for (const child of children) {
			results.push(...flattenTree(child, newPath));
		}
		return results;
	} else if (Array.isArray(node) && node.every(n => typeof n === 'number')) {
		// Simple schedule array (week0-only format)
		return [[node]];
	}
	return [];
}

// Parse schedules from data
let schedules = [];

if (data.length > 0) {
	if (isTreeNode(data[0]) || isLeafNode(data[0])) {
		// Multi-week tree format
		for (const rootNode of data) {
			schedules.push(...flattenTree(rootNode));
		}
	} else if (Array.isArray(data[0]) && data[0].every(n => typeof n === 'number')) {
		// Week0-only format: array of schedule arrays
		schedules = data.map(sched => [sched]);
	}
}

// Display header info
console.log(`File: ${file}`);
console.log(`Teams: ${N_TEAMS} (${TEAMS.join('')}), Slots: ${N_SLOTS}`);
console.log(`Schedules in file: ${schedules.length}`);
console.log();

// Display schedules
const toShow = limit === 0 ? schedules.length : Math.min(limit, schedules.length);

for (let i = 0; i < toShow; i++) {
	const weeks = schedules[i];
	console.log(`=== Schedule ${i + 1} of ${schedules.length} ===`);

	for (let w = 0; w < weeks.length; w++) {
		console.log(formatWeek(weeks[w], w));
		console.log();
	}
}

if (toShow < schedules.length) {
	console.log(`... and ${schedules.length - toShow} more. Use --limit=0 to show all.`);
}
