#!/usr/bin/env node
// Display schedules from saved .txt files

import { resolve } from 'path';
import { readFileSync } from 'fs';

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

// Parse .txt format: header + tab-indented tree
const content = readFileSync(resolve(file), 'utf-8');
const lines = content.split('\n');

// Parse header: # teams=N weeks=W count=Z
const header = lines[0];
const headerMatch = header.match(/^#\s*teams=(\d+)\s+weeks=(\d+)/);
if (!headerMatch) {
	console.error('Invalid file format: missing header');
	process.exit(1);
}
const N_TEAMS = parseInt(headerMatch[1]);

// Parse tree structure based on tab indentation
let data = [];
const stack = [{ children: data, depth: -1 }];

for (let i = 1; i < lines.length; i++) {
	const line = lines[i];
	if (!line.trim()) continue;

	// Count leading tabs
	let depth = 0;
	while (depth < line.length && line[depth] === '\t') depth++;

	// Skip incomplete markers
	const content = line.trim();
	if (content === '…') continue;

	const schedule = content.split(',').map(Number);

	// Pop stack until we find parent
	while (stack.length > 1 && stack[stack.length - 1].depth >= depth) {
		stack.pop();
	}

	const parent = stack[stack.length - 1];
	const node = [schedule, []];
	parent.children.push(node);
	stack.push({ children: node[1], depth });
}

// Convert nodes with empty children arrays to leaf nodes [schedule]
function cleanTree(nodes) {
	return nodes.map(node => {
		if (node[1].length === 0) {
			return [node[0]]; // Leaf node
		}
		return [node[0], cleanTree(node[1])]; // Internal node
	});
}
data = cleanTree(data);

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
// Uses iterative approach to avoid stack overflow on large trees
function flattenTree(roots) {
	const results = [];
	// Stack of {node, pathSoFar}
	const stack = [];

	// Initialize with root nodes
	for (let i = roots.length - 1; i >= 0; i--) {
		stack.push({ node: roots[i], path: [] });
	}

	while (stack.length > 0) {
		const { node, path } = stack.pop();

		if (isLeafNode(node)) {
			// Leaf: [schedule] with no children
			results.push(path.concat([node[0]]));
		} else {
			// Internal: [schedule, children]
			const [schedule, children] = node;
			const newPath = path.concat([schedule]);
			// Push children in reverse order so we process them in order
			for (let i = children.length - 1; i >= 0; i--) {
				stack.push({ node: children[i], path: newPath });
			}
		}
	}

	return results;
}

// Parse schedules from data
const schedules = flattenTree(data);

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
