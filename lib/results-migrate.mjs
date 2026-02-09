// Helpers for migrating legacy .js result files to streaming tree format

import { readFile } from 'fs/promises';
import { pathToFileURL } from 'url';
import { resolve } from 'path';
import { TreeWriter } from './tree-format.mjs';

export function isTreeNode(item) {
	return Array.isArray(item) && item.length === 2 && Array.isArray(item[0]) && Array.isArray(item[1]);
}

export function isLeafNode(item) {
	return Array.isArray(item) && item.length === 1 && Array.isArray(item[0]);
}

// Flatten tree into array of complete paths
export function flattenTree(node, path = []) {
	if (isLeafNode(node)) {
		return [path.concat([Array.from(node[0])])];
	}
	if (isTreeNode(node)) {
		const [schedule, children] = node;
		const newPath = path.concat([Array.from(schedule)]);
		const results = [];
		for (const child of children) {
			results.push(...flattenTree(child, newPath));
		}
		return results;
	}
	if (Array.isArray(node) && node.every(n => typeof n === 'number')) {
		return [[Array.from(node)]];
	}
	return [];
}

// Parse header comments from .js files
export function parseJsHeader(content) {
	const result = { teams: 0, weeks: 0, count: 0 };

	// Look for patterns like: "8 teams, 2 weeks, count=7136"
	const teamsMatch = content.match(/(\d+)\s*teams/i);
	if (teamsMatch) result.teams = parseInt(teamsMatch[1], 10);

	const weeksMatch = content.match(/(\d+)\s*weeks?/i);
	if (weeksMatch) result.weeks = parseInt(weeksMatch[1], 10);

	// week0 only = 1 week
	if (content.includes('week0 only')) result.weeks = 1;

	const countMatch = content.match(/count=(\d+)/);
	if (countMatch) result.count = parseInt(countMatch[1], 10);

	return result;
}

export async function migrateResultsFile(jsPath, { dryRun = false } = {}) {
	console.log(`\nProcessing: ${jsPath}`);

	// Load the JS module
	const absPath = resolve(jsPath);
	let data;
	try {
		const module = await import(pathToFileURL(absPath).href);
		data = module.default;
	} catch (e) {
		console.error(`  Failed to load: ${e.message}`);
		return false;
	}

	// Read raw file for header info
	const rawContent = await readFile(jsPath, 'utf8');
	const headerInfo = parseJsHeader(rawContent);

	console.log(`  Header info: ${headerInfo.teams} teams, ${headerInfo.weeks} weeks, count=${headerInfo.count}`);

	// Flatten all paths
	let paths = [];
	if (data.length > 0) {
		if (isTreeNode(data[0]) || isLeafNode(data[0])) {
			// Multi-week tree format
			for (const rootNode of data) {
				paths.push(...flattenTree(rootNode));
			}
		} else if (Array.isArray(data[0]) && data[0].every(n => typeof n === 'number')) {
			// Week0-only format: array of schedule arrays
			paths = data.map(sched => [Array.from(sched)]);
		}
	}

	console.log(`  Found ${paths.length} complete paths`);

	if (paths.length === 0) {
		console.log('  Skipping: no paths found');
		return false;
	}

	// Determine weeks from data if header didn't have it
	const weeks = headerInfo.weeks || paths[0].length;

	// Determine output filename
	const txtPath = jsPath.replace(/\.js$/, '.txt');

	if (dryRun) {
		console.log(`  Would write to: ${txtPath}`);
		console.log(`  Sample path: [${paths[0].map(s => `[${s.slice(0, 3).join(',')}...]`).join(', ')}]`);
		return true;
	}

	// Sort paths for consistent tree output (depth-first by schedule values)
	paths.sort((a, b) => {
		for (let w = 0; w < Math.min(a.length, b.length); w++) {
			for (let s = 0; s < Math.min(a[w].length, b[w].length); s++) {
				if (a[w][s] !== b[w][s]) return a[w][s] - b[w][s];
			}
		}
		return a.length - b.length;
	});

	// Write to new format
	const writer = new TreeWriter(txtPath);
	writer.writeHeader(headerInfo.teams, weeks, paths.length);

	for (const path of paths) {
		writer.writePath(path);
	}

	await writer.finalize({
		teams: headerInfo.teams,
		weeks: weeks,
		count: paths.length
	});

	console.log(`  Wrote ${paths.length} paths to ${txtPath}`);
	return true;
}
