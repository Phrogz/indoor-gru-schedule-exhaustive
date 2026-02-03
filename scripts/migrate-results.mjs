#!/usr/bin/env node
// Migrate existing .js result files to new .txt streaming format

import { readdir, readFile } from 'fs/promises';
import { existsSync } from 'fs';
import { pathToFileURL } from 'url';
import { resolve } from 'path';
import { TreeWriter } from '../lib/tree-format.mjs';

const RESULTS_DIR = 'results';

// Parse command line args
const args = process.argv.slice(2);
let dryRun = false;
let specificFile = null;

for (const arg of args) {
	if (arg === '--dry-run') dryRun = true;
	else if (!arg.startsWith('--')) specificFile = arg;
}

// Detect tree node types (from display-schedules.mjs)
function isTreeNode(item) {
	return Array.isArray(item) && item.length === 2 && Array.isArray(item[0]) && Array.isArray(item[1]);
}

function isLeafNode(item) {
	return Array.isArray(item) && item.length === 1 && Array.isArray(item[0]);
}

// Flatten tree into array of complete paths
function flattenTree(node, path = []) {
	if (isLeafNode(node)) {
		return [path.concat([Array.from(node[0])])];
	} else if (isTreeNode(node)) {
		const [schedule, children] = node;
		const newPath = path.concat([Array.from(schedule)]);
		const results = [];
		for (const child of children) {
			results.push(...flattenTree(child, newPath));
		}
		return results;
	} else if (Array.isArray(node) && node.every(n => typeof n === 'number')) {
		return [[Array.from(node)]];
	}
	return [];
}

// Parse header comments from .js files
function parseJsHeader(content) {
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

async function migrateFile(jsPath) {
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

async function main() {
	console.log('Tournament Results Migration Tool');
	console.log('Converts .js result files to .txt streaming format\n');

	if (dryRun) {
		console.log('DRY RUN MODE - no files will be written\n');
	}

	let filesToProcess = [];

	if (specificFile) {
		filesToProcess = [specificFile];
	} else {
		if (!existsSync(RESULTS_DIR)) {
			console.error(`Results directory not found: ${RESULTS_DIR}`);
			process.exit(1);
		}

		const files = await readdir(RESULTS_DIR);
		filesToProcess = files
			.filter(f => f.endsWith('.js'))
			.map(f => `${RESULTS_DIR}/${f}`);
	}

	if (filesToProcess.length === 0) {
		console.log('No .js files found to migrate');
		return;
	}

	console.log(`Found ${filesToProcess.length} files to process`);

	let success = 0;
	let failed = 0;

	for (const file of filesToProcess) {
		try {
			const ok = await migrateFile(file);
			if (ok) success++;
			else failed++;
		} catch (err) {
			console.error(`  Error: ${err.message}`);
			failed++;
		}
	}

	console.log(`\n${'='.repeat(40)}`);
	console.log(`Migration complete: ${success} succeeded, ${failed} failed`);

	if (!dryRun && success > 0) {
		console.log('\nNew .txt files created alongside .js files.');
		console.log('After verifying, you can delete the old .js files.');
	}
}

main().catch(console.error);
