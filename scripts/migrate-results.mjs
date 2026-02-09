#!/usr/bin/env node
// Migrate existing .js result files to new .txt streaming format

import { readdir } from 'fs/promises';
import { existsSync } from 'fs';
import { migrateResultsFile } from '../lib/results-migrate.mjs';

const RESULTS_DIR = 'results';

// Parse command line args
const args = process.argv.slice(2);
let dryRun = false;
let specificFile = null;

for (const arg of args) {
	if (arg === '--dry-run') dryRun = true;
	else if (!arg.startsWith('--')) specificFile = arg;
}

async function migrateFile(jsPath) {
	return migrateResultsFile(jsPath, { dryRun });
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
