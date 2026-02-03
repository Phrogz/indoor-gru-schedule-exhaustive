// Tournament Schedule Evaluator
// Reads multi-week schedules and scores them using weighted pain metrics

import { writeFileSync } from 'fs';
import { TreeReader } from './lib/tree-format.mjs';

// ============================================================================
// Configuration
// ============================================================================

const overallPainMultipliers = {
	doubleHeaderPain: 0.05,
	unfairDoubleHeaderPain: 0.5,
	// doubleByePain: 3,
	unfairDoubleByePain: 0.5,
	totalSlotsPain: 0.01,
	unfairSlotsPain: 0.1,
	// unfairPainPerTeam: 0.3,
	unfairEarlyLate: 0.4,
	unfair3rdVs2nd: 0.5,
	unevenMatchups: 1.0,
};

// ============================================================================
// CLI Argument Parsing
// ============================================================================

function parseArgs() {
	const args = process.argv.slice(2);

	if (args.includes('--help') || args.includes('-h')) {
		console.log(`
Tournament Schedule Evaluator

Usage: node evaluate.mjs [options]

Options:
  --teams=N     Number of teams (must be even, 4-16)
  --weeks=N     Number of weeks in schedules
  --help, -h    Show this help message

Example:
  node evaluate.mjs --teams=6 --weeks=5

Reads results/{N}teams-{W}weeks.txt and evaluates all schedules.
Outputs best schedules to results/{N}teams-{W}weeks-best.txt
`);
		process.exit(0);
	}

	let teams = null, weeks = null;
	for (const arg of args) {
		if (arg.startsWith('--teams=')) teams = parseInt(arg.slice(8));
		else if (arg.startsWith('--weeks=')) weeks = parseInt(arg.slice(8));
	}

	if (!teams || !weeks) {
		console.error('Error: --teams and --weeks are required');
		process.exit(1);
	}

	return { teams, weeks };
}

// ============================================================================
// Constants (set after parsing args)
// ============================================================================

const CONFIG = parseArgs();
const N_TEAMS = CONFIG.teams;
const N_WEEKS = CONFIG.weeks;
const TEAMS = 'ABCDEFGHIJKLMNOP'.slice(0, N_TEAMS).split('');
const N_SLOTS = (N_TEAMS * 3) / 2;
const N_MATCHUPS = (N_TEAMS * (N_TEAMS - 1)) / 2;

// Precompute matchup encoding/decoding
const matchupToTeams = new Uint8Array(N_MATCHUPS * 2);

let idx = 0;
for (let ti1 = 0; ti1 < N_TEAMS - 1; ti1++) {
	for (let ti2 = ti1 + 1; ti2 < N_TEAMS; ti2++) {
		matchupToTeams[idx * 2] = ti1;
		matchupToTeams[idx * 2 + 1] = ti2;
		idx++;
	}
}

// ============================================================================
// Utility Functions
// ============================================================================

function stddev(values) {
	if (values.length === 0) return 0;
	const mean = values.reduce((a, b) => a + b, 0) / values.length;
	const squaredDiffs = values.map(v => (v - mean) ** 2);
	return Math.sqrt(squaredDiffs.reduce((a, b) => a + b, 0) / values.length);
}

function sum(values) {
	return values.reduce((a, b) => a + b, 0);
}

function matchupToString(matchupIdx) {
	const t1 = matchupToTeams[matchupIdx * 2];
	const t2 = matchupToTeams[matchupIdx * 2 + 1];
	return `${TEAMS[t1]}v${TEAMS[t2]}`;
}

// ============================================================================
// Per-Week Metric Functions
// Each returns a result object keyed by metric name
// ============================================================================

const weekMetrics = {
	/**
	 * Count of consecutive-slot games (gap = 0) per team
	 * A "double header" is when a team plays in slot N and slot N+1
	 */
	doubleHeaderCountByTeam(schedule, nTeams, nSlots) {
		// First, collect slots per team
		const slotsByTeam = [];
		for (let ti = 0; ti < nTeams; ti++) slotsByTeam.push([]);

		for (let s = 0; s < nSlots; s++) {
			const m = schedule[s];
			const t1 = matchupToTeams[m * 2];
			const t2 = matchupToTeams[m * 2 + 1];
			slotsByTeam[t1].push(s);
			slotsByTeam[t2].push(s);
		}

		const counts = new Uint8Array(nTeams);
		for (let ti = 0; ti < nTeams; ti++) {
			const slots = slotsByTeam[ti];
			for (let i = 0; i < slots.length - 1; i++) {
				if (slots[i + 1] - slots[i] === 1) counts[ti]++;
			}
		}
		return counts;
	},

	/**
	 * Count of 3-slot gaps between consecutive games per team
	 * A "double bye" is when a team has a gap of 3 slots between games
	 */
	doubleByeCountByTeam(schedule, nTeams, nSlots) {
		const slotsByTeam = [];
		for (let ti = 0; ti < nTeams; ti++) slotsByTeam.push([]);

		for (let s = 0; s < nSlots; s++) {
			const m = schedule[s];
			const t1 = matchupToTeams[m * 2];
			const t2 = matchupToTeams[m * 2 + 1];
			slotsByTeam[t1].push(s);
			slotsByTeam[t2].push(s);
		}

		const counts = new Uint8Array(nTeams);
		for (let ti = 0; ti < nTeams; ti++) {
			const slots = slotsByTeam[ti];
			for (let i = 0; i < slots.length - 1; i++) {
				if (slots[i + 1] - slots[i] === 3) counts[ti]++;
			}
		}
		return counts;
	},

	/**
	 * Array of slot indices where each team plays
	 */
	weekSlotsByTeam(schedule, nTeams, nSlots) {
		const slotsByTeam = [];
		for (let ti = 0; ti < nTeams; ti++) slotsByTeam.push([]);

		for (let s = 0; s < nSlots; s++) {
			const m = schedule[s];
			const t1 = matchupToTeams[m * 2];
			const t2 = matchupToTeams[m * 2 + 1];
			slotsByTeam[t1].push(s);
			slotsByTeam[t2].push(s);
		}
		return slotsByTeam;
	},

	/**
	 * Count of games in first two slots (0, 1) per team
	 */
	earlyGamesByTeam(schedule, nTeams, nSlots) {
		const counts = new Uint8Array(nTeams);
		for (let s = 0; s < Math.min(2, nSlots); s++) {
			const m = schedule[s];
			const t1 = matchupToTeams[m * 2];
			const t2 = matchupToTeams[m * 2 + 1];
			counts[t1]++;
			counts[t2]++;
		}
		return counts;
	},

	/**
	 * Count of games in last two slots per team
	 */
	lateGamesByTeam(schedule, nTeams, nSlots) {
		const counts = new Uint8Array(nTeams);
		const startSlot = Math.max(0, nSlots - 2);
		for (let s = startSlot; s < nSlots; s++) {
			const m = schedule[s];
			const t1 = matchupToTeams[m * 2];
			const t2 = matchupToTeams[m * 2 + 1];
			counts[t1]++;
			counts[t2]++;
		}
		return counts;
	},

	/**
	 * Count of times each team plays their 3rd game against another team's 2nd game
	 */
	game3rdVs2ndByTeam(schedule, nTeams, nSlots) {
		// First, find each team's game slots
		const slotsByTeam = [];
		for (let ti = 0; ti < nTeams; ti++) slotsByTeam.push([]);

		for (let s = 0; s < nSlots; s++) {
			const m = schedule[s];
			const t1 = matchupToTeams[m * 2];
			const t2 = matchupToTeams[m * 2 + 1];
			slotsByTeam[t1].push(s);
			slotsByTeam[t2].push(s);
		}

		// Count times a team's 3rd game is against opponent's 2nd
		const counts = new Uint8Array(nTeams);
		for (let ti = 0; ti < nTeams; ti++) {
			const mySlots = slotsByTeam[ti];
			if (mySlots.length < 3) continue;
			const my3rdSlot = mySlots[2];
			// Find opponent in that slot
			const m = schedule[my3rdSlot];
			const t1 = matchupToTeams[m * 2];
			const t2 = matchupToTeams[m * 2 + 1];
			const opponent = (t1 === ti) ? t2 : t1;
			// Check if this is opponent's 2nd game
			if (slotsByTeam[opponent].length >= 2 && slotsByTeam[opponent][1] === my3rdSlot) {
				counts[ti]++;
			}
		}
		return counts;
	},
};

// ============================================================================
// Week Metric Cache
// ============================================================================

const weekMetricCache = new Map();

function getWeekMetrics(schedule) {
	const key = schedule.join(',');
	if (weekMetricCache.has(key)) {
		return weekMetricCache.get(key);
	}

	const results = {};
	for (const [name, fn] of Object.entries(weekMetrics)) {
		results[name] = fn(schedule, N_TEAMS, N_SLOTS);
	}
	weekMetricCache.set(key, results);
	return results;
}

// ============================================================================
// Overall Pain Functions
// Each has score() returning a number and explain() returning a string
// ============================================================================

const overallPainFunctions = {
	/**
	 * Sum of all teams' double-header counts across all weeks
	 */
	doubleHeaderPain: {
		score(weekResults, nTeams) {
			let total = 0;
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					total += week.doubleHeaderCountByTeam[ti];
				}
			}
			return total;
		},
		explain(weekResults, nTeams) {
			const perTeam = new Array(nTeams).fill(0);
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					perTeam[ti] += week.doubleHeaderCountByTeam[ti];
				}
			}
			return perTeam.map((c, i) => `${TEAMS[i]}:${c}`).join(' ');
		},
	},

	/**
	 * Stddev of double-header counts per team across all weeks
	 */
	unfairDoubleHeaderPain: {
		score(weekResults, nTeams) {
			const perTeam = new Array(nTeams).fill(0);
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					perTeam[ti] += week.doubleHeaderCountByTeam[ti];
				}
			}
			return stddev(perTeam);
		},
		explain(weekResults, nTeams) {
			const perTeam = new Array(nTeams).fill(0);
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					perTeam[ti] += week.doubleHeaderCountByTeam[ti];
				}
			}
			return perTeam.map((c, i) => `${TEAMS[i]}:${c}`).join(' ');
		},
	},

	/**
	 * Sum of all teams' double-bye counts across all weeks
	 */
	doubleByePain: {
		score(weekResults, nTeams) {
			let total = 0;
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					total += week.doubleByeCountByTeam[ti];
				}
			}
			return total;
		},
		explain(weekResults, nTeams) {
			const perTeam = new Array(nTeams).fill(0);
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					perTeam[ti] += week.doubleByeCountByTeam[ti];
				}
			}
			return perTeam.map((c, i) => `${TEAMS[i]}:${c}`).join(' ');
		},
	},

	/**
	 * Stddev of double-bye counts per team across all weeks
	 */
	unfairDoubleByePain: {
		score(weekResults, nTeams) {
			const perTeam = new Array(nTeams).fill(0);
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					perTeam[ti] += week.doubleByeCountByTeam[ti];
				}
			}
			return stddev(perTeam);
		},
		explain(weekResults, nTeams) {
			const perTeam = new Array(nTeams).fill(0);
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					perTeam[ti] += week.doubleByeCountByTeam[ti];
				}
			}
			return perTeam.map((c, i) => `${TEAMS[i]}:${c}`).join(' ');
		},
	},

	/**
	 * Sum of slot spans (last - first + 1) per team per week, across all weeks
	 */
	totalSlotsPain: {
		score(weekResults, nTeams) {
			let total = 0;
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					const slots = week.weekSlotsByTeam[ti];
					if (slots.length > 0) {
						const span = slots[slots.length - 1] - slots[0] + 1;
						total += span;
					}
				}
			}
			return total;
		},
		explain(weekResults, nTeams) {
			let total = 0;
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					const slots = week.weekSlotsByTeam[ti];
					if (slots.length > 0) {
						total += slots[slots.length - 1] - slots[0] + 1;
					}
				}
			}
			return `${total} total slots spanned`;
		},
	},

	/**
	 * Stddev across teams of (sum of slot spans per team across all weeks)
	 */
	unfairSlotsPain: {
		score(weekResults, nTeams) {
			const perTeam = new Array(nTeams).fill(0);
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					const slots = week.weekSlotsByTeam[ti];
					if (slots.length > 0) {
						const span = slots[slots.length - 1] - slots[0] + 1;
						perTeam[ti] += span;
					}
				}
			}
			return stddev(perTeam);
		},
		explain(weekResults, nTeams) {
			const perTeam = new Array(nTeams).fill(0);
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					const slots = week.weekSlotsByTeam[ti];
					if (slots.length > 0) {
						const span = slots[slots.length - 1] - slots[0] + 1;
						perTeam[ti] += span;
					}
				}
			}
			return perTeam.map((c, i) => `${TEAMS[i]}:${c}`).join(' ');
		},
	},

	/**
	 * Stddev across teams of (sum of doubleHeader + doubleBye pain per team)
	 */
	unfairPainPerTeam: {
		score(weekResults, nTeams) {
			const perTeam = new Array(nTeams).fill(0);
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					perTeam[ti] += week.doubleHeaderCountByTeam[ti];
					perTeam[ti] += week.doubleByeCountByTeam[ti];
				}
			}
			return stddev(perTeam);
		},
		explain(weekResults, nTeams) {
			const perTeam = new Array(nTeams).fill(0);
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					perTeam[ti] += week.doubleHeaderCountByTeam[ti];
					perTeam[ti] += week.doubleByeCountByTeam[ti];
				}
			}
			return perTeam.map((c, i) => `${TEAMS[i]}:${c}`).join(' ');
		},
	},

	/**
	 * Stddev of early games per team + stddev of late games per team
	 */
	unfairEarlyLate: {
		score(weekResults, nTeams) {
			const earlyPerTeam = new Array(nTeams).fill(0);
			const latePerTeam = new Array(nTeams).fill(0);
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					earlyPerTeam[ti] += week.earlyGamesByTeam[ti];
					latePerTeam[ti] += week.lateGamesByTeam[ti];
				}
			}
			return stddev(earlyPerTeam) + stddev(latePerTeam);
		},
		explain(weekResults, nTeams) {
			const earlyPerTeam = new Array(nTeams).fill(0);
			const latePerTeam = new Array(nTeams).fill(0);
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					earlyPerTeam[ti] += week.earlyGamesByTeam[ti];
					latePerTeam[ti] += week.lateGamesByTeam[ti];
				}
			}
			return TEAMS.map((t, i) => `${t}:[${earlyPerTeam[i]},${latePerTeam[i]}]`).join(' ');
		},
	},

	/**
	 * Stddev of times each team plays their 3rd game against another team's 2nd
	 */
	unfair3rdVs2nd: {
		score(weekResults, nTeams) {
			const perTeam = new Array(nTeams).fill(0);
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					perTeam[ti] += week.game3rdVs2ndByTeam[ti];
				}
			}
			return stddev(perTeam);
		},
		explain(weekResults, nTeams) {
			const perTeam = new Array(nTeams).fill(0);
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					perTeam[ti] += week.game3rdVs2ndByTeam[ti];
				}
			}
			return perTeam.map((c, i) => `${TEAMS[i]}:${c}`).join(' ');
		},
	},

	/**
	 * 100 if any team plays another with max-min > 1, else 0
	 */
	unevenMatchups: {
		score(weekResults, nTeams, path) {
			// Build matchup count matrix
			const counts = Array.from({ length: nTeams }, () => new Array(nTeams).fill(0));
			for (const week of path) {
				for (const matchupIdx of week) {
					const t1 = matchupToTeams[matchupIdx * 2];
					const t2 = matchupToTeams[matchupIdx * 2 + 1];
					counts[t1][t2]++;
					counts[t2][t1]++;
				}
			}
			// For each team, check if max - min > 1
			for (let ti = 0; ti < nTeams; ti++) {
				const opponents = counts[ti].filter((_, j) => j !== ti);
				const min = Math.min(...opponents);
				const max = Math.max(...opponents);
				if (max - min > 1) return 100;
			}
			return 0;
		},
		explain(weekResults, nTeams, path) {
			// Build matchup count matrix
			const counts = Array.from({ length: nTeams }, () => new Array(nTeams).fill(0));
			for (const week of path) {
				for (const matchupIdx of week) {
					const t1 = matchupToTeams[matchupIdx * 2];
					const t2 = matchupToTeams[matchupIdx * 2 + 1];
					counts[t1][t2]++;
					counts[t2][t1]++;
				}
			}
			// Find teams with uneven matchups
			const uneven = [];
			for (let ti = 0; ti < nTeams; ti++) {
				const opponents = counts[ti].filter((_, j) => j !== ti);
				const min = Math.min(...opponents);
				const max = Math.max(...opponents);
				if (max - min > 1) {
					uneven.push(`${TEAMS[ti]}:${min}-${max}`);
				}
			}
			return uneven.length > 0 ? uneven.join(' ') : 'balanced';
		},
	},
};

// ============================================================================
// Score Calculation
// ============================================================================

function calculateOverallScore(weekResults, path) {
	let totalScore = 0;
	for (const [painName, multiplier] of Object.entries(overallPainMultipliers)) {
		if (multiplier > 0) {
			const painFn = overallPainFunctions[painName];
			const score = painFn.score(weekResults, N_TEAMS, path);
			totalScore += score * multiplier;
		}
	}
	return totalScore;
}

function explainScore(weekResults, path) {
	const metrics = [];
	let totalScore = 0;

	// Find the longest metric name for alignment
	const maxNameLen = Math.max(...Object.keys(overallPainMultipliers).map(n => n.length));

	for (const [painName, multiplier] of Object.entries(overallPainMultipliers)) {
		const painFn = overallPainFunctions[painName];
		const rawScore = painFn.score(weekResults, N_TEAMS, path);
		const weighted = rawScore * multiplier;
		totalScore += weighted;
		const teamDetails = painFn.explain(weekResults, N_TEAMS, path);
		const paddedName = painName.padEnd(maxNameLen);
		metrics.push({
			name: paddedName,
			weighted,
			rawScore,
			multiplier,
			teamDetails
		});
	}

	const formatMetric = (m) => {
		return `${m.name}: ${m.weighted.toFixed(2).padStart(5)} (${m.multiplier} * ${m.rawScore.toFixed(1)}) ${m.teamDetails}`;
	};

	return {
		totalScore,
		metrics,
		toString() {
			return `Total Score: ${totalScore.toFixed(4)}\n${metrics.map(formatMetric).join('\n')}`;
		},
		toFileString() {
			return `# Total Score: ${totalScore.toFixed(4)}\n${metrics.map(formatMetric).join('\n')}`;
		}
	};
}

// ============================================================================
// Schedule Formatting
// ============================================================================

function formatSchedule(path) {
	return path.map((week, i) => `Week ${i + 1}: ${week.map(matchupToString).join(' ')}`).join('\n');
}

function formatScheduleCompact(path) {
	return path.map(week => week.map(matchupToString).join(' ')).join('\n');
}

/**
 * Generate a grid showing how many times each team plays each other
 */
function formatTeamMatchupsGrid(path) {
	// Build matchup count matrix
	const counts = Array.from({ length: N_TEAMS }, () => new Array(N_TEAMS).fill(0));

	for (const week of path) {
		for (const matchupIdx of week) {
			const t1 = matchupToTeams[matchupIdx * 2];
			const t2 = matchupToTeams[matchupIdx * 2 + 1];
			counts[t1][t2]++;
			counts[t2][t1]++;
		}
	}

	// Build grid string
	const lines = [];
	// Header row
	lines.push('  ' + TEAMS.join(' '));
	// Data rows
	for (let ti = 0; ti < N_TEAMS; ti++) {
		const row = TEAMS[ti] + ' ' + counts[ti].map((c, j) => ti === j ? '·' : c).join(' ');
		lines.push(row);
	}

	return lines.join('\n');
}

// ============================================================================
// Main Evaluation
// ============================================================================

async function main() {
	const inputPath = `results/${N_TEAMS}teams-${N_WEEKS}weeks.txt`;
	const outputPath = `results/${N_TEAMS}teams-${N_WEEKS}weeks-best.txt`;

	console.log(`Evaluating schedules from: ${inputPath}`);
	console.log(`Teams: ${N_TEAMS}, Weeks: ${N_WEEKS}, Slots per week: ${N_SLOTS}`);
	console.log(`Pain multipliers:`, overallPainMultipliers);
	console.log();

	const reader = new TreeReader(inputPath);
	const header = await reader.readHeader();
	console.log(`File header: teams=${header.teams} weeks=${header.weeks} count=${header.count}`);
	console.log();

	let bestScore = Infinity;
	let bestSchedules = [];
	let evaluated = 0;

	for await (const path of reader.paths()) {
		evaluated++;

		// Gather week metrics (cached)
		const weekResults = path.map(week => getWeekMetrics(week));

		// Calculate overall score
		const score = calculateOverallScore(weekResults, path);

		if (score < bestScore) {
			// New best - reset list
			bestScore = score;
			bestSchedules = [{ path, weekResults, scheduleNum: evaluated }];

			const explained = explainScore(weekResults, path);
			console.log(`Schedule ${evaluated}/${header.count} :: Score: ${explained.totalScore.toFixed(4)}`);
			console.log(formatSchedule(path));
			console.log(explained.toString().split('\n').slice(1).join('\n'));
			console.log('Team Matchups:');
			console.log(formatTeamMatchupsGrid(path));
			console.log();
		} else if (score === bestScore) {
			// Equal to best - add to list
			bestSchedules.push({ path, weekResults, scheduleNum: evaluated });

			const explained = explainScore(weekResults, path);
			console.log(`Schedule ${evaluated}/${header.count} :: Score: ${explained.totalScore.toFixed(4)}`);
			console.log(formatSchedule(path));
			console.log(explained.toString().split('\n').slice(1).join('\n'));
			console.log('Team Matchups:');
			console.log(formatTeamMatchupsGrid(path));
			console.log();
		}
	}

	console.log(`\n${'='.repeat(60)}`);
	console.log(`Evaluation complete.`);
	console.log(`Total schedules evaluated: ${evaluated}`);
	console.log(`Week schedules cached: ${weekMetricCache.size}`);
	console.log(`Best score: ${bestScore.toFixed(4)}`);
	console.log(`Schedules with best score: ${bestSchedules.length}`);

	// Write results to file
	const outputLines = [];
	outputLines.push(`# teams=${N_TEAMS} weeks=${N_WEEKS} count=${bestSchedules.length}`);
	outputLines.push(`# Pain multipliers: ${JSON.stringify(overallPainMultipliers)}`);
	outputLines.push('');

	for (const { path, weekResults, scheduleNum } of bestSchedules) {
		const explained = explainScore(weekResults, path);
		outputLines.push(`Schedule ${scheduleNum}/${evaluated} :: Score: ${explained.totalScore.toFixed(4)}`);
		outputLines.push(formatSchedule(path));
		outputLines.push(explained.toString().split('\n').slice(1).join('\n'));
		outputLines.push('Team Matchups:');
		outputLines.push(formatTeamMatchupsGrid(path));
		outputLines.push('');
	}

	writeFileSync(outputPath, outputLines.join('\n'));
	console.log(`\nResults written to: ${outputPath}`);
}

main().catch(err => {
	console.error('Error:', err);
	process.exit(1);
});
