// Tournament Schedule Evaluator
// Reads multi-week schedules and scores them using weighted pain metrics

import { writeFileSync } from 'fs';
import { performance } from 'perf_hooks';
import { TreeReader } from './lib/tree-format.mjs';

// ============================================================================
// Configuration
// ============================================================================

const painConfig = {
	doubleHeaderPain: { overall: 0.1,  unfairness: 1 },
	doubleByePain:    { overall: 0.1,  unfairness: 2 },
	totalSlotsPain:   { overall: 0.01, unfairness: 4 },
	earlyWeeks:       { overall: 0.01, unfairness: 1 },
	lateWeeks:        { overall: 0.01, unfairness: 1 },
	thirdVs2nd:       { overall: 0.05, unfairness: 5 },
	// Schedules coming in should not have any uneven matchups
	// unevenMatchups: { overall: 99.0, unfairness: 0 },
};
const unfairnessOverallWeight = 1;

// ============================================================================
// CLI Argument Parsing
// ============================================================================

function parseArgs() {
	const args = process.argv.slice(2);

	if (args.includes('--help') || args.includes('-h')) {
		console.log(`
Tournament Schedule Evaluator

Usage: node evaluate.mjs <file> [options]
       node evaluate.mjs --teams=N --weeks=N [options]

Arguments:
  <file>        Path to results file (reads teams/weeks from header)

Options:
  --teams=N     Number of teams (optional if file provided)
  --weeks=N     Number of weeks (optional if file provided)
  --help, -h    Show this help message

Examples:
  node evaluate.mjs results/6teams-5weeks.txt
  node evaluate.mjs --teams=6 --weeks=5

Outputs best schedules to results/{N}teams-{W}weeks-best.txt
`);
		process.exit(0);
	}

	let teams = null, weeks = null, inputFile = null;
	for (const arg of args) {
		if (arg.startsWith('--teams=')) teams = parseInt(arg.slice(8));
		else if (arg.startsWith('--weeks=')) weeks = parseInt(arg.slice(8));
		else if (!arg.startsWith('-')) inputFile = arg;
	}

	return { teams, weeks, inputFile };
}

// ============================================================================
// Constants (initialized by initConstants after reading file header)
// ============================================================================

const CLI_ARGS = parseArgs();
let N_TEAMS, N_WEEKS, TEAMS, N_SLOTS, N_MATCHUPS, matchupToTeams;
const painTiming = new Map();

function initConstants(teams, weeks) {
	N_TEAMS = teams;
	N_WEEKS = weeks;
	TEAMS = 'ABCDEFGHIJKLMNOP'.slice(0, N_TEAMS).split('');
	N_SLOTS = (N_TEAMS * 3) / 2;
	N_MATCHUPS = (N_TEAMS * (N_TEAMS - 1)) / 2;

	// Precompute matchup encoding/decoding
	matchupToTeams = new Uint8Array(N_MATCHUPS * 2);

	let idx = 0;
	for (let ti1 = 0; ti1 < N_TEAMS - 1; ti1++) {
		for (let ti2 = ti1 + 1; ti2 < N_TEAMS; ti2++) {
			matchupToTeams[idx * 2] = ti1;
			matchupToTeams[idx * 2 + 1] = ti2;
			idx++;
		}
	}
}

// ============================================================================
// Utility Functions
// ============================================================================

function stddev(values) {
	if (values.length === 0) return 0;
	let total = 0;
	for (let i = 0; i < values.length; i++) total += values[i];
	const mean = total / values.length;
	let squaredSum = 0;
	for (let i = 0; i < values.length; i++) {
		const diff = values[i] - mean;
		squaredSum += diff * diff;
	}
	return Math.sqrt(squaredSum / values.length);
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
	 * Count of weeks where each team plays in the first two slots (0, 1)
	 */
	earlyWeeksByTeam(schedule, nTeams, nSlots) {
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
		const earlyCutoff = Math.min(2, nSlots);
		for (let ti = 0; ti < nTeams; ti++) {
			const slots = slotsByTeam[ti];
			for (let i = 0; i < slots.length; i++) {
				if (slots[i] < earlyCutoff) {
					counts[ti] = 1;
					break;
				}
			}
		}
		return counts;
	},

	/**
	 * Count of weeks where each team plays in the last two slots
	 */
	lateWeeksByTeam(schedule, nTeams, nSlots) {
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
		const startSlot = Math.max(0, nSlots - 2);
		for (let ti = 0; ti < nTeams; ti++) {
			const slots = slotsByTeam[ti];
			for (let i = 0; i < slots.length; i++) {
				if (slots[i] >= startSlot) {
					counts[ti] = 1;
					break;
				}
			}
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
	if (results.weekSlotsByTeam) {
		const spans = new Uint8Array(N_TEAMS);
		for (let ti = 0; ti < N_TEAMS; ti++) {
			const slots = results.weekSlotsByTeam[ti];
			if (slots.length > 0) {
				spans[ti] = slots[slots.length - 1] - slots[0] + 1;
			}
		}
		results.weekSlotSpanByTeam = spans;
	}
	weekMetricCache.set(key, results);
	return results;
}

// ============================================================================
// Base Pain Functions
// Each has score() returning a total, perTeam() returning per-team array,
// and explain() returning a display string
// ============================================================================

const basePainFunctions = {
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
		perTeam(weekResults, nTeams) {
			const perTeam = new Array(nTeams).fill(0);
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					perTeam[ti] += week.doubleHeaderCountByTeam[ti];
				}
			}
			return perTeam;
		},
		explain(weekResults, nTeams) {
			return this.perTeam(weekResults, nTeams).map((c, i) => `${TEAMS[i]}:${c}`).join(' ');
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
		perTeam(weekResults, nTeams) {
			const perTeam = new Array(nTeams).fill(0);
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					perTeam[ti] += week.doubleByeCountByTeam[ti];
				}
			}
			return perTeam;
		},
		explain(weekResults, nTeams) {
			return this.perTeam(weekResults, nTeams).map((c, i) => `${TEAMS[i]}:${c}`).join(' ');
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
						total += slots[slots.length - 1] - slots[0] + 1;
					}
				}
			}
			return total;
		},
		perTeam(weekResults, nTeams) {
			const perTeam = new Array(nTeams).fill(0);
			for (const week of weekResults) {
				const spans = week.weekSlotSpanByTeam;
				for (let ti = 0; ti < nTeams; ti++) {
					perTeam[ti] += spans[ti];
				}
			}
			return perTeam;
		},
		explain(weekResults, nTeams) {
			return this.perTeam(weekResults, nTeams).map((c, i) => `${TEAMS[i]}:${c}`).join(' ');
		},
	},

	/**
	 * Sum of early-week counts per team across all weeks
	 */
	earlyWeeks: {
		score(weekResults, nTeams) {
			let total = 0;
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					total += week.earlyWeeksByTeam[ti];
				}
			}
			return total;
		},
		perTeam(weekResults, nTeams) {
			const perTeam = new Array(nTeams).fill(0);
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					perTeam[ti] += week.earlyWeeksByTeam[ti];
				}
			}
			return perTeam;
		},
		explain(weekResults, nTeams) {
			return this.perTeam(weekResults, nTeams).map((c, i) => `${TEAMS[i]}:${c}`).join(' ');
		},
	},

	/**
	 * Sum of late-week counts per team across all weeks
	 */
	lateWeeks: {
		score(weekResults, nTeams) {
			let total = 0;
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					total += week.lateWeeksByTeam[ti];
				}
			}
			return total;
		},
		perTeam(weekResults, nTeams) {
			const perTeam = new Array(nTeams).fill(0);
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					perTeam[ti] += week.lateWeeksByTeam[ti];
				}
			}
			return perTeam;
		},
		explain(weekResults, nTeams) {
			return this.perTeam(weekResults, nTeams).map((c, i) => `${TEAMS[i]}:${c}`).join(' ');
		},
	},

	/**
	 * Sum of times each team plays their 3rd game against another team's 2nd
	 */
	thirdVs2nd: {
		score(weekResults, nTeams) {
			let total = 0;
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					total += week.game3rdVs2ndByTeam[ti];
				}
			}
			return total;
		},
		perTeam(weekResults, nTeams) {
			const perTeam = new Array(nTeams).fill(0);
			for (const week of weekResults) {
				for (let ti = 0; ti < nTeams; ti++) {
					perTeam[ti] += week.game3rdVs2ndByTeam[ti];
				}
			}
			return perTeam;
		},
		explain(weekResults, nTeams) {
			return this.perTeam(weekResults, nTeams).map((c, i) => `${TEAMS[i]}:${c}`).join(' ');
		},
	},

	/**
	 * 100 if any team plays another with max-min > 1, else 0
	 * Allows unevenness of at most ±1 (e.g., partial final round)
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
		perTeam() { return []; },
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

function formatNum(v) {
	return Number.isInteger(v) ? String(v) : v.toFixed(1);
}

/**
 * Calculate the unified unfairness score: sum of (weight × stdev) per metric.
 * This measures per-metric fairness independently, preventing unfairness in one
 * dimension from being masked by compensation in another.
 * Returns { perTeamPain, perMetricPerTeam, score }
 */
function calculateUnfairness(weekResults, nTeams) {
	const perTeamPain = new Array(nTeams).fill(0);
	const perMetricPerTeam = {};
	let score = 0;
	for (const [metric, config] of Object.entries(painConfig)) {
		if (config.unfairness > 0) {
			const perTeam = basePainFunctions[metric].perTeam(weekResults, nTeams);
			perMetricPerTeam[metric] = { perTeam, weight: config.unfairness };
			score += config.unfairness * stddev(perTeam);
			for (let ti = 0; ti < nTeams; ti++) {
				perTeamPain[ti] += perTeam[ti] * config.unfairness;
			}
		}
	}
	return { perTeamPain, perMetricPerTeam, score };
}

const UNFAIRNESS_METRIC_SPECS = [
	{ name: 'doubleHeaderPain', weekField: 'doubleHeaderCountByTeam' },
	{ name: 'doubleByePain', weekField: 'doubleByeCountByTeam' },
	{ name: 'totalSlotsPain', weekField: 'weekSlotSpanByTeam' },
	{ name: 'earlyWeeks', weekField: 'earlyWeeksByTeam' },
	{ name: 'lateWeeks', weekField: 'lateWeeksByTeam' },
	{ name: 'thirdVs2nd', weekField: 'game3rdVs2ndByTeam' },
];

function calculateUnfairnessFromTotals(perMetricTotals, nTeams) {
	const perTeamPain = new Array(nTeams).fill(0);
	const perMetricPerTeam = {};
	let score = 0;
	for (const { name } of UNFAIRNESS_METRIC_SPECS) {
		const weight = painConfig[name]?.unfairness ?? 0;
		if (weight <= 0) continue;
		const perTeam = perMetricTotals[name];
		perMetricPerTeam[name] = { perTeam, weight };
		score += weight * stddev(perTeam);
		for (let ti = 0; ti < nTeams; ti++) {
			perTeamPain[ti] += perTeam[ti] * weight;
		}
	}
	return { perTeamPain, perMetricPerTeam, score };
}

// ============================================================================
// Score Calculation
// ============================================================================

function calculateOverallScore(weekResults, path, precomputedUnfairnessScore = null) {
	let totalScore = 0;
	for (const [painName, config] of Object.entries(painConfig)) {
		if (config.overall > 0) {
			const painFn = basePainFunctions[painName];
			const start = performance.now();
			const score = painFn.score(weekResults, N_TEAMS, path);
			const elapsed = performance.now() - start;
			const timing = painTiming.get(painName) ?? { totalMs: 0, calls: 0 };
			timing.totalMs += elapsed;
			timing.calls += 1;
			painTiming.set(painName, timing);
			totalScore += score * config.overall;
		}
	}
	if (unfairnessOverallWeight > 0) {
		const start = performance.now();
		const unfairScore = precomputedUnfairnessScore ?? calculateUnfairness(weekResults, N_TEAMS).score;
		totalScore += unfairScore * unfairnessOverallWeight;
		const elapsed = performance.now() - start;
		const timing = painTiming.get('unfairPainPerTeam') ?? { totalMs: 0, calls: 0 };
		timing.totalMs += elapsed;
		timing.calls += 1;
		painTiming.set('unfairPainPerTeam', timing);
	}
	return totalScore;
}

function explainScore(weekResults, path, precomputedUnfairness = null) {
	const lines = [];
	let totalScore = 0;

	// Compute all values first
	const metricData = [];
	for (const [painName, config] of Object.entries(painConfig)) {
		const painFn = basePainFunctions[painName];
		const rawScore = painFn.score(weekResults, N_TEAMS, path);
		const weighted = rawScore * config.overall;
		const teamDetails = painFn.explain(weekResults, N_TEAMS, path);
		metricData.push({ name: painName, weighted, rawScore, weight: config.overall, teamDetails });
		totalScore += weighted;
	}

	const { perTeamPain, perMetricPerTeam, score: unfairScore } = precomputedUnfairness
		? precomputedUnfairness
		: calculateUnfairness(weekResults, N_TEAMS);
	const unfairWeighted = unfairScore * unfairnessOverallWeight;
	totalScore += unfairWeighted;

	// Compute alignment widths for main metric lines
	const allNames = [...metricData.map(d => d.name), 'unfairPainPerTeam'];
	const maxNameLen = Math.max(...allNames.map(n => n.length));

	const allWeights = [...metricData.map(d => d.weight), unfairnessOverallWeight];
	const maxWeightWidth = Math.max(...allWeights.map(w => w.toFixed(2).length));

	const allRawScores = [...metricData.map(d => d.rawScore), unfairScore];
	const maxRawWidth = Math.max(...allRawScores.map(s => s.toFixed(1).length));

	const allWeightedScores = [...metricData.map(d => d.weighted), unfairWeighted];
	const maxWeightedWidth = Math.max(...allWeightedScores.map(w => w.toFixed(2).length));

	// Format main metric lines
	for (const { name, weighted, rawScore, weight, teamDetails } of metricData) {
		const paddedName = name.padEnd(maxNameLen);
		const weightedStr = weighted.toFixed(2).padStart(maxWeightedWidth);
		const weightStr = weight.toFixed(2).padStart(maxWeightWidth);
		const rawStr = rawScore.toFixed(1).padStart(maxRawWidth);
		lines.push(`${paddedName} : ${weightedStr} (${weightStr} × ${rawStr}) ${teamDetails}`);
	}

	// Format unfairPainPerTeam line
	const teamTotals = perTeamPain.map((v, i) => `${TEAMS[i]}:${formatNum(v)}`).join(' ');
	const paddedUnfairName = 'unfairPainPerTeam'.padEnd(maxNameLen);
	const unfairWeightedStr = unfairWeighted.toFixed(2).padStart(maxWeightedWidth);
	const unfairWeightStr = unfairnessOverallWeight.toFixed(2).padStart(maxWeightWidth);
	const unfairRawStr = unfairScore.toFixed(1).padStart(maxRawWidth);
	lines.push(`${paddedUnfairName} : ${unfairWeightedStr} (${unfairWeightStr} × ${unfairRawStr}) ${teamTotals}`);

	// Sub-lines: show unfairness weight × per-metric stdev (informational)
	const subNameMap = {
		doubleHeaderPain: 'doubleHeaders',
		doubleByePain: 'doubleByes',
		totalSlotsPain: 'totalSlots',
		earlyWeeks: 'earlyWeeks',
		lateWeeks: 'lateWeeks',
		thirdVs2nd: 'thirdVs2nd',
	};

	const subEntries = Object.entries(perMetricPerTeam);
	const maxSubNameLen = Math.max(...subEntries.map(([m]) => (subNameMap[m] || m).length));

	const subData = subEntries.map(([metric, { perTeam, weight }]) => {
		const metricStdev = stddev(perTeam);
		const subWeighted = weight * metricStdev;
		return { metric, weight, metricStdev, subWeighted };
	});

	const maxSubWeightedWidth = Math.max(...subData.map(d => d.subWeighted.toFixed(2).length));
	const maxSubWeightWidth = Math.max(...subData.map(d => d.weight.toFixed(2).length));
	const maxSubStdevWidth = Math.max(...subData.map(d => d.metricStdev.toFixed(2).length));

	for (const { metric, weight, metricStdev, subWeighted } of subData) {
		const subName = (subNameMap[metric] || metric).padEnd(maxSubNameLen);
		const subWeightedStr = subWeighted.toFixed(2).padStart(maxSubWeightedWidth);
		const subWeightStr = weight.toFixed(2).padStart(maxSubWeightWidth);
		const subStdevStr = metricStdev.toFixed(2).padStart(maxSubStdevWidth);
		lines.push(`    ${subName} : ${subWeightedStr} (${subWeightStr} × ${subStdevStr})`);
	}

	return {
		totalScore,
		toString() {
			return `Total Score: ${totalScore.toFixed(4)}\n${lines.join('\n')}`;
		},
		toFileString() {
			return `# Total Score: ${totalScore.toFixed(4)}\n${lines.join('\n')}`;
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
	// Determine input file
	let inputPath;
	if (CLI_ARGS.inputFile) {
		inputPath = CLI_ARGS.inputFile;
	} else if (CLI_ARGS.teams && CLI_ARGS.weeks) {
		inputPath = `results/${CLI_ARGS.teams}teams-${CLI_ARGS.weeks}weeks.txt`;
	} else {
		console.error('Error: Provide a file path or --teams and --weeks');
		process.exit(1);
	}

	// Read header to get teams/weeks
	const reader = new TreeReader(inputPath);
	const header = await reader.readHeader();

	// Use header values (or CLI overrides if provided)
	const teams = CLI_ARGS.teams ?? header.teams;
	const weeks = CLI_ARGS.weeks ?? header.weeks;

	// Validate CLI args match header if both provided
	if (CLI_ARGS.teams && CLI_ARGS.teams !== header.teams) {
		console.warn(`Warning: --teams=${CLI_ARGS.teams} doesn't match file header (${header.teams})`);
	}
	if (CLI_ARGS.weeks && CLI_ARGS.weeks !== header.weeks) {
		console.warn(`Warning: --weeks=${CLI_ARGS.weeks} doesn't match file header (${header.weeks})`);
	}

	// Initialize constants based on resolved values
	initConstants(teams, weeks);

	const outputPath = CLI_ARGS.inputFile
		? CLI_ARGS.inputFile.replace(/\.txt$/, '-best.txt')
		: `results/${N_TEAMS}teams-${N_WEEKS}weeks-best.txt`;

	console.log(`Evaluating schedules from: ${inputPath}`);
	console.log(`Teams: ${N_TEAMS}, Weeks: ${N_WEEKS}, Slots per week: ${N_SLOTS}`);
	console.log(`Pain config:`, painConfig, `Unfairness overall weight:`, unfairnessOverallWeight);
	console.log();
	console.log(`File header: teams=${header.teams} weeks=${header.weeks} count=${header.count}`);
	console.log();

	let bestScore = Infinity;
	let bestSchedules = [];
	let evaluated = 0;

	let skipped = 0;

	function schedulesEqual(a, b) {
		if (!a || !b || a.length !== b.length) return false;
		for (let i = 0; i < a.length; i++) {
			if (a[i] !== b[i]) return false;
		}
		return true;
	}

	function createMetricTotals(nTeams) {
		const totals = {};
		for (const { name } of UNFAIRNESS_METRIC_SPECS) {
			totals[name] = new Float64Array(nTeams);
		}
		return totals;
	}

	function accumulateTotals(targetTotals, baseTotals, weekMetrics, nTeams) {
		for (const { name, weekField } of UNFAIRNESS_METRIC_SPECS) {
			const target = targetTotals[name];
			const base = baseTotals ? baseTotals[name] : null;
			const counts = weekMetrics[weekField];
			for (let ti = 0; ti < nTeams; ti++) {
				target[ti] = (base ? base[ti] : 0) + counts[ti];
			}
		}
	}

	let previousPath = null;
	const totalsByDepth = new Array(N_WEEKS).fill(null);

	for await (const path of reader.paths()) {
		evaluated++;

		// Validate schedule structure (skip corrupted entries from interrupted writes)
		let valid = path.length === N_WEEKS;
		if (valid) {
			for (const week of path) {
				if (week.length !== N_SLOTS) {
					valid = false;
					break;
				}
			}
		}
		if (!valid) {
			skipped++;
			continue;
		}

		// Gather week metrics (cached)
		const weekResults = path.map(week => getWeekMetrics(week));

		// Incremental unfairness totals using shared prefixes
		let commonDepth = 0;
		if (previousPath) {
			while (
				commonDepth < N_WEEKS &&
				schedulesEqual(previousPath[commonDepth], path[commonDepth])
			) {
				commonDepth++;
			}
		}

		for (let depth = commonDepth; depth < N_WEEKS; depth++) {
			if (!totalsByDepth[depth]) totalsByDepth[depth] = createMetricTotals(N_TEAMS);
			const baseTotals = depth > 0 ? totalsByDepth[depth - 1] : null;
			accumulateTotals(totalsByDepth[depth], baseTotals, weekResults[depth], N_TEAMS);
		}

		const unfairnessData = calculateUnfairnessFromTotals(totalsByDepth[N_WEEKS - 1], N_TEAMS);

		// Calculate overall score
		const score = calculateOverallScore(weekResults, path, unfairnessData.score);

		if (score < bestScore) {
			// New best - reset list
			bestScore = score;
			bestSchedules = [{ path, weekResults, scheduleNum: evaluated }];

			const explained = explainScore(weekResults, path, unfairnessData);
			console.log(`Schedule ${evaluated}/${header.count} :: Score: ${explained.totalScore.toFixed(2)}`);
			console.log(formatSchedule(path));
			console.log(explained.toString().split('\n').slice(1).join('\n'));
			console.log('Team Matchups:');
			console.log(formatTeamMatchupsGrid(path));
			console.log();
		} else if (score === bestScore) {
			// Equal to best - add to list
			bestSchedules.push({ path, weekResults, scheduleNum: evaluated });

			const explained = explainScore(weekResults, path, unfairnessData);
			console.log(`Schedule ${evaluated}/${header.count} :: Score: ${explained.totalScore.toFixed(2)}`);
			console.log(formatSchedule(path));
			console.log(explained.toString().split('\n').slice(1).join('\n'));
			console.log('Team Matchups:');
			console.log(formatTeamMatchupsGrid(path));
			console.log();
		}

		previousPath = path;
	}

	console.log(`\n${'='.repeat(60)}`);
	console.log(`Evaluation complete.`);
	console.log(`Total schedules evaluated: ${evaluated}`);
	console.log(`Week schedules cached: ${weekMetricCache.size}`);
	console.log(`Best score: ${bestScore.toFixed(4)}`);
	console.log(`Schedules with best score: ${bestSchedules.length}`);
	if (painTiming.size > 0) {
		console.log('\nPain metric timings (overall score only):');
		const rows = [];
		for (const [painName, timing] of painTiming.entries()) {
			const avgMs = timing.calls > 0 ? timing.totalMs / timing.calls : 0;
			rows.push({
				name: painName,
				totalMs: timing.totalMs,
				avgMs,
				calls: timing.calls
			});
		}
		rows.sort((a, b) => b.totalMs - a.totalMs);
		const namePad = Math.max(...rows.map(r => r.name.length));
		for (const row of rows) {
			const name = row.name.padEnd(namePad);
			const total = row.totalMs.toFixed(2).padStart(9);
			const avg = row.avgMs.toFixed(4).padStart(9);
			const calls = String(row.calls).padStart(8);
			console.log(`${name} : total ${total} ms | avg ${avg} ms | calls ${calls}`);
		}
	}

	// Write results to file
	const outputLines = [];
	outputLines.push(`# teams=${N_TEAMS} weeks=${N_WEEKS} count=${bestSchedules.length}`);
	outputLines.push(`# Pain config: ${JSON.stringify(painConfig)}`);
	outputLines.push(`# Unfairness overall weight: ${unfairnessOverallWeight}`);
	outputLines.push('');

	for (const { path, weekResults, scheduleNum } of bestSchedules) {
		const explained = explainScore(weekResults, path);
		outputLines.push(`Schedule ${scheduleNum}/${evaluated} :: Score: ${explained.totalScore.toFixed(2)}`);
		outputLines.push(formatSchedule(path));
		outputLines.push(explained.toString().split('\n').slice(1).join('\n'));
		outputLines.push('Team Matchups:');
		outputLines.push(formatTeamMatchupsGrid(path));
		outputLines.push('');
	}

	writeFileSync(outputPath, outputLines.join('\n'));
	console.log(`\nResults written to: ${outputPath}`);
	if (skipped > 0) {
		console.warn(`Warning: Skipped ${skipped} invalid/corrupted schedules`);
	}
}

main().catch(err => {
	console.error('Error:', err);
	process.exit(1);
});
