/**
 * Tool names are backend identifiers. What the user sees while waiting should
 * read like what the app is doing, not like a function call.
 *
 * Unmapped names fall back to a de-underscored version rather than being
 * hidden: a new backend tool should still show something honest.
 */

const LABELS: Record<string, string> = {
  search_players: 'finding the player',
  get_player_projection: 'pulling the projection',
  get_player_history: 'reading game logs',
  get_player_storylines: 'checking the news',
  compare_players: 'comparing players',
  get_schedule: 'checking the schedule',
  get_matchup: 'loading the matchup',
  get_matchup_insights: 'reading the matchup',
  get_parlays: 'scanning betting lines',
  sleeper_find_leagues: 'finding your leagues',
  sleeper_list_teams: 'listing teams',
  sleeper_analyze_roster: 'analyzing your roster',
  sleeper_waiver_targets: 'scanning the waiver wire',
  get_status: 'checking data freshness',
};

export function toolLabel(name: string): string {
  return LABELS[name] || name.replace(/_/g, ' ');
}
