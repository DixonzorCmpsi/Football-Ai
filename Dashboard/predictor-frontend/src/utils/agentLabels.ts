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
  open_screen: 'opening the page',
  open_player: 'opening the player',
  open_compare: 'opening the comparison',
  open_team: 'opening the team',
  open_game: 'opening the game',
  open_player_card: 'opening the player card',
  add_to_compare: 'adding to the comparison',
  go_back: 'going back',
  web_search: 'searching the web',
  fetch_page: 'reading a web page',
  read_screen: 'looking at your screen',
  click: 'clicking',
  type_text: 'typing',
  press_key: 'pressing a key',
  select_option: 'choosing an option',
  scroll: 'scrolling',
};

export function toolLabel(name: string): string {
  return LABELS[name] || name.replace(/_/g, ' ');
}
