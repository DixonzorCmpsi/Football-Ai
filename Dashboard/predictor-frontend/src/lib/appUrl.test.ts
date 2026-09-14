import { describe, it, expect } from 'vitest';
import { formatAppUrl, parseAppUrl } from './appUrl';
import type { AppLocation } from './appUrl';

const roundTrip = (loc: AppLocation) => {
  const url = formatAppUrl(loc);
  const parsed = parseAppUrl(url);
  expect(parsed).not.toBeNull();
  expect(parsed).toEqual(loc);
};

describe('appUrl round-trips', () => {
  it('SCHEDULE', () => roundTrip({ view: 'SCHEDULE' }));
  it('GAME', () => roundTrip({ view: 'GAME', home: 'HOU', away: 'BUF' }));
  it('LOOKUP', () => roundTrip({ view: 'LOOKUP' }));
  it('COMPARE with ids', () => roundTrip({ view: 'COMPARE', ids: ['00-001', '00-002'] }));
  it('COMPARE empty', () => roundTrip({ view: 'COMPARE', ids: [] }));
  it('HISTORY', () => roundTrip({ view: 'HISTORY', playerId: '00-0037834' }));
  it('TRENDING', () => roundTrip({ view: 'TRENDING' }));
  it('PICKS', () => roundTrip({ view: 'PICKS' }));
  it('PLAYOFFS', () => roundTrip({ view: 'PLAYOFFS' }));
  it('TIERS', () => roundTrip({ view: 'TIERS' }));
  it('TEAMS', () => roundTrip({ view: 'TEAMS' }));
  it('GAME_RANKS', () => roundTrip({ view: 'GAME_RANKS' }));
  it('TEAM_PAGE overview', () => roundTrip({ view: 'TEAM_PAGE', team: 'BUF', tab: 'overview' }));
  it('TEAM_PAGE builder', () => roundTrip({ view: 'TEAM_PAGE', team: 'NE', tab: 'builder' }));
  it('MY_TEAM lineup', () => roundTrip({ view: 'MY_TEAM', tab: 'LINEUP' }));
  it('MY_TEAM waivers', () => roundTrip({ view: 'MY_TEAM', tab: 'WAIVERS' }));
  it('MY_TEAM league', () => roundTrip({ view: 'MY_TEAM', tab: 'LEAGUE' }));
});

describe('appUrl strict rejections', () => {
  it('rejects unknown path', () => {
    expect(parseAppUrl('/nope')).toBeNull();
  });
  it('rejects game with path traversal', () => {
    expect(parseAppUrl('/game/../x')).toBeNull();
  });
  it('rejects game with too-long team', () => {
    expect(parseAppUrl('/game/BUF/TOOLONG')).toBeNull();
  });
  it('rejects player with script tag', () => {
    expect(parseAppUrl('/player/<script>')).toBeNull();
  });
  it('rejects team with too-long abbrev', () => {
    expect(parseAppUrl('/team/TOOLONG')).toBeNull();
  });
  it('rejects my-team with bad sub-path', () => {
    expect(parseAppUrl('/my-team/badtab')).toBeNull();
  });
});

describe('appUrl /my-team/league', () => {
  it('parses to LEAGUE tab', () => {
    expect(parseAppUrl('/my-team/league')).toEqual({ view: 'MY_TEAM', tab: 'LEAGUE' });
  });
  it('parses waivers', () => {
    expect(parseAppUrl('/my-team/waivers')).toEqual({ view: 'MY_TEAM', tab: 'WAIVERS' });
  });
});

describe('appUrl compare-id trimming', () => {
  it('keeps at most 4 ids', () => {
    const loc = parseAppUrl('/compare?ids=00-001,00-002,00-003,00-004,00-005');
    expect(loc).toEqual({ view: 'COMPARE', ids: ['00-001', '00-002', '00-003', '00-004'] });
  });
  it('drops invalid ids', () => {
    const loc = parseAppUrl('/compare?ids=00-001,<bad>,00-002');
    expect(loc).toEqual({ view: 'COMPARE', ids: ['00-001', '00-002'] });
  });
});

describe('appUrl tolerances', () => {
  it('tolerates leading/trailing slashes', () => {
    expect(parseAppUrl('//ranks/')).toEqual({ view: 'GAME_RANKS' });
  });
  it('tolerates case in path segment names', () => {
    expect(parseAppUrl('/Ranks')).toEqual({ view: 'GAME_RANKS' });
  });
});
describe('game links carry their week', () => {
  it('round-trips the week and ignores a bad one', () => {
    expect(formatAppUrl({ view: 'GAME', away: 'NE', home: 'SEA', week: 1 })).toBe('/game/NE/SEA?week=1');
    expect(parseAppUrl('/game/NE/SEA?week=1')).toEqual({ view: 'GAME', away: 'NE', home: 'SEA', week: 1 });
    expect(parseAppUrl('/game/NE/SEA?week=99')).toEqual({ view: 'GAME', away: 'NE', home: 'SEA' });
    expect(parseAppUrl('/game/NE/SEA')).toEqual({ view: 'GAME', away: 'NE', home: 'SEA' });
  });
});

describe('player cards and player page tabs', () => {
  it('opens a card over a game, on a tab', () => {
    const loc: AppLocation = { view: 'GAME', away: 'NYJ', home: 'TEN', week: 1, player: '00-0039890', card: 'vegas' };
    expect(formatAppUrl(loc)).toBe('/game/NYJ/TEN?week=1&player=00-0039890&tab=vegas');
    expect(parseAppUrl('/game/NYJ/TEN?week=1&player=00-0039890&tab=vegas')).toEqual(loc);
  });
  it('defaults the card to its game log and drops a bad tab or player', () => {
    expect(parseAppUrl('/game/NYJ/TEN?player=00-0039890')).toEqual({ view: 'GAME', away: 'NYJ', home: 'TEN', player: '00-0039890', card: 'log' });
    expect(formatAppUrl({ view: 'GAME', away: 'NYJ', home: 'TEN', player: '00-0039890', card: 'log' })).toBe('/game/NYJ/TEN?player=00-0039890');
    expect(parseAppUrl('/game/NYJ/TEN?player=00-0039890&tab=nope')).toMatchObject({ card: 'log' });
    expect(parseAppUrl('/game/NYJ/TEN?player=<x>')).toEqual({ view: 'GAME', away: 'NYJ', home: 'TEN' });
  });
  it('opens the player page on a chosen view', () => {
    expect(formatAppUrl({ view: 'HISTORY', playerId: '00-0039890', show: 'table' })).toBe('/player/00-0039890?view=table');
    expect(parseAppUrl('/player/00-0039890?view=table')).toEqual({ view: 'HISTORY', playerId: '00-0039890', show: 'table' });
    expect(parseAppUrl('/player/00-0039890?view=bogus')).toEqual({ view: 'HISTORY', playerId: '00-0039890' });
  });
});
