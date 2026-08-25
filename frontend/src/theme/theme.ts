import { createTheme } from '@mui/material/styles';

/**
 * One `createTheme` call, tokens declared explicitly (palette/typography/
 * shape) rather than scattered as inline `sx={{ color: '#...' }}` across
 * components — the entire point being that changing a brand color or
 * adding a dark-mode palette later is a change to THIS file alone, not a
 * grep-and-replace across every component that happened to hardcode a hex
 * value. See docs/frontend-architecture.md's "adapting the theme" section
 * for exactly what a dark-mode addition would look like here.
 *
 * Deliberately NOT customizing MUI's `transitions` config or reaching for
 * an animation library — "simple, no fancy animation" means leaning on
 * MUI's own restrained defaults (a component still has its normal hover/
 * focus states), not stripping interactivity entirely.
 */
export const theme = createTheme({
  palette: {
    mode: 'light',
    primary: {
      main: '#1d4ed8',
    },
    secondary: {
      main: '#0f766e',
    },
    background: {
      default: '#f8fafc',
      paper: '#ffffff',
    },
  },
  typography: {
    fontFamily: [
      '"Inter"',
      '-apple-system',
      'BlinkMacSystemFont',
      '"Segoe UI"',
      'Roboto',
      'sans-serif',
    ].join(','),
    h1: { fontSize: '2rem', fontWeight: 600 },
    h2: { fontSize: '1.5rem', fontWeight: 600 },
    h3: { fontSize: '1.25rem', fontWeight: 600 },
  },
  shape: {
    borderRadius: 8,
  },
  components: {
    // MuiAppBar's default `elevation` shadow reads as "fancy" at a glance
    // (a soft drop shadow under every scroll position) — flattening it to
    // a hairline border is the one deliberate, repo-wide "no fanciness"
    // override; everything else stays at MUI's stock defaults.
    MuiAppBar: {
      styleOverrides: {
        root: {
          boxShadow: 'none',
          borderBottom: '1px solid #e2e8f0',
        },
      },
    },
  },
});
