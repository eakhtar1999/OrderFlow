import AppBar from '@mui/material/AppBar';
import Box from '@mui/material/Box';
import Divider from '@mui/material/Divider';
import Drawer from '@mui/material/Drawer';
import IconButton from '@mui/material/IconButton';
import List from '@mui/material/List';
import ListItemButton from '@mui/material/ListItemButton';
import ListItemIcon from '@mui/material/ListItemIcon';
import ListItemText from '@mui/material/ListItemText';
import Toolbar from '@mui/material/Toolbar';
import Typography from '@mui/material/Typography';
import MenuIcon from '@mui/icons-material/Menu';
import DashboardIcon from '@mui/icons-material/SpaceDashboard';
import ReceiptLongIcon from '@mui/icons-material/ReceiptLong';
import InsightsIcon from '@mui/icons-material/Insights';
import ShieldIcon from '@mui/icons-material/Shield';
import Inventory2Icon from '@mui/icons-material/Inventory2';
import StorefrontIcon from '@mui/icons-material/Storefront';
import useMediaQuery from '@mui/material/useMediaQuery';
import { useTheme } from '@mui/material/styles';
import { NavLink, Outlet, useLocation } from 'react-router-dom';
import { useUiStore } from '@/store/ui';

const DRAWER_WIDTH = 240;

const NAV_ITEMS = [
  { label: 'Overview', to: '/admin', icon: <DashboardIcon /> },
  { label: 'Orders', to: '/admin/orders', icon: <ReceiptLongIcon /> },
  { label: 'Analytics', to: '/admin/analytics', icon: <InsightsIcon /> },
  { label: 'Fraud', to: '/admin/fraud', icon: <ShieldIcon /> },
  { label: 'Inventory', to: '/admin/inventory', icon: <Inventory2Icon /> },
] as const;

/**
 * The ops-dashboard chrome: a responsive AppBar+Drawer shell wrapping
 * whatever route the router places in `<Outlet/>`. "Responsive" here
 * means the SAME `<Drawer>` component switches `variant` at the `md`
 * breakpoint (permanent, always visible, pushes content over — vs.
 * temporary, an overlay you open/close) rather than maintaining two
 * separate drawer implementations for desktop and mobile.
 *
 * This is paired with layouts/SimpleLayout.tsx, selected per route group
 * in routes/routes.tsx (nested routes each wrap their children in one
 * layout or the other) — see that file and docs/frontend-architecture.md
 * for why a layout is a routing concern here, not a per-page choice.
 */
export function DashboardLayout() {
  const theme = useTheme();
  const isDesktop = useMediaQuery(theme.breakpoints.up('md'));
  const drawerOpen = useUiStore((state) => state.dashboardDrawerOpen);
  const toggleDrawer = useUiStore((state) => state.toggleDashboardDrawer);
  const location = useLocation();

  const drawerContent = (
    <>
      <Toolbar>
        <StorefrontIcon color="primary" sx={{ mr: 1 }} />
        <Typography variant="h3" component="span">
          OrderFlow
        </Typography>
      </Toolbar>
      <Divider />
      <List>
        {NAV_ITEMS.map((item) => {
          const selected =
            item.to === '/admin'
              ? location.pathname === '/admin'
              : location.pathname.startsWith(item.to);
          return (
            <ListItemButton
              key={item.to}
              component={NavLink}
              to={item.to}
              selected={selected}
              onClick={() => !isDesktop && toggleDrawer()}
            >
              <ListItemIcon>{item.icon}</ListItemIcon>
              <ListItemText primary={item.label} />
            </ListItemButton>
          );
        })}
      </List>
    </>
  );

  return (
    <Box sx={{ display: 'flex' }}>
      <AppBar
        position="fixed"
        color="inherit"
        sx={{
          width: { md: `calc(100% - ${DRAWER_WIDTH}px)` },
          ml: { md: `${DRAWER_WIDTH}px` },
        }}
      >
        <Toolbar>
          {!isDesktop && (
            <IconButton edge="start" onClick={toggleDrawer} sx={{ mr: 2 }}>
              <MenuIcon />
            </IconButton>
          )}
          <Typography variant="h3" component="h1">
            Ops Dashboard
          </Typography>
        </Toolbar>
      </AppBar>

      <Drawer
        variant={isDesktop ? 'permanent' : 'temporary'}
        open={isDesktop ? true : drawerOpen}
        onClose={toggleDrawer}
        ModalProps={{ keepMounted: true }}
        sx={{
          width: DRAWER_WIDTH,
          flexShrink: 0,
          '& .MuiDrawer-paper': { width: DRAWER_WIDTH, boxSizing: 'border-box' },
        }}
      >
        {drawerContent}
      </Drawer>

      <Box
        component="main"
        sx={{
          flexGrow: 1,
          width: { md: `calc(100% - ${DRAWER_WIDTH}px)` },
          p: 3,
        }}
      >
        <Toolbar />
        <Outlet />
      </Box>
    </Box>
  );
}
