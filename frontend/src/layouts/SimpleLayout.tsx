import AppBar from '@mui/material/AppBar';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Container from '@mui/material/Container';
import Stack from '@mui/material/Stack';
import Toolbar from '@mui/material/Toolbar';
import Typography from '@mui/material/Typography';
import StorefrontIcon from '@mui/icons-material/Storefront';
import { NavLink, Outlet } from 'react-router-dom';

/**
 * The customer-facing chrome: a plain top bar plus a centered `Container`
 * — deliberately no `Drawer`/dashboard nav here. Pairs with
 * layouts/DashboardLayout.tsx; see that file's header comment for how
 * routes/routes.tsx picks between the two.
 */
export function SimpleLayout() {
  return (
    <Box>
      <AppBar position="static" color="inherit">
        <Toolbar>
          <StorefrontIcon color="primary" sx={{ mr: 1 }} />
          <Typography variant="h3" component="h1" sx={{ flexGrow: 1 }}>
            OrderFlow
          </Typography>
          <Stack direction="row" spacing={1}>
            <Button component={NavLink} to="/order/new" color="inherit">
              Place an order
            </Button>
            <Button component={NavLink} to="/order/track" color="inherit">
              Track my orders
            </Button>
            <Button component={NavLink} to="/admin" color="inherit">
              Ops dashboard
            </Button>
          </Stack>
        </Toolbar>
      </AppBar>

      <Container maxWidth="md" sx={{ py: 4 }}>
        <Outlet />
      </Container>
    </Box>
  );
}
