import { lazy, Suspense } from 'react';
import { createBrowserRouter } from 'react-router-dom';
import { SimpleLayout } from '@/layouts/SimpleLayout';
import { DashboardLayout } from '@/layouts/DashboardLayout';
import { LoadingState } from '@/shared/LoadingState';

/**
 * Every leaf page is `React.lazy`-loaded (each becomes its own JS chunk,
 * fetched only when its route is actually visited) rather than imported
 * eagerly at the top of this file. `npm run build`'s own output is what
 * justified this: with every page imported eagerly, MUI + MUI X Charts +
 * every feature landed in ONE ~920KB chunk shipped on the very first page
 * load — including features/analytics's chart code, which most visitors
 * (anyone just placing or tracking an order) never touch. Route-based
 * splitting means a customer placing an order downloads roughly what
 * PlaceOrderPage needs, not the ops dashboard's charts too.
 *
 * Layouts (SimpleLayout/DashboardLayout) stay eagerly imported —
 * they're the shell every page in their section renders inside, so
 * lazy-loading them would just delay the chrome AND the page together
 * for no split benefit.
 */
const HomePage = lazy(() => import('@/features/home/HomePage').then((m) => ({ default: m.HomePage })));
const PlaceOrderPage = lazy(() =>
  import('@/features/place-order/PlaceOrderPage').then((m) => ({ default: m.PlaceOrderPage })),
);
const TrackOrderPage = lazy(() =>
  import('@/features/track-order/TrackOrderPage').then((m) => ({ default: m.TrackOrderPage })),
);
const OverviewPage = lazy(() =>
  import('@/features/overview/OverviewPage').then((m) => ({ default: m.OverviewPage })),
);
const OrderSearchPage = lazy(() =>
  import('@/features/order-search/OrderSearchPage').then((m) => ({ default: m.OrderSearchPage })),
);
const AnalyticsPage = lazy(() =>
  import('@/features/analytics/AnalyticsPage').then((m) => ({ default: m.AnalyticsPage })),
);
const FraudPage = lazy(() =>
  import('@/features/fraud/FraudPage').then((m) => ({ default: m.FraudPage })),
);
const InventoryPlaceholderPage = lazy(() =>
  import('@/features/inventory/InventoryPlaceholderPage').then((m) => ({
    default: m.InventoryPlaceholderPage,
  })),
);

/** One Suspense boundary per route, not one global boundary wrapping the
 * whole router — so navigating between two lazy pages shows THAT page's
 * loading state in place, rather than unmounting the shared layout chrome
 * (AppBar/Drawer) while the next chunk downloads. */
function withSuspense(element: React.ReactElement) {
  return <Suspense fallback={<LoadingState />}>{element}</Suspense>;
}

/**
 * The layout-per-route-group pattern: `SimpleLayout` and `DashboardLayout`
 * are each a parent route with no `path` of their own beyond the prefix,
 * rendering `<Outlet/>` for whichever child matches. Adding a new page to
 * an existing section (e.g. a second admin page) means adding one child
 * route here — it automatically inherits that section's chrome, nothing
 * about the layout itself needs to change. This is the concrete mechanism
 * behind "adaptable to future changes" for navigation/layout specifically;
 * see features/inventory's placeholder for the matching example on the
 * data/feature-module side.
 */
export const router = createBrowserRouter([
  {
    element: <SimpleLayout />,
    children: [
      { path: '/', element: withSuspense(<HomePage />) },
      { path: '/order/new', element: withSuspense(<PlaceOrderPage />) },
      { path: '/order/track', element: withSuspense(<TrackOrderPage />) },
    ],
  },
  {
    path: '/admin',
    element: <DashboardLayout />,
    children: [
      { index: true, element: withSuspense(<OverviewPage />) },
      { path: 'orders', element: withSuspense(<OrderSearchPage />) },
      { path: 'analytics', element: withSuspense(<AnalyticsPage />) },
      { path: 'fraud', element: withSuspense(<FraudPage />) },
      { path: 'inventory', element: withSuspense(<InventoryPlaceholderPage />) },
    ],
  },
]);
