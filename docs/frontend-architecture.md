# OrderFlow Frontend — Architecture Notes

Same spirit as [`docs/aws-cloud-deployment.md`](aws-cloud-deployment.md) and
[`docs/git-github-workflow.md`](git-github-workflow.md): this explains the
*why* behind `frontend/`'s structure deeply enough that the pattern
transfers to a different React app, not just this one. Read this before
adding a page, a store, or a new API call — it's the map for where new
code belongs and why.

---

## 1. Why a React + Vite SPA, not Next.js

OrderFlow's backend is a pure REST API behind an ALB (`docs/aws-cloud-
deployment.md`'s Phase 5c) — there is no server-rendering need, no SEO
requirement, no page that benefits from being pre-rendered on a server.
Next.js's actual value (SSR, server components, file-based API routes)
solves problems this project doesn't have, at the cost of real complexity
(a Node server to run and operate, a build output that isn't just static
files). A Vite SPA builds to plain HTML/CSS/JS, deploys to S3 behind
CloudFront (`cloud/cdk/lib/frontend-stack.ts`), and needs no server of its
own — the same "no server to operate" trade-off this project's backend
deliberately does NOT make (it needs real compute for Kafka/Postgres/etc.),
but the frontend, being a pure client of that backend's REST API, can.

## 2. State management: TanStack Query + Zustand, not Redux

Two genuinely different kinds of state exist in this app, and mixing them
into one store is the actual anti-pattern this split avoids:

- **Server state** (an order search result, analytics numbers, a fraud
  score) — owned entirely by **TanStack Query** (`src/features/*/use*.ts`
  hooks). It's not "data the app owns," it's a cached, revalidatable copy
  of something the backend owns. Caching, retries, polling
  (`refetchInterval` — see `features/analytics/useAnalytics.ts`), and
  request deduplication all come for free.
- **Client state** (is the mobile nav drawer open, which saga style did
  the user last pick, the toast queue) — owned by **Zustand**
  (`src/store/ui.ts`). Nothing here was ever fetched from a server; it has
  no staleness concept.

If you're about to add a `useState` that holds data from a `fetch` call,
it belongs in a TanStack Query hook instead. If you're about to add a
query hook for something that never touches the network, it belongs in
Zustand instead. See `src/store/ui.ts`'s own header comment for the same
rule stated from the store's side.

Redux Toolkit was the other option on the table — passed over because
RTK Query would duplicate TanStack Query's job, and plain RTK's reducer/
action ceremony has no payoff for the amount of genuinely-client state
this app has (one small store, see `src/store/ui.ts`).

## 3. Folder structure — feature modules

```
frontend/src/
  app/         # root composition: providers, router mount, error boundary, toasts
  theme/       # MUI theme tokens
  layouts/     # SimpleLayout (customer flow), DashboardLayout (ops)
  shared/      # reusable presentational components (PageHeader, DataTable, ...)
  api/         # one typed function per real backend endpoint, routed through api/client.ts
  features/    # one directory per user-facing feature
  store/       # Zustand stores
  routes/      # route tree, wires layouts to features
  types/       # TS types mirroring backend DTOs
  config/      # env.ts — validated env var access
```

`features/<name>/` is the unit of "a thing a user can do" — its own page
component, its own `use*.ts` query/mutation hooks, and (for the
non-trivial ones) its own form component. A feature never imports another
feature's internals; if two features need the same data, they both call
the same `api/*.ts` function and the same TanStack Query cache
deduplicates the actual network request (see
`features/overview/OverviewPage.tsx` reusing `features/analytics`'s own
hooks as the concrete example).

**Adding a new feature is the concrete "adaptable to future changes"
story**: add `api/<domain>.ts` (one function per endpoint, routed through
`apiRequest`), add `features/<name>/use<Name>.ts` (a `useQuery`/
`useMutation` wrapping that function), add `features/<name>/<Name>Page.tsx`,
add one line to `routes/routes.tsx`. Nothing else in the app — no layout,
no other feature, no shared component — needs to change. `features/
inventory/InventoryPlaceholderPage.tsx` is this pattern's worked example
in reverse: it names the exact backend file (`InternalReservationController
.java`) that would need a new public endpoint before that feature could be
built for real, rather than faking data.

## 4. The API layer and five base URLs

Every feature calls a function in `api/*.ts`; every one of those routes
through `api/client.ts`'s single `apiRequest()`. That's the isolation
boundary — if the backend ever requires an `Authorization` header on every
call, `api/client.ts` is the only file that changes.

This project's backend genuinely has **five different origins** the
frontend must call, not one — see `.env.example` for the full explanation.
Short version: locally, each service is its own process on its own port
with no path prefix; in AWS, all five sit behind one ALB on one hostname,
split by path prefix (`cloud/cdk/lib/app-services-stack.ts`'s
`SERVICE_SPECS`). Baking the entire origin+prefix into each env var (`src/
config/env.ts`) means `api/*.ts` files never branch on environment —
only the *values* differ between `.env.local` and production.

`src/config/env.ts` fails loudly at module load if any var is missing,
rather than letting `undefined` silently become the string `"undefined"`
inside a request URL — see that file's own comment for why that
distinction matters.

## 5. MUI: theme, layout, and the "no fancy animation" constraint

`src/theme/theme.ts` is the one place palette/typography/shape tokens
live — components use `sx` for one-off layout, but a brand color change
or (eventually) a dark-mode palette is a change to that one file, not a
grep across every component. "No fancy animation" was implemented as
*restraint*, not as stripping interactivity: MUI's own default
transitions (hover/focus states, standard easing) are kept; the one
deliberate override is flattening `MuiAppBar`'s drop-shadow to a hairline
border (see `theme.ts`'s `components.MuiAppBar` override) because a
shadow that shifts on scroll reads as "fancy" at a glance.

**Layout is a routing concern, not a per-page choice.** `routes/routes.tsx`
nests every customer-facing page under `SimpleLayout` and every ops page
under `DashboardLayout` — each layout is a parent route rendering
`<Outlet/>`. Adding a page to an existing section means adding one child
route; it inherits that section's chrome automatically. See `layouts/
DashboardLayout.tsx`'s header comment for how the same `<Drawer>`
component becomes `variant="permanent"` (desktop) or `"temporary"`
(mobile) off one `useMediaQuery` check, rather than two separate
implementations.

**Adapting the theme for dark mode later** would mean: add a `dark`
palette object to `theme.ts` (or switch to `createTheme({ colorSchemes:
{ light: {...}, dark: {...} } })`, MUI's built-in multi-scheme API), wire
a toggle into `store/ui.ts` (another small piece of genuinely client-only
state), and nothing else — no component currently hardcodes a color
outside the theme, so no component-level changes would be needed.

## 6. Why no form library (yet)

`features/place-order/PlaceOrderForm.tsx` uses plain `useState` for four
fields plus a small dynamic item list — small enough that react-hook-form/
Formik would be net-negative complexity for what this form currently does.
The trade-off flips once a form needs cross-field validation, nested
field arrays with complex validation rules, or gets reused across many
pages with different schemas — none of which is true here yet.

## 7. Testing

Vitest + React Testing Library + MSW (`src/test/`). MSW intercepts at the
`fetch` layer (not `vi.mock('@/api/client')`) so a test failure means the
actual HTTP request shape was wrong, not that a mocked function received
unexpected arguments — see `src/test/server.ts`'s header comment.
`renderWithProviders` (`src/test/renderWithProviders.tsx`) wraps a
component in the same MUI/TanStack Query providers `app/App.tsx` sets up
for real, with a fresh `QueryClient` per test so cached data never leaks
between tests.

There's no Testcontainers/Embedded-Kafka equivalent here — the frontend
has no database or broker of its own to spin up; MSW's mocked network
layer is the right-sized tool for this layer of the stack.

## 8. Code splitting

`routes/routes.tsx` lazy-loads every leaf page (`React.lazy` + one
`Suspense` boundary per route) rather than importing every feature
eagerly. This was a measured decision, not a guess: eager imports produced
one ~920KB initial chunk (`npm run build`'s own bundle-size warning) with
MUI X Charts' code shipped to every visitor, including someone who only
places an order and never opens the ops dashboard. Splitting per-route
brought the initial chunk to ~369KB and moved `AnalyticsPage`'s ~306KB
(mostly chart library) into its own chunk, fetched only when that route
is visited. Layouts (`SimpleLayout`/`DashboardLayout`) stay eagerly
imported since they're the shell every page in their section renders
inside — splitting them would delay the chrome along with the page for no
benefit.

## 9. Known gaps, stated plainly

- **No `GET /api/orders/{id}`.** `order-service`'s own `OrderController`
  says as much in its own comment. `features/track-order` searches by
  `customerId` against search-indexer-service instead — the one filter
  that endpoint actually supports and a customer would know.
- **No public inventory read endpoint.** `features/inventory` is a
  placeholder naming the exact backend file that would need to change.
- **Fraud velocity lookups only work correctly with one running instance**
  of fraud-detection-service (surfaced as a visible `Alert` on
  `FraudPage`, not hidden) — same limitation `FraudQueryController.java`
  documents about itself.
- **Search results are eventually consistent**, lagging Postgres by
  however far search-indexer-service's consumer group lag currently is —
  surfaced as a visible note on both `TrackOrderPage` and the search flow,
  not hidden.
