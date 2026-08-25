import Stack from '@mui/material/Stack';
import Typography from '@mui/material/Typography';
import type { ReactNode } from 'react';

interface PageHeaderProps {
  title: string;
  description?: string;
  action?: ReactNode;
}

/**
 * Every page in both the customer flow and the ops dashboard opens with
 * this — one component instead of every page repeating the same
 * `<Stack><Typography variant="h1">...` boilerplate. Small, but it's the
 * actual mechanism behind "feature-rich but simple UI, no fancy
 * animation": a shared primitive like this is what keeps ten pages
 * looking like the SAME app instead of ten slightly-different ones.
 */
export function PageHeader({ title, description, action }: PageHeaderProps) {
  return (
    <Stack
      direction={{ xs: 'column', sm: 'row' }}
      spacing={2}
      // alignItems/justifyContent go through sx, not top-level props — this
      // MUI version's StackOwnProps only exposes direction/spacing/divider/
      // useFlexGap as dedicated shorthands (see node_modules/@mui/material/
      // Stack/Stack.d.ts); every other flexbox property is an `sx` value.
      sx={{
        justifyContent: 'space-between',
        alignItems: { xs: 'flex-start', sm: 'center' },
        mb: 3,
      }}
    >
      <Stack spacing={0.5}>
        <Typography variant="h1">{title}</Typography>
        {description && (
          <Typography variant="body1" color="text.secondary">
            {description}
          </Typography>
        )}
      </Stack>
      {action}
    </Stack>
  );
}
